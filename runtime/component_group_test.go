package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/testharness"
	runtimeauth "github.com/viant/datly/runtime/auth"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	hp "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/scy/auth/jwt"
	xhandler "github.com/viant/xdatly/handler"
)

type groupLeafRow struct {
	Value string `sqlx:"value"`
}
type groupLeafOutput struct{ Rows []*groupLeafRow }
type groupLeafInput struct{}
type groupSource struct {
	native dml.Source
	opens  atomic.Int32
	data   *dml.Data
}

func (s *groupSource) InvocationKey() any { return s.native.InvocationKey() }
func (s *groupSource) Open(ctx context.Context) (xhandler.Data, error) {
	s.opens.Add(1)
	v, e := s.native.Open(ctx)
	if e != nil {
		return nil, e
	}
	s.data = v.(*dml.Data)
	if e = v.Execute("INSERT INTO audit VALUES(101,'caller queued')"); e != nil {
		return nil, e
	}
	return v, nil
}

type groupScopedInput struct {
	Jwt   *jwt.Claims                `parameter:",kind=header,in=Authorization,dataType=string,required=true,errorCode=401" codec:"JwtClaim"`
	Child *groupLeafOutput           `parameter:",kind=component,in=GET:/group-leaf,scope=lazy,cacheable=true"`
	Has   *struct{ Jwt, Child bool } `setMarker:"true"`
}
type groupScopedContract struct {
	calls  atomic.Int32
	mode   string
	saved  xhandler.ScopedBinder
	input  *groupScopedInput
	events []string
	before func()
}

func (*groupScopedContract) BindsInputByScope() bool { return true }
func (h *groupScopedContract) Exec(ctx context.Context, s xhandler.Session, i *groupScopedInput, o *scopedNativeOutput) error {
	h.calls.Add(1)
	h.events = append(h.events, "begin")
	if i.Jwt == nil || i.Has == nil || !i.Has.Jwt || i.Has.Child || i.Child != nil {
		return errors.New("default binding state")
	}
	b := s.Binder().(xhandler.ScopedBinder)
	h.saved, h.input = b, i
	value, found, e := b.Lookup(ctx, xhandler.InputKey)
	if e != nil || !found || value != i {
		return errors.New("canonical pointer lost")
	}
	if i.Jwt.Subject == "denied" {
		return errors.New("source authorization denied")
	}
	h.events = append(h.events, "authorize")
	marker, claims := i.Has, i.Jwt
	if h.before != nil {
		h.before()
	}
	if h.mode == "imperative" {
		v, f, e := b.Lookup(ctx, xhandler.ComponentInvokerKey)
		if e != nil || !f {
			return fmt.Errorf("invoker %v", e)
		}
		_, e = v.(xhandler.ComponentInvoker).Invoke(context.Background(), xhandler.ComponentRequest{Target: xhandler.ComponentTarget{Scope: "example.com/components", Name: "GroupLeaf", Method: "GET", Path: "/group-leaf"}})
		return e
	}

	if strings.HasPrefix(h.mode, "sql:") || h.mode == "transaction start" {
		key := rh.TransactionSQLCapabilityKey
		if h.mode == "transaction start" {
			key = xhandler.TransactionStarterKey
		}
		v, f, e := b.Lookup(ctx, key)
		if e != nil || !f {
			return fmt.Errorf("managed SQL provider %v", e)
		}
		var denied error
		if h.mode == "transaction start" {
			denied = v.(xhandler.TransactionStarter).Start(context.Background())
		} else {
			capability, e := v.(rh.TransactionSQLProvider).Connector(ctx, "ci_ads")
			if e != nil {
				return e
			}
			switch h.mode {
			case "sql:Exec":
				_, denied = capability.ExecContext(context.Background(), "INSERT INTO audit VALUES(99,'forbidden')")
			case "sql:Query":
				var rows *sql.Rows
				rows, denied = capability.QueryContext(context.Background(), "SELECT value FROM records")
				if rows != nil {
					rows.Close()
				}
			case "sql:QueryRow":
				var value string
				denied = capability.QueryRowContext(context.Background(), "SELECT value FROM records").Scan(&value)
			}
		}
		if !errors.Is(denied, drainowner.ErrBindingGroup) {
			return fmt.Errorf("expected group SQL guard: %w", denied)
		}
		return nil // Deliberately caught; managed failure must remain sticky.
	}
	if strings.HasPrefix(h.mode, "effect:") {
		v, f, e := b.Lookup(ctx, xhandler.DataKey)
		if e != nil || !f {
			return fmt.Errorf("Data %v", e)
		}
		data := v.(xhandler.Data)
		var denied error
		row := &struct {
			ID   int    `sqlx:"id,primaryKey"`
			Name string `sqlx:"name"`
		}{ID: 99, Name: "forbidden"}
		switch strings.TrimPrefix(h.mode, "effect:") {
		case "Insert":
			denied = data.Insert("audit", row)
		case "Update":
			denied = data.Update("audit", row)
		case "Delete":
			denied = data.Delete("audit", row)
		case "Execute":
			denied = data.Execute("INSERT INTO audit VALUES(99,'forbidden')")
		case "Allocate":
			denied = data.Allocate(ctx, "audit", row, "ID")
		case "Reserve":
			denied = data.(interface {
				Reserve(context.Context, string, any, string) error
			}).Reserve(ctx, "audit", row, "ID")
		case "Flush":
			denied = data.Flush(context.Background(), "")
		}
		if denied == nil {
			return errors.New("managed effect was not rejected")
		}
		// Deliberately ignore the denied operation: completion must retain it.
		return nil
	}
	e = b.BindScope(ctx, i, "lazy")
	if e != nil {
		h.events = append(h.events, "child-failed")
		if h.mode == "catch" || h.mode == "reload failure" || h.mode == "registration mismatch" {
			return nil
		}
		return fmt.Errorf("source child wrapper: %w", e)
	}
	if marker != i.Has || claims != i.Jwt || !i.Has.Child || i.Child == nil || len(i.Child.Rows) != 1 {
		return errors.New("canonical lazy state lost")
	}
	if h.mode == "repeat" {
		return b.BindScope(context.Background(), i, "lazy")
	}
	o.Value = i.Child.Rows[0].Value
	h.events = append(h.events, "end")
	return nil
}

type groupBody struct{ Value string }
type groupRootInput struct {
	Jwt    *jwt.Claims         `parameter:",kind=header,in=Authorization,dataType=string,required=true,errorCode=401" codec:"JwtClaim"`
	Data   *groupBody          `parameter:",kind=body,in=data,required=true"`
	ID     int                 `parameter:",kind=path,in=id,resolutionGroup=records,resolutionAfter=Jwt|Data"`
	Scoped *scopedNativeOutput `parameter:",kind=component,in=GET:/group-scoped,resolutionGroup=records,resolutionAfter=Jwt|Data"`
	A      *groupLeafOutput    `parameter:",kind=component,in=GET:/group-a/{id},resolutionGroup=records,resolutionAfter=Jwt|Data,dependsOn=ID"`
	B      *groupLeafOutput    `parameter:",kind=component,in=GET:/group-b/{id},resolutionGroup=records,resolutionAfter=Jwt|Data,dependsOn=ID"`
	C      *groupLeafOutput    `parameter:",kind=component,in=GET:/group-c/{id},resolutionGroup=records,resolutionAfter=Jwt|Data,dependsOn=ID"`
}
type groupPathInput struct {
	ID int `parameter:",kind=path,in=id"`
}

func TestScopedGroupGenuineNativeJWTLeafAndEffects(t *testing.T) {
	for _, mode := range []string{"success", "repeat", "reload failure", "registration mismatch", "missing", "malformed", "denied", "body missing", "child failure", "catch", "imperative", "effect:Insert", "effect:Update", "effect:Delete", "effect:Execute", "effect:Allocate", "effect:Reserve", "effect:Flush", "transaction start", "sql:Exec", "sql:Query", "sql:QueryRow"} {
		t.Run(mode, func(t *testing.T) {
			h := testharness.NewSQLiteHarness(t)
			if e := h.ExecStatements(t.Context(), "CREATE TABLE records(value TEXT)", "INSERT INTO records VALUES('native child')", "CREATE TABLE audit(id INTEGER PRIMARY KEY,name TEXT)"); e != nil {
				t.Fatal(e)
			}
			key := testharness.NewJWT(t)
			service, e := runtimeauth.New(t.Context(), &runtimeauth.Config{JWTValidator: key.Config()})
			if e != nil {
				t.Fatal(e)
			}
			source := &groupSource{native: dml.Source{DB: h.DB}}
			connectors := &dsql.SQLComponent{}
			if e := connectors.RegisterConnector("ci_ads", h.DB); e != nil {
				t.Fatal(e)
			}
			contract := &groupScopedContract{mode: mode}
			auth, e := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: componentSpec("GroupScoped", "GET", "/group-scoped", nil), InputType: reflect.TypeFor[groupScopedInput](), OutputType: reflect.TypeFor[scopedNativeOutput](), Handler: custom.New[groupScopedInput, scopedNativeOutput](contract), CodecFactory: service})
			if e != nil {
				t.Fatal(e)
			}
			ar, e := auth.Registration(registry.RegisteredComponent{Capabilities: rh.InvocationCapabilities{Connector: connectors}})
			if e != nil {
				t.Fatal(e)
			}
			registrations := []*registry.RegisteredComponent{ar}
			for _, item := range []struct {
				name, path string
				input      reflect.Type
			}{{"GroupLeaf", "/group-leaf", reflect.TypeFor[groupLeafInput]()}, {"GroupA", "/group-a/{id}", reflect.TypeFor[groupPathInput]()}, {"GroupB", "/group-b/{id}", reflect.TypeFor[groupPathInput]()}, {"GroupC", "/group-c/{id}", reflect.TypeFor[groupPathInput]()}} {
				c := componentSpec(item.name, "GET", item.path, []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}})
				table := "records"
				if item.name == "GroupLeaf" && (mode == "child failure" || mode == "catch") {
					table = "absent_actual_leaf"
				}
				c.RootView = &spec.View{Name: item.name, Source: &spec.ViewSource{SQL: "SELECT value FROM " + table}}
				a, e := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: item.input, OutputType: reflect.TypeFor[groupLeafOutput](), DirectViewField: "Rows"})
				if e != nil {
					t.Fatal(e)
				}
				reader, e := a.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: h.DB}})
				if e != nil {
					t.Fatal(e)
				}
				registrations = append(registrations, &registry.RegisteredComponent{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[groupLeafOutput](), Reader: reader})
			}
			root, e := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: componentSpec("GroupRoot", "POST", "/group-root/{id}", nil), InputType: reflect.TypeFor[groupRootInput](), OutputType: reflect.TypeFor[scopedNativeOutput](), CodecFactory: service})
			if e != nil {
				t.Fatal(e)
			}
			rootCalls := 0
			registrations = append(registrations, &registry.RegisteredComponent{Component: root.Component, Input: root.Input, Output: root.Output, OutputType: reflect.TypeFor[scopedNativeOutput](), DataSource: source, Capabilities: rh.InvocationCapabilities{Connector: connectors}, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
				rootCalls++
				i := in.Input.(*groupRootInput)
				if i.Scoped == nil || i.Scoped.Value != "native child" || i.A == nil || i.B == nil || i.C == nil {
					return nil, errors.New("sibling results missing")
				}
				return &scopedNativeOutput{Value: "all native results"}, nil
			})})
			rt, e := NewRuntime(registrations)
			if e != nil {
				t.Fatal(e)
			}
			defer rt.Shutdown(context.Background())
			if mode == "reload failure" {
				contract.before = func() { rt.registered[registrations[1].Component.Key.String()].Reader = nil }
			}
			if mode == "registration mismatch" {
				contract.before = func() {
					copied := *rt.registered[registrations[1].Component.Key.String()].Output
					rt.registered[registrations[1].Component.Key.String()].Output = &copied
				}
			}
			body := `{"data":{"value":"ok"}}`
			if mode == "body missing" {
				body = `{}`
			}
			req := httptest.NewRequest("POST", "/group-root/7", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			subject := "approved"

			if mode == "denied" {
				subject = "denied"
			}
			if mode != "missing" {
				req.Header.Set("Authorization", key.Sign(t, subject, time.Now().Add(time.Hour)))
			}
			if mode == "malformed" {
				req.Header.Set("Authorization", "Bearer malformed")
			}
			result, e := executeTestRoute(t, rt, t.Context(), testharness.Request{HTTP: req})
			if mode == "success" {
				if e != nil || rootCalls != 1 || result.(*scopedNativeOutput).Value != "all native results" || strings.Join(contract.events, ",") != "begin,authorize,end" {
					t.Fatal(e, rootCalls, contract.events)
				}
			} else {
				if e == nil || rootCalls != 0 {
					t.Fatalf("failure swallowed mode%s calls%d err%v", mode, rootCalls, e)
				}
			}
			if mode == "reload failure" || mode == "registration mismatch" {
				if strings.Join(contract.events, ",") != "begin,authorize,child-failed" || !errors.Is(e, drainowner.ErrBindingGroup) {
					t.Fatalf("early declared failure was not retained after catch: order%v err%v", contract.events, e)
				}
			}
			if mode == "missing" || mode == "malformed" || mode == "body missing" {
				if contract.calls.Load() != 0 {
					t.Fatal("barrier failed after begin")
				}
			}
			if mode == "denied" && strings.Join(contract.events, ",") != "begin" {
				t.Fatal(contract.events)
			}
			if contract.saved != nil {
				if e := contract.saved.BindScope(t.Context(), contract.input, "lazy"); e == nil {
					t.Fatal("retained binder admitted")
				}
			}
			if source.opens.Load() != 1 {
				t.Fatalf("native owner opened %d times", source.opens.Load())
			}
			if mode != "missing" && mode != "malformed" && mode != "body missing" && contract.calls.Load() != 1 {
				t.Fatalf("custom invocation never reached: %v", e)
			}
			var count int
			wantCount := 0
			if mode == "success" {
				wantCount = 1
			}
			if e := h.DB.QueryRow("SELECT count(*) FROM audit").Scan(&count); e != nil || count != wantCount {
				t.Fatal("physical effect", count, e)
			}
		})
	}
}

func TestScopedGroupDescriptorRegistrationAndOptionGuards(t *testing.T) {
	parent := componentArtifact(t, componentSpec("DescriptorParent", "GET", "/group-scoped", nil), reflect.TypeFor[scopedNativeInput](), reflect.TypeFor[scopedNativeOutput]())
	parent.Handler = custom.New[scopedNativeInput, scopedNativeOutput](scopedNativeContract{})
	leaf := componentArtifact(t, componentSpec("DescriptorLeaf", "GET", "/scope-child", nil), reflect.TypeFor[groupLeafInput](), reflect.TypeFor[scopedNativeChildOutput]())
	reader := &groupedSeedReader{new(int)}
	pr, e := parent.Registration(registry.RegisteredComponent{})
	if e != nil {
		t.Fatal(e)
	}
	cr := &registry.RegisteredComponent{Component: leaf.Component, Input: leaf.Input, Output: leaf.Output, OutputType: reflect.TypeFor[scopedNativeChildOutput](), Reader: reader}
	rt, e := NewRuntime([]*registry.RegisteredComponent{pr, cr})
	if e != nil {
		t.Fatal(e)
	}
	defer rt.Shutdown(context.Background())
	route := spec.RouteRef{Method: "GET", Path: "/group-scoped"}
	if _, e = rt.scopedGroupDescriptor(t.Context(), pr, route, dexec.ComponentRequest{}, true); e != nil {
		t.Fatal(e)
	}
	wrapped := *pr
	wrapped.Providers = append([]locator.Provider(nil), pr.Providers...)
	if groupRegistration(&wrapped) != groupRegistration(pr) {
		t.Fatal("fresh registered wrapper changed compiled identity")
	}
	for _, mode := range []string{"input mismatch", "output mismatch", "raw handler", "source", "mutation", "protected", "sequence", "independent", "provider", "reader options", "completion", "binding output"} {
		t.Run(mode, func(t *testing.T) {
			c := *pr
			component := *pr.Component
			c.Component = &component
			request := dexec.ComponentRequest{}
			switch mode {
			case "input mismatch":
				c.Input = leaf.Input
			case "output mismatch":
				c.OutputType = reflect.TypeFor[int]()
			case "raw handler":
				c.Handler = rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) { t.Fatal("raw handler executed"); return nil, nil })
			case "source":
				component.RootView = &spec.View{Name: "source", Source: &spec.ViewSource{SQL: "SELECT 1"}}
			case "mutation":
				component.Settings = &spec.Settings{Mutation: "auto"}
			case "protected":
				component.Settings = &spec.Settings{ProtectedFlushTables: []string{"records"}}
			case "sequence":
				component.Settings = &spec.Settings{SequenceStrategy: "reserve"}
			case "independent":
				component.Settings = &spec.Settings{IndependentChildTransactions: true}
			case "provider":
				c.Providers = []locator.Provider{hp.Named("component", func(context.Context, reflect.Type, string) (any, bool, error) {
					t.Fatal("override called")
					return nil, false, nil
				})}
			case "reader options":
				request.ReaderOptions = &dexec.ReaderOptions{}
			case "completion":
				request.Completion = func(xhandler.Outcome) {}
			case "binding output":
				request.BindingOutput = new(int)
			}
			if _, e := rt.scopedGroupDescriptor(t.Context(), &c, route, request, true); !errors.Is(e, drainowner.ErrBindingGroup) {
				t.Fatalf("admitted %s %v", mode, e)
			}
		})
	}
}
