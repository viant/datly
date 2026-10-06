package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	requestprovider "github.com/viant/bindly/provider/request"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	engine "github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
	xh "github.com/viant/xdatly/handler"
)

type bufferedCallRow struct {
	ID    int    `sqlx:"id,primaryKey=true" json:"id"`
	Label string `sqlx:"label" json:"label"`
}
type bufferedCallInput struct {
	Rows        []*bufferedCallRow `parameter:"Rows,kind=body,in=data" view:"Rows,table=records" json:"data"`
	CurrentRows []*bufferedCallRow `parameter:"CurrentRows,kind=view,in=CurrentRows" view:"CurrentRows,table=records" sql:"SELECT id,label FROM records WHERE 1=0" json:"-"`
}
type bufferedCallOutput struct {
	Data []*bufferedCallRow `parameter:"Data,kind=output,in=body" json:"data"`
}

func TestInjectedBufferedNativeWriterCallSiteOrder(t *testing.T) {
	for _, mode := range []string{"buffered", "buffered failure", "ordinary", "nested replaced context", "binding nested replaced context", "overlapping buffered children", "request independent", "caller transaction", "caller failure", "source-less caller", "callee imperative", "callee independent", "root independent"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)", "CREATE TABLE sqlx_sequence_reservations(table_name TEXT PRIMARY KEY,value INTEGER NOT NULL CHECK(value >= 0))", "CREATE TABLE journal_order(seq INTEGER PRIMARY KEY AUTOINCREMENT,id INTEGER NOT NULL)", "CREATE TRIGGER record_order AFTER INSERT ON records BEGIN INSERT INTO journal_order(id) VALUES(new.id); END"); err != nil {
				t.Fatal(err)
			}
			var callerTx *sql.Tx
			if mode == "caller transaction" || mode == "caller failure" {
				var err error
				callerTx, err = db.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer callerTx.Rollback()
				if _, err = callerTx.ExecContext(ctx, "INSERT INTO records VALUES(99,'caller prior work')"); err != nil {
					t.Fatal(err)
				}
			}
			var retained dexec.ComponentInvoker
			childSpec := componentSpec("BufferedChild", "PATCH", "/buffered-child", nil)
			childSpec.Settings = &spec.Settings{Mutation: "patch"}
			if mode == "callee imperative" {
				childSpec.Settings.ComponentCallPolicy = "imperative"
			}
			if mode == "callee independent" {
				childSpec.Settings.IndependentChildTransactions = true
			}
			childSpec.RootView = &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "label", Source: "label"}}}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: childSpec, InputType: reflect.TypeFor[bufferedCallInput](), OutputType: reflect.TypeFor[bufferedCallOutput]()})
			if err != nil {
				t.Fatal(err)
			}
			native, err := writer.New(artifact.Component, reflect.TypeFor[bufferedCallInput](), reflect.TypeFor[bufferedCallOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			views, err := artifact.NewViewProvider(bootstrap.ViewRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
			if err != nil {
				t.Fatal(err)
			}
			child := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[bufferedCallOutput](), Handler: native, Providers: []locator.Provider{views}, DataSource: dml.Source{DB: db.DB}}
			var entered chan struct{}
			var release chan struct{}
			if mode == "overlapping buffered children" {
				entered = make(chan struct{}, 2)
				release = make(chan struct{})
				child.Handler = &bufferedConcurrentNative{Handler: native, entered: entered, release: release}
			}
			parentSpec := componentSpec("BufferedParent", "POST", "/buffered-parent", nil)
			if mode != "ordinary" {
				parentSpec.Settings = &spec.Settings{ComponentCallPolicy: "buffered"}
			}
			parentArtifact := componentArtifact(t, parentSpec, reflect.TypeFor[struct{}](), reflect.TypeFor[bufferedCallOutput]())
			childTarget := dexec.ComponentTarget{Component: childSpec.Key, Route: spec.RouteRef{Method: "PATCH", Path: "/buffered-child"}}
			sentinel := errors.New("parent failure after successful child")
			var nested *registry.RegisteredComponent
			nestedTarget := dexec.ComponentTarget{}
			if mode == "nested replaced context" || mode == "binding nested replaced context" {
				nestedSpec := componentSpec("BufferedNested", "POST", "/buffered-nested", nil)
				nestedArtifact := componentArtifact(t, nestedSpec, reflect.TypeFor[struct{}](), reflect.TypeFor[bufferedCallOutput]())
				nestedTarget = dexec.ComponentTarget{Component: nestedSpec.Key, Route: spec.RouteRef{Method: "POST", Path: "/buffered-nested"}}
				nested = &registry.RegisteredComponent{Component: nestedArtifact.Component, Input: nestedArtifact.Input, OutputType: reflect.TypeFor[bufferedCallOutput](), Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
					value, found, err := inv.Binder.Lookup(ctx, dexec.ComponentInvokerKey)
					if err != nil || !found {
						return nil, fmt.Errorf("nested invoker: %v", err)
					}
					return value.(dexec.ComponentInvoker).InvokeComponent(context.Background(), dexec.ComponentRequest{Target: childTarget, Input: &bufferedCallInput{Rows: []*bufferedCallRow{{ID: 2, Label: "child"}}}})
				})}
			}
			parent := &registry.RegisteredComponent{Component: parentArtifact.Component, Input: parentArtifact.Input, OutputType: reflect.TypeFor[bufferedCallOutput](), DataSource: dml.Source{DB: db.DB}, Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				var data xh.Data
				if mode != "source-less caller" {
					value, found, err := inv.Binder.Lookup(ctx, xh.DataKey)
					if err != nil || !found {
						return nil, fmt.Errorf("parent data: %v", err)
					}
					data = value.(xh.Data)
					if err = data.Execute("INSERT INTO records VALUES(1,'parent before')"); err != nil {
						return nil, err
					}
				}
				value, found, err := inv.Binder.Lookup(ctx, dexec.ComponentInvokerKey)
				if err != nil || !found {
					return nil, fmt.Errorf("parent invoker: %v", err)
				}
				invoker := value.(dexec.ComponentInvoker)
				retained = invoker
				if mode == "overlapping buffered children" {
					callCtx := ctx
					admissionTimeout := time.NewTimer(5 * time.Second)
					defer admissionTimeout.Stop()
					results := make(chan error, 2)
					for _, id := range []int{2, 4} {
						go func(id int) {
							value, err := invoker.InvokeComponent(callCtx, dexec.ComponentRequest{Target: childTarget, Input: &bufferedCallInput{Rows: []*bufferedCallRow{{ID: id, Label: "overlap"}}}})
							if err == nil {
								out := value.(*bufferedCallOutput)
								if len(out.Data) != 1 || out.Data[0].ID != id {
									err = fmt.Errorf("wrong concurrent native output %v", out)
								}
							}
							results <- err
						}(id)
					}
					var admissionErr error
					consumed := 0
					for n := 0; n < 2; n++ {
						select {
						case <-entered:
						case admissionErr = <-results:
							consumed++
							n = 2
						case <-admissionTimeout.C:
							admissionErr = fmt.Errorf("native concurrent child admission timed out")
							n = 2
						}
					}
					tx, err := dexec.InvocationTransaction(ctx, db.DB)
					count := 0
					if err == nil && tx != nil {
						err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM journal_order").Scan(&count)
					}
					if err == nil && count != 0 {
						err = fmt.Errorf("concurrent admitted children drained early: %d", count)
					}
					close(release)
					if admissionErr != nil {
						for n := consumed; n < 2; n++ {
							admissionErr = errors.Join(admissionErr, <-results)
						}
						return nil, admissionErr
					}
					for n := 0; n < 2; n++ {
						if childErr := <-results; childErr != nil {
							err = errors.Join(err, childErr)
						}
					}
					if err != nil {
						return nil, err
					}
					tx, err = dexec.InvocationTransaction(ctx, db.DB)
					if err == nil && tx != nil {
						err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM journal_order").Scan(&count)
					}
					if err != nil {
						return nil, err
					}
					if count != 0 {
						return nil, fmt.Errorf("joined children drained early: %d", count)
					}
					if err = data.Execute("INSERT INTO records VALUES(3,'parent after joined children')"); err != nil {
						return nil, err
					}
					return &bufferedCallOutput{}, nil
				}
				request := dexec.ComponentRequest{Target: childTarget, Input: &bufferedCallInput{Rows: []*bufferedCallRow{{ID: 2, Label: "child"}}}}
				if mode == "nested replaced context" || mode == "binding nested replaced context" {
					request = dexec.ComponentRequest{Target: nestedTarget, Input: &struct{}{}}
				}
				if mode == "request independent" {
					request.IndependentChildTransactions = true
				}
				var result any
				if mode == "binding nested replaced context" {
					var dependency struct {
						Child *bufferedCallOutput `bind:"kind=component,in=POST:/buffered-nested,required"`
					}
					err = inv.Binder.Bind(ctx, &dependency)
					result = dependency.Child
				} else {
					result, err = invoker.InvokeComponent(ctx, request)
				}
				if mode == "request independent" {
					if err == nil {
						t.Fatal("independent call accepted")
					}
					return nil, err
				}
				if err != nil {
					return nil, err
				}
				output := result.(*bufferedCallOutput)
				if len(output.Data) != 1 || output.Data[0].ID != 2 || output.Data[0].Label != "child" {
					t.Fatalf("native output %+v", output)
				}
				assertPending := func(want int) error {
					tx, err := dexec.InvocationTransaction(ctx, db.DB)
					if err != nil {
						return err
					}
					var count int
					if tx != nil {
						err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM journal_order").Scan(&count)
					} else {
						err = db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM journal_order").Scan(&count)
					}
					if err != nil {
						return err
					}
					if count != want {
						return fmt.Errorf("early physical actions %d want%d", count, want)
					}
					return nil
				}
				pending := 0
				if callerTx != nil {
					pending = 1
				}
				if mode == "ordinary" {
					pending = 2
				}
				if err = assertPending(pending); err != nil {
					return nil, err
				}
				if data != nil {
					if err = data.Execute("INSERT INTO records VALUES(3,'parent after')"); err != nil {
						return nil, err
					}
				}
				// The retained invoker must keep buffering even with a replaced context.
				secondContext := context.Background()
				if mode == "ordinary" {
					secondContext = ctx
				}
				_, err = invoker.InvokeComponent(secondContext, dexec.ComponentRequest{Target: childTarget, Input: &bufferedCallInput{Rows: []*bufferedCallRow{{ID: 4, Label: "second child"}}}})
				if err != nil {
					return nil, err
				}
				pending = 0
				if callerTx != nil {
					pending = 1
				}
				if mode == "ordinary" {
					pending = 4
				}
				if err = assertPending(pending); err != nil {
					return nil, err
				}
				if mode == "buffered failure" || mode == "caller failure" {
					return nil, sentinel
				}
				return output, nil
			})}
			if callerTx != nil {
				parent.DataSource = dml.Source{DB: db.DB, Tx: callerTx}
			}
			if mode == "source-less caller" {
				parent.DataSource = nil
			}
			entries := []*registry.RegisteredComponent{parent, child}
			if nested != nil {
				entries = append(entries, nested)
			}
			runtime, err := NewRuntime(entries)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Shutdown(ctx)
			rootRequest := dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: parentSpec.Key, Route: spec.RouteRef{Method: "POST", Path: "/buffered-parent"}}, Input: &struct{}{}}
			if mode == "root independent" {
				rootRequest.IndependentChildTransactions = true
			}
			_, err = runtime.InvokeComponent(ctx, rootRequest)
			failure := mode == "buffered failure" || mode == "caller failure" || mode == "callee imperative" || strings.Contains(mode, "independent")
			if (err != nil) != failure {
				t.Fatalf("root error %v", err)
			}
			if (mode == "buffered failure" || mode == "caller failure") && !errors.Is(err, sentinel) {
				t.Fatalf("lost parent cause %v", err)
			}
			if mode != "ordinary" && retained != nil {
				if _, lateErr := retained.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: childTarget, Input: &bufferedCallInput{Rows: []*bufferedCallRow{{ID: 5, Label: "late"}}}}); lateErr == nil {
					t.Fatal("retained invoker escaped completed root")
				}
			}
			var rows *sql.Rows
			if callerTx != nil {
				rows, err = callerTx.QueryContext(ctx, "SELECT id FROM journal_order ORDER BY seq")
			} else {
				rows, err = db.DB.QueryContext(ctx, "SELECT id FROM journal_order ORDER BY seq")
			}

			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var order []int
			for rows.Next() {
				var id int
				if err = rows.Scan(&id); err != nil {
					t.Fatal(err)
				}
				order = append(order, id)
			}
			if err = rows.Err(); err != nil {
				t.Fatal(err)
			}
			expected := []int{1, 2, 3, 4}
			if mode == "binding nested replaced context" {
				expected = []int{1, 3, 4, 2}
			}
			if mode == "source-less caller" {
				expected = []int{2, 4}
			}
			if failure {
				expected = nil
			}
			if callerTx != nil {
				expected = append([]int{99}, expected...)
			}
			if mode == "overlapping buffered children" {
				if !reflect.DeepEqual(order, []int{1, 2, 4, 3}) && !reflect.DeepEqual(order, []int{1, 4, 2, 3}) {
					t.Fatalf("concurrent call-site order %v", order)
				}
				expected = order
			}
			if !reflect.DeepEqual(order, expected) {
				t.Fatalf("call-site order %v want%v", order, expected)
			}
			if callerTx != nil {
				rows.Close()
				if err = callerTx.Rollback(); err != nil {
					t.Fatal(err)
				}
				var count int
				if err = db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 0 {
					t.Fatalf("caller rollback count%d err%v", count, err)
				}
			}
		})
	}
}

func TestBufferedCallAdmissionRejectsBeforeBusiness(t *testing.T) {
	for _, settings := range []*spec.Settings{{ComponentCallPolicy: "unknown"}, {ComponentCallPolicy: "buffered", IndependentChildTransactions: true}} {
		component := componentSpec("InvalidCallPolicy", "POST", "/invalid-call-policy", nil)
		component.Settings = settings
		if _, err := NewRuntime([]*registry.RegisteredComponent{{Component: component}}); err == nil || !strings.Contains(err.Error(), "component call") {
			t.Fatalf("invalid registration admission: %v", err)
		}
	}
	component := componentSpec("UnsupportedBufferedOwner", "POST", "/unsupported-buffered-owner", nil)
	component.Settings = &spec.Settings{ComponentCallPolicy: "buffered"}
	artifact := componentArtifact(t, component, reflect.TypeFor[struct{}](), reflect.TypeFor[struct{}]())
	calls := 0
	source := &componentGuardSource{data: &componentGuardData{}}
	runtime, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[struct{}](), DataSource: source, Handler: rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) { calls++; return &struct{}{}, nil })}})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Shutdown(context.Background())
	_, err = runtime.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "POST", Path: "/unsupported-buffered-owner"}}, Input: &struct{}{}})
	if err == nil || !strings.Contains(err.Error(), "journal ownership") || calls != 0 || source.data.writes != 0 {
		t.Fatalf("unsupported owner reached work: calls%d writes%d error%v", calls, source.data.writes, err)
	}
}

func TestBufferedRelationMarkerCannotReplaceCallerPolicy(t *testing.T) {
	component := componentSpec("ForgedBufferedCaller", "POST", "/forged-buffered-caller", nil)
	artifact := componentArtifact(t, component, reflect.TypeFor[struct{}](), reflect.TypeFor[struct{}]())
	calls := 0
	runtime, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[struct{}](), Handler: rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) { calls++; return &struct{}{}, nil })}})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Shutdown(context.Background())
	ctx := engine.PrepareComponent(context.Background(), engine.ComponentBufferedImperative, "")
	_, err = runtime.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "POST", Path: "/forged-buffered-caller"}}, Input: &struct{}{}})
	if err == nil || !strings.Contains(err.Error(), "canonical caller ownership") || calls != 0 {
		t.Fatalf("forged relation admission calls%d err%v", calls, err)
	}
}

func TestBufferedEmptyRootClosesRetainedInvoker(t *testing.T) {
	for _, mode := range []string{"buffered root", "ordinary root buffered child", "ordinary root invoker original context", "ordinary root invoker replaced context", "ordinary root parent failure"} {
		t.Run(mode, func(t *testing.T) {
			var retained, rootRetained dexec.ComponentInvoker
			var rootContext context.Context
			parentFailure := errors.New("parent failed after independent sibling commit")
			db := sqlite.New(t)
			if err := db.ExecStatements(context.Background(), "CREATE TABLE active_records(id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			calls := 0
			makeComponent := func(name, path, policy string, handler rh.Handler) *registry.RegisteredComponent {
				s := componentSpec(name, "POST", path, nil)
				if policy != "" {
					s.Settings = &spec.Settings{ComponentCallPolicy: policy}
				}
				a := componentArtifact(t, s, reflect.TypeFor[struct{}](), reflect.TypeFor[struct{}]())
				return &registry.RegisteredComponent{Component: a.Component, Input: a.Input, OutputType: reflect.TypeFor[struct{}](), Handler: handler}
			}
			late := makeComponent("LateCustomChild", "/late-custom-child", "", rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) { calls++; return &struct{}{}, nil }))
			capture := rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				value, found, err := inv.Binder.Lookup(ctx, dexec.ComponentInvokerKey)
				if err != nil || !found {
					return nil, fmt.Errorf("capture invoker: %v", err)
				}
				retained = value.(dexec.ComponentInvoker)
				return &struct{}{}, nil
			})
			active := makeComponent("ActiveOrdinarySibling", "/active-ordinary-sibling", "", rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				if engine.IsBufferedComponent(ctx) {
					return nil, fmt.Errorf("ordinary sibling policy promoted")
				}
				value, found, err := inv.Binder.Lookup(ctx, xh.DataKey)
				if err != nil || !found {
					return nil, fmt.Errorf("active sibling data: %v", err)
				}
				if err = value.(xh.Data).Execute("INSERT INTO active_records VALUES(99)"); err != nil {
					return nil, err
				}
				return &struct{}{}, nil
			}))
			active.DataSource = dml.Source{DB: db.DB}
			child := makeComponent("EmptyBufferedChild", "/empty-buffered-child", "buffered", capture)
			root := makeComponent("EmptyBufferedRoot", "/empty-buffered-root", "buffered", capture)
			if mode != "buffered root" {
				root = makeComponent("OrdinaryRoot", "/ordinary-root", "", rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
					value, found, err := inv.Binder.Lookup(ctx, dexec.ComponentInvokerKey)
					if err != nil || !found {
						return nil, fmt.Errorf("root invoker: %v", err)
					}
					rootRetained = value.(dexec.ComponentInvoker)
					rootContext = ctx
					result, err := rootRetained.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: child.Component.Key, Route: spec.RouteRef{Method: "POST", Path: "/empty-buffered-child"}}, Input: &struct{}{}})
					if err != nil {
						return nil, err
					}
					_, err = rootRetained.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: active.Component.Key, Route: spec.RouteRef{Method: "POST", Path: "/active-ordinary-sibling"}}, Input: &struct{}{}})
					if err != nil {
						return nil, err
					}
					count := 0
					err = db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM active_records").Scan(&count)
					if err != nil || count != 1 {
						return nil, fmt.Errorf("active ordinary replaced-context sibling lost independent ownership: count%d err%v", count, err)
					}
					if mode == "ordinary root parent failure" {
						return result, parentFailure
					}
					return result, nil
				}))
			}
			if mode != "buffered root" {
				root.DataSource = dml.Source{DB: db.DB}
			}
			rt, err := NewRuntime([]*registry.RegisteredComponent{root, child, late, active})
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Shutdown(context.Background())
			path := "/empty-buffered-root"
			if mode != "buffered root" {
				path = "/ordinary-root"
			}
			_, err = rt.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: root.Component.Key, Route: spec.RouteRef{Method: "POST", Path: path}}, Input: &struct{}{}})
			if mode == "ordinary root parent failure" {
				if !errors.Is(err, parentFailure) {
					t.Fatalf("parent error identity lost: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if mode != "buffered root" {
				count := 0
				if err := db.DB.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM active_records").Scan(&count); err != nil || count != 1 {
					t.Fatalf("independent sibling commit lost after parent completion: count%d err%v", count, err)
				}
			}
			if retained == nil {
				t.Fatal("missing retained invoker")
			}
			callContext := context.Background()
			if strings.Contains(mode, "root invoker") {
				retained = rootRetained
				if strings.Contains(mode, "original") {
					callContext = rootContext
				}
			}
			_, err = retained.InvokeComponent(callContext, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: late.Component.Key, Route: spec.RouteRef{Method: "POST", Path: "/late-custom-child"}}, Input: &struct{}{}})
			if err == nil || calls != 0 {
				t.Fatalf("late business escaped completed root: calls=%d err=%v", calls, err)
			}
		})
	}
}

// This view exposes the native component journal while hiding the actual owner's invocation lifecycle.
type bufferedLifecycleHiddenView struct {
	xh.Data
	native *dml.Data
}

func (v *bufferedLifecycleHiddenView) ComponentData(relation, order string) xh.Data {
	return &bufferedLifecycleHiddenView{Data: v.native.ComponentData(relation, order), native: v.native}
}
func (v *bufferedLifecycleHiddenView) SealComponent() { v.native.SealComponent() }

type bufferedLifecycleHiddenSource struct{ db *sql.DB }

func (s *bufferedLifecycleHiddenSource) Open(context.Context) (xh.Data, error) {
	native := dml.NewData(s.db)
	return &bufferedLifecycleHiddenView{Data: native, native: native}, nil
}
func (s *bufferedLifecycleHiddenSource) InvocationKey() any { return s.db }
func TestBufferedOwnerRejectsLifecycleHidingWrapper(t *testing.T) {
	db := sqlite.New(t)
	component := componentSpec("HiddenLifecycle", "POST", "/hidden-lifecycle", nil)
	component.Settings = &spec.Settings{ComponentCallPolicy: "buffered"}
	a := componentArtifact(t, component, reflect.TypeFor[struct{}](), reflect.TypeFor[struct{}]())
	calls := 0
	rt, err := NewRuntime([]*registry.RegisteredComponent{{Component: a.Component, Input: a.Input, OutputType: reflect.TypeFor[struct{}](), DataSource: &bufferedLifecycleHiddenSource{db: db.DB}, Handler: rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) { calls++; return &struct{}{}, nil })}})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(context.Background())
	_, err = rt.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "POST", Path: "/hidden-lifecycle"}}, Input: &struct{}{}})
	if err == nil || calls != 0 {
		t.Fatalf("lifecycle hiding owner reached business: calls%d err%v", calls, err)
	}
}

// Preserve all native handler capabilities; only its execution entrance is synchronized.
type bufferedConcurrentNative struct {
	*writer.Handler
	entered chan<- struct{}
	release <-chan struct{}
}

func (h *bufferedConcurrentNative) Execute(ctx context.Context, inv rh.Invocation) (any, error) {
	select {
	case h.entered <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-h.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return h.Handler.Execute(ctx, inv)
}

type bufferedInjectorNativeOutput struct {
	Data []*bufferedCallRow
	hook func(context.Context, xh.InjectorLookup) error
}

func (o *bufferedInjectorNativeOutput) Finalize(ctx context.Context, lookup xh.InjectorLookup) error {
	return o.hook(ctx, lookup)
}
func TestBufferedInjectorNativeWriterNoEarlyDrain(t *testing.T) {
	for _, mode := range []string{"owned", "callback failure", "caller transaction", "caller callback failure", "ignored child failure", "callback panic", "cancelled callback"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)", "CREATE TABLE sqlx_sequence_reservations(table_name TEXT PRIMARY KEY,value INTEGER NOT NULL CHECK(value >= 0))", "CREATE TABLE journal_order(seq INTEGER PRIMARY KEY AUTOINCREMENT,id INTEGER NOT NULL)", "CREATE TRIGGER record_order AFTER INSERT ON records BEGIN INSERT INTO journal_order(id) VALUES(new.id); END"); err != nil {
				t.Fatal(err)
			}
			var callerTx *sql.Tx
			if strings.HasPrefix(mode, "caller") {
				var err error
				callerTx, err = db.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer callerTx.Rollback()
				if _, err = callerTx.ExecContext(ctx, "INSERT INTO records VALUES(99,'prior')"); err != nil {
					t.Fatal(err)
				}
			}
			childSpec := componentSpec("InjectorNativeChild", "PATCH", "/injector-native-child", nil)
			childSpec.Settings = &spec.Settings{Mutation: "patch"}
			childSpec.RootView = &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "label", Source: "label"}}}
			a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: childSpec, InputType: reflect.TypeFor[bufferedCallInput](), OutputType: reflect.TypeFor[bufferedCallOutput]()})
			if err != nil {
				t.Fatal(err)
			}
			native, err := writer.New(a.Component, reflect.TypeFor[bufferedCallInput](), reflect.TypeFor[bufferedCallOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			views, err := a.NewViewProvider(bootstrap.ViewRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
			if err != nil {
				t.Fatal(err)
			}
			child := &registry.RegisteredComponent{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[bufferedCallOutput](), Handler: native, Providers: []locator.Provider{views}, DataSource: dml.Source{DB: db.DB}}
			parentSpec := componentSpec("InjectorNativeParent", "POST", "/injector-native-parent", nil)
			parentSpec.Settings = &spec.Settings{ComponentCallPolicy: "buffered"}
			pa := componentArtifact(t, parentSpec, reflect.TypeFor[struct{}](), reflect.TypeFor[bufferedInjectorNativeOutput]())
			var retained xh.Binder
			sentinel := errors.New("callback business failure")
			failure := strings.Contains(mode, "failure") || mode == "callback panic" || mode == "cancelled callback"
			if mode == "ignored child failure" {
				child.Handler = &bufferedInjectorFaultNative{Handler: native, cause: sentinel}
			}
			parent := &registry.RegisteredComponent{Component: pa.Component, Input: pa.Input, OutputType: reflect.TypeFor[bufferedInjectorNativeOutput](), DataSource: dml.Source{DB: db.DB, Tx: callerTx}, Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				value, found, err := inv.Binder.Lookup(ctx, xh.DataKey)
				if err != nil || !found {
					return nil, fmt.Errorf("parent data %v", err)
				}
				data := value.(xh.Data)
				if err = data.Execute("INSERT INTO records VALUES(1,'parent before')"); err != nil {
					return nil, err
				}
				output := &bufferedInjectorNativeOutput{}
				output.hook = func(ctx context.Context, lookup xh.InjectorLookup) error {
					checkPending := func() error {
						tx, err := dexec.InvocationTransaction(ctx, db.DB)
						if err != nil {
							return err
						}
						count := 0
						if tx != nil {
							err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM journal_order").Scan(&count)
						} else {
							err = db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM journal_order").Scan(&count)
						}
						want := 0
						if callerTx != nil {
							want = 1
						}
						if err == nil && count != want {
							err = fmt.Errorf("injector callback early SQL %d want%d", count, want)
						}
						return err
					}
					if err := checkPending(); err != nil {
						return err
					}
					binder, err := lookup(context.Background(), xh.Route{Method: "PATCH", URL: "/injector-native-child"})
					if err != nil {
						return err
					}
					retained = binder
					callContext := context.Background()
					if mode == "cancelled callback" {
						cancelled, cancel := context.WithCancel(callContext)
						cancel()
						callContext = cancelled
					}
					value, found, err := binder.Lookup(callContext, xh.ResultKey)
					if (mode == "ignored child failure" || mode == "cancelled callback") && err != nil {
						return nil
					}
					if err != nil || !found {
						return fmt.Errorf("native injector result: %v", err)
					}
					childOutput := value.(*bufferedCallOutput)
					if len(childOutput.Data) != 1 || childOutput.Data[0].ID != 2 {
						return fmt.Errorf("native injector body not hydrated: %+v", childOutput)
					}
					output.Data = childOutput.Data
					if err := checkPending(); err != nil {
						return err
					}
					if err = data.Execute("INSERT INTO records VALUES(3,'parent after')"); err != nil {
						return err
					}
					if mode == "callback panic" {
						panic(sentinel)
					}
					if failure {
						return sentinel
					}
					return nil
				}
				return output, nil
			})}
			rt, err := NewRuntime([]*registry.RegisteredComponent{parent, child})
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Shutdown(ctx)
			req := httptest.NewRequest("POST", "/injector-native-parent", strings.NewReader(`{"data":[{"id":2,"label":"child"}]}`))
			req.Header.Set("Content-Type", "application/json")
			scope, err := requestprovider.New(req)
			if err != nil {
				t.Fatal(err)
			}
			_, err = rt.ExecuteRoute(ctx, "POST", "/injector-native-parent", scope)
			if mode == "callback panic" {
				var panicErr *dexec.PanicError
				if !errors.As(err, &panicErr) || panicErr.Cause() != sentinel {
					t.Fatalf("private panic identity %v", err)
				}
			} else if mode == "cancelled callback" {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("callback cancellation %v", err)
				}
			} else if failure {
				if !errors.Is(err, sentinel) {
					t.Fatalf("callback failure identity %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if retained != nil {
				if _, _, err = retained.Lookup(ctx, xh.ResultKey); err == nil {
					t.Fatal("callback binder remained live")
				}
			}
			count := 0
			if callerTx != nil {
				err = callerTx.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count)
			} else {
				err = db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count)
			}
			want := 3
			if failure {
				want = 0
			}
			if callerTx != nil {
				want++
			}
			if err != nil || count != want {
				t.Fatalf("outcome count%d want%d err%v", count, want, err)
			}
			var rows *sql.Rows
			if callerTx != nil {
				rows, err = callerTx.QueryContext(ctx, "SELECT id FROM journal_order ORDER BY seq")
			} else {
				rows, err = db.DB.QueryContext(ctx, "SELECT id FROM journal_order ORDER BY seq")
			}
			if err != nil {
				t.Fatal(err)
			}
			var order []int
			for rows.Next() {
				var id int
				if err = rows.Scan(&id); err != nil {
					t.Fatal(err)
				}
				order = append(order, id)
			}
			if err = rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			expected := []int{1, 2, 3}
			if failure {
				expected = nil
			}
			if callerTx != nil {
				expected = append([]int{99}, expected...)
			}
			if !reflect.DeepEqual(order, expected) {
				t.Fatalf("injector call-site order %v want%v", order, expected)
			}
			if callerTx != nil {
				if _, err = callerTx.ExecContext(ctx, "INSERT INTO records VALUES(100,'caller remains usable')"); err != nil {
					t.Fatal(err)
				}
				if err = callerTx.Rollback(); err != nil {
					t.Fatal(err)
				}
				if err = db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 0 {
					t.Fatalf("caller rollback count%d err%v", count, err)
				}
			}

		})
	}
}

type bufferedInjectorFaultNative struct {
	*writer.Handler
	cause error
}

func (h *bufferedInjectorFaultNative) Execute(ctx context.Context, inv rh.Invocation) (any, error) {
	result, err := h.Handler.Execute(ctx, inv)
	if err != nil {
		return result, err
	}
	return result, h.cause
}
