package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/drainowner"
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

// Two body paths from one real protocol request provide distinct native child
// bodies. They are ordinary native graph bindings, not fabricated input frames.
type lease49FirstInput struct {
	Rows        []*bufferedCallRow `parameter:"Rows,kind=body,in=first" view:"Rows,table=records"`
	CurrentRows []*bufferedCallRow `parameter:"CurrentRows,kind=view,in=CurrentRows" view:"CurrentRows,table=records" sql:"SELECT id,label FROM records WHERE 1=0"`
}
type lease49SecondInput struct {
	Rows        []*bufferedCallRow `parameter:"Rows,kind=body,in=second" view:"Rows,table=records"`
	CurrentRows []*bufferedCallRow `parameter:"CurrentRows,kind=view,in=CurrentRows" view:"CurrentRows,table=records" sql:"SELECT id,label FROM records WHERE 1=0"`
}
type lease49Output struct {
	Children []*bufferedCallOutput
	hook     func(context.Context, xh.InjectorLookup) error
}

func (o *lease49Output) Finalize(ctx context.Context, lookup xh.InjectorLookup) error {
	return o.hook(ctx, lookup)
}

// Embed the entire native ABI; observe only the actual executing child context.
type lease49NativeTrace struct {
	*writer.Handler
	ctx context.Context
}

func (h *lease49NativeTrace) Execute(ctx context.Context, inv rh.Invocation) (any, error) {
	h.ctx = ctx
	return h.Handler.Execute(ctx, inv)
}
func lease49Child(t *testing.T, name, path, policy string, inputType reflect.Type, db *sqlite.Harness) (*registry.RegisteredComponent, *lease49NativeTrace) {
	t.Helper()
	s := componentSpec(name, "PATCH", path, nil)
	s.Settings = &spec.Settings{Mutation: "patch", ComponentCallPolicy: policy}
	s.RootView = &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "label", Source: "label"}}}
	a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: s, InputType: inputType, OutputType: reflect.TypeFor[bufferedCallOutput]()})
	if err != nil {
		t.Fatal(err)
	}
	h, err := writer.New(a.Component, inputType, reflect.TypeFor[bufferedCallOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	v, err := a.NewViewProvider(bootstrap.ViewRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
	if err != nil {
		t.Fatal(err)
	}
	trace := &lease49NativeTrace{Handler: h}
	return &registry.RegisteredComponent{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[bufferedCallOutput](), Handler: trace, Providers: []locator.Provider{v}, DataSource: dml.Source{DB: db.DB}}, trace
}
func lease49Rows(t *testing.T, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) []int {
	t.Helper()
	rows, err := q.QueryContext(context.Background(), "SELECT id FROM journal_order ORDER BY seq")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
func lease49Run(t *testing.T, late, twoDB, caller bool, fault string, ordinarySecond, envelope bool) {
	t.Helper()
	ctx := context.Background()
	db := activity49DB(t)
	other := db
	if twoDB {
		other = activity49DB(t)
	}
	var tx *sql.Tx
	if caller {
		var err error
		tx, err = db.DB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tx.Rollback() })
		if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(99,'caller prior')"); err != nil {
			t.Fatal(err)
		}
	}
	first, firstTrace := lease49Child(t, "LeaseNativeFirst", "/lease-native-first", "buffered", reflect.TypeFor[lease49FirstInput](), db)
	secondPolicy := "buffered"
	if ordinarySecond {
		secondPolicy = ""
	}
	second, secondTrace := lease49Child(t, "LeaseNativeSecond", "/lease-native-second", secondPolicy, reflect.TypeFor[lease49SecondInput](), other)
	var physical []*registry.RegisteredComponent
	if envelope {
		physical = []*registry.RegisteredComponent{first, second}
		first = lease49Envelope(t, "LeaseFirstEnvelope", "/lease-first-envelope", first)
		second = lease49Envelope(t, "LeaseSecondEnvelope", "/lease-second-envelope", second)
	}
	policy := "buffered"
	if late {
		policy = ""
	}
	parentSpec := componentSpec("LeaseNativeRoot", "POST", "/lease-native-root", nil)
	parentSpec.Settings = &spec.Settings{ComponentCallPolicy: policy}
	a := componentArtifact(t, parentSpec, reflect.TypeFor[struct{}](), reflect.TypeFor[lease49Output]())
	sentinel := errors.New("real composition veto after native child")
	var retained xh.Binder
	var retainedLookup xh.InjectorLookup
	callbackEntered, firstFinished, secondFinished := false, false, false
	directRejected := late && !envelope
	var directProbe error
	parent := &registry.RegisteredComponent{Component: a.Component, Input: a.Input, OutputType: reflect.TypeFor[lease49Output](), DataSource: dml.Source{DB: db.DB, Tx: tx}, Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
		if engine.IsBufferedComponent(ctx) == late {
			return nil, fmt.Errorf("root ordinary/buffered policy changed")
		}
		value, ok, err := inv.Binder.Lookup(ctx, xh.DataKey)
		if err != nil || !ok {
			return nil, fmt.Errorf("actual root data: %v", err)
		}
		data := value.(xh.Data)
		if err := data.Execute("INSERT INTO records VALUES(1,'root before')"); err != nil {
			return nil, err
		}
		out := &lease49Output{}
		out.hook = func(ctx context.Context, lookup xh.InjectorLookup) error {
			callbackEntered = true
			retainedLookup = lookup
			pending := func(unit *sqlite.Harness) error {
				n, err := activity49Pending(ctx, unit.DB)
				want := 0
				if unit == db {
					if caller {
						want = 1
					} // Real caller prior row; local preparation skips caller TX.
					if late && !caller {
						want = 1
					} // Ordinary preparation completed this prefix before enrollment.
				}
				if err != nil {
					return err
				}
				if n != want {
					return fmt.Errorf("DESIRED_LEASE_DRAIN_GAP: native SQL inside composing lease rows=%d want=%d twoDB=%t", n, want, twoDB)
				}
				return nil
			}
			if err := pending(db); err != nil {
				return err
			}
			bind, err := lookup(context.Background(), xh.Route{Method: first.Component.Routes[0].Method, URL: first.Component.Routes[0].Path})
			if err != nil {
				return err
			}
			retained = bind
			value, ok, err := bind.Lookup(context.Background(), xh.ResultKey)
			if err != nil || !ok {
				if directRejected {
					directProbe = pending(db)
					if twoDB {
						directProbe = errors.Join(directProbe, pending(other))
					}
				}
				return fmt.Errorf("real first child result: %w", err)
			}
			c := value.(*bufferedCallOutput)
			if len(c.Data) != 1 || c.Data[0].ID != 2 {
				return fmt.Errorf("first native body identity %+v", c)
			}
			out.Children = append(out.Children, c)
			firstFinished = true
			if engine.IsBufferedComponent(ctx) == late {
				return fmt.Errorf("callback child stamped root policy")
			}
			if err := pending(db); err != nil {
				return err
			}
			if fault == "after-first" {
				return sentinel
			}
			if err := data.Execute("INSERT INTO records VALUES(3,'root between')"); err != nil {
				return err
			}
			bind, err = lookup(context.Background(), xh.Route{Method: second.Component.Routes[0].Method, URL: second.Component.Routes[0].Path})
			if err != nil {
				return err
			}
			retained = bind
			call := context.Background()
			if fault == "cancel-second" {
				cancelled, cancel := context.WithCancel(call)
				cancel()
				call = cancelled
			}
			value, ok, err = bind.Lookup(call, xh.ResultKey)
			if err != nil {
				return err
			}
			if !ok {
				return errors.New("real second child absent")
			}
			c = value.(*bufferedCallOutput)
			if len(c.Data) != 1 || c.Data[0].ID != 4 {
				return fmt.Errorf("second native body identity %+v", c)
			}
			out.Children = append(out.Children, c)
			secondFinished = true
			if err := pending(db); err != nil {
				return err
			}
			if twoDB {
				if err := pending(other); err != nil {
					return err
				}
			}
			if err := data.Execute("INSERT INTO records VALUES(5,'root after')"); err != nil {
				return err
			}
			if fault == "after-second" {
				return sentinel
			}
			if fault == "panic-after-second" {
				panic(sentinel)
			}
			return nil
		}
		return out, nil
	})}
	entries := append([]*registry.RegisteredComponent{parent, first, second}, physical...)
	rt := activity49Runtime(t, entries...)
	request := httptest.NewRequest("POST", "/lease-native-root", strings.NewReader(`{"first":[{"id":2,"label":"first"}],"second":[{"id":4,"label":"second"}]}`))
	request.Header.Set("Content-Type", "application/json")
	scope, err := requestprovider.New(request)
	if err != nil {
		t.Fatal(err)
	}
	result, operation := rt.ExecuteRoute(ctx, "POST", "/lease-native-root", scope)
	if directRejected {
		if !callbackEntered || firstFinished || secondFinished || firstTrace.ctx == nil || secondTrace.ctx != nil || !errors.Is(operation, drainowner.ErrDrain) || directProbe != nil {
			t.Fatalf("protected direct callee rejection invalid: callback=%t first=%t second=%t error=%v liveSQL=%v", callbackEntered, firstFinished, secondFinished, operation, directProbe)
		}
		// The callback observed only the admitted ordinary prefix (1), never child
		// SQL. Owning root failure then rolls that prefix back on both databases.
		for _, unit := range []*sqlite.Harness{db, other} {
			if rows := lease49Rows(t, unit.DB); len(rows) != 0 {
				t.Fatalf("direct callee root rollback left rows=%v", rows)
			}
		}
		return
	}

	if !callbackEntered || !firstFinished {
		t.Fatalf("intended real composition stage missing callback=%t first=%t err=%v", callbackEntered, firstFinished, operation)
	}
	if ordinarySecond {
		if operation != nil {
			t.Errorf("native no-early-SQL desired assertion failed: %v", operation)
		}
	} else {
		switch fault {
		case "":
			if operation != nil {
				t.Errorf("composition success failed: %v", operation)
			}
		case "cancel-second":
			if !errors.Is(operation, context.Canceled) {
				t.Errorf("cancellation identity lost: %v", operation)
			}
		case "panic-after-second":
			var p *dexec.PanicError
			if !errors.As(operation, &p) || p.Cause() != sentinel {
				t.Errorf("panic cause lost: %v", operation)
			}
		default:
			if !errors.Is(operation, sentinel) {
				t.Errorf("native child/veto stage error: %v", operation)
			}
		}
	}
	if fault != "after-first" && fault != "cancel-second" && !secondFinished {
		t.Errorf("real second child never finished: %v", operation)
	}
	rootRows := []int(nil)
	if caller {
		rootRows = lease49Rows(t, tx)
	} else {
		rootRows = lease49Rows(t, db.DB)
	}
	var otherRows []int
	if twoDB {
		otherRows = lease49Rows(t, other.DB)
	}
	wantRoot := []int{1, 2, 3, 4, 5}
	wantOther := []int(nil)
	if twoDB {
		wantRoot = []int{1, 2, 3, 5}
		wantOther = []int{4}
	}
	if fault != "" || (ordinarySecond && operation != nil) {
		wantRoot = nil
		wantOther = nil
	}
	if caller {
		wantRoot = append([]int{99}, wantRoot...)
	}
	if !reflect.DeepEqual(rootRows, wantRoot) || !reflect.DeepEqual(otherRows, wantOther) {
		t.Errorf("physical queue order/outcome root=%v want=%v other=%v want=%v operation=%v", rootRows, wantRoot, otherRows, wantOther, operation)
	}
	if operation == nil {
		out, ok := result.(*lease49Output)
		if !ok || len(out.Children) != 2 {
			t.Errorf("real child outputs omitted result=%T", result)
		}
	}
	beforeRoot := append([]int(nil), rootRows...)
	beforeOther := append([]int(nil), otherRows...)
	if retained != nil {
		if _, _, err := retained.Lookup(context.Background(), xh.ResultKey); err == nil {
			t.Error("retained lease remained usable after composition")
		}
	}
	if retainedLookup != nil {
		if _, err := retainedLookup(context.Background(), xh.Route{Method: second.Component.Routes[0].Method, URL: second.Component.Routes[0].Path}); err == nil {
			t.Error("retained lookup admitted new child after lease closed")
		}
	}
	contextStates := make([]string, 0, 2)
	for _, trace := range []*lease49NativeTrace{firstTrace, secondTrace} {
		if trace.ctx == nil {
			contextStates = append(contextStates, "not-executed")
			continue
		}
		contextStates = append(contextStates, fmt.Sprint(trace.ctx.Err()))
		if !errors.Is(trace.ctx.Err(), context.Canceled) {
			t.Errorf("actual borrowed child context not expired: %v", trace.ctx.Err())
		}
		if _, lateErr := rt.ExecuteRoute(trace.ctx, "PATCH", "/lease-native-first", scope); lateErr == nil {
			t.Error("expired native child context admitted fresh canonical invocation")
		}
	}
	afterRoot := []int(nil)
	if caller {
		afterRoot = lease49Rows(t, tx)
	} else {
		afterRoot = lease49Rows(t, db.DB)
	}
	var afterOther []int
	if twoDB {
		afterOther = lease49Rows(t, other.DB)
	}
	if !reflect.DeepEqual(beforeRoot, afterRoot) || !reflect.DeepEqual(beforeOther, afterOther) {
		t.Error("retained lease changed physical rows")
	}
	if caller {
		if _, err := tx.ExecContext(ctx, "INSERT INTO records VALUES(101,'caller owns completion')"); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if rows := lease49Rows(t, db.DB); len(rows) != 0 {
			t.Fatalf("caller rollback rows=%v", rows)
		}
	}
	t.Logf("REAL_NATIVE_INJECTOR_COMPOSITION late=%t twoDB=%t caller=%t fault=%q ordinarySecond=%t first=%t second=%t error=%v rootOrder=%v otherOrder=%v contextStates=%v", late, twoDB, caller, fault, ordinarySecond, firstFinished, secondFinished, operation, rootRows, otherRows, contextStates)
}
func TestNativeInjector49CompositionControls(t *testing.T) {
	for _, late := range []bool{false, true} {
		for _, twoDB := range []bool{false, true} {
			for _, caller := range []bool{false, true} {
				for _, fault := range []string{"", "after-first", "after-second", "cancel-second", "panic-after-second"} {
					t.Run(fmt.Sprintf("late=%t/twoDB=%t/caller=%t/fault=%s", late, twoDB, caller, fault), func(t *testing.T) { lease49Run(t, late, twoDB, caller, fault, false, late) })
				}
			}
		}
	}
}
func TestNativeInjector49ProtectedDirectCalleeRejectsDrain(t *testing.T) {
	for _, twoDB := range []bool{false, true} {
		t.Run(fmt.Sprintf("twoDB=%t", twoDB), func(t *testing.T) { lease49Run(t, true, twoDB, false, "", false, false) })
	}
}

// A real buffered source-less composing component acquires protection and
// invokes its native child through the existing canonical injected invoker.
// Its entry is ordinary; buffered metadata governs genuine child calls.
func lease49Envelope(t *testing.T, name, path string, native *registry.RegisteredComponent) *registry.RegisteredComponent {
	t.Helper()
	s := componentSpec(name, "POST", path, nil)
	s.Settings = &spec.Settings{ComponentCallPolicy: "buffered"}
	a := componentArtifact(t, s, reflect.TypeFor[struct{}](), reflect.TypeFor[bufferedCallOutput]())
	return &registry.RegisteredComponent{Component: a.Component, Input: a.Input, OutputType: reflect.TypeFor[bufferedCallOutput](), Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
		if !engine.IsBufferedComponent(ctx) {
			return nil, errors.New("real composing envelope lacks canonical buffered child policy")
		}
		caller, err := activity49Invoker(ctx, inv.Binder)
		if err != nil {
			return nil, err
		}
		return caller.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: native.Component.Key, Route: spec.RouteRef{Method: native.Component.Routes[0].Method, Path: native.Component.Routes[0].Path}}})
	})}
}
