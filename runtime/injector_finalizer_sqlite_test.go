package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	dexec "github.com/viant/datly/exec"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

type injectorParentInput struct{}
type injectorRow struct {
	ID   int
	Name string
}
type injectorChildInput struct {
	ID            int    `parameter:"ID,kind=caller_output,in=ID,required"`
	PathID        int    `parameter:"PathID,kind=path,in=id,required"`
	Query         string `parameter:"Query,kind=query,in=q,required"`
	Header        string `parameter:"Header,kind=header,in=X-Trace,required"`
	Authorization string `parameter:"Authorization,kind=header,in=Authorization,required"`
	Undeclared    string
}
type injectorReadInput struct {
	ID int `parameter:"ID,kind=caller_output,in=ID,required"`
}
type injectorReadOutput struct{ Data []*injectorRow }
type injectorChildOutput struct{ Name string }
type injectorParentOutput struct {
	ID      int
	Name    string         `bind:"kind=param,in=Name"`
	Data    []*injectorRow `bind:"kind=param,in=Data"`
	Enabled bool
	hook    func(context.Context, xhandler.InjectorLookup) error
	calls   int
	mcp     int
}

func (o *injectorParentOutput) Finalize(ctx context.Context, lookup xhandler.InjectorLookup) error {
	o.calls++
	if !o.Enabled {
		return nil
	}
	return o.hook(ctx, lookup)
}
func (o *injectorParentOutput) FinalizeMCP(context.Context, xmcp.Context) error { o.mcp++; return nil }

var _ xhandler.InjectorFinalizer = (*injectorParentOutput)(nil)

// Reuse the real artifact compiler, native SQLite reader and buffered Data owner.
type injectorFixture struct {
	h            *testharness.Harness
	rt           *Runtime
	parent       *registry.RegisteredComponent
	child        *registry.RegisteredComponent
	output       *injectorParentOutput
	childCalls   atomic.Int32
	childErr     error
	operationErr error
	flush        bool
	tx           *sql.Tx
}

func (f *injectorFixture) init(t *testing.T, external bool) {
	t.Helper()
	ctx := context.Background()
	f.h = testharness.NewSQLiteHarness(t)
	if err := f.h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT)", "INSERT INTO records VALUES(7,'seven')", "CREATE TABLE audit(id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	if external {
		var err error
		f.tx, err = f.h.DB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.tx.Rollback() })
	}
	f.output = &injectorParentOutput{ID: 7, Enabled: true}
	parent := componentSpec("FinalizerParent", "POST", "/injector/parent", nil)
	pa := componentArtifact(t, parent, reflect.TypeOf(injectorParentInput{}), reflect.TypeOf(injectorParentOutput{}))
	f.parent = &registry.RegisteredComponent{Component: pa.Component, Input: pa.Input, OutputType: reflect.TypeOf(injectorParentOutput{}), DataSource: dml.Source{DB: f.h.DB, Tx: f.tx}, Handler: rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
		value, found, err := inv.Binder.Lookup(ctx, xhandler.DMLKey)
		if err != nil || !found {
			return nil, fmt.Errorf("parent DML: %v", err)
		}
		if err = value.(xhandler.DML).Execute("INSERT INTO audit VALUES(1,'parent')"); err != nil {
			return nil, err
		}
		return f.output, f.operationErr
	})}
	child := componentSpec("FinalizerChild", "POST", "/injector/child/{id}", nil)
	ca := componentArtifact(t, child, reflect.TypeOf(injectorChildInput{}), reflect.TypeOf(injectorChildOutput{}))
	f.child = &registry.RegisteredComponent{Component: ca.Component, Input: ca.Input, OutputType: reflect.TypeOf(injectorChildOutput{}), DataSource: dml.Source{DB: f.h.DB}, Handler: rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
		f.childCalls.Add(1)
		input := inv.Input.(*injectorChildInput)
		if input.ID != 7 || input.PathID != 19 || input.Query != "child" || input.Header != "current-trace" || input.Authorization != "Bearer current-token" || input.Undeclared != "" {
			return nil, fmt.Errorf("child binding: %+v", input)
		}
		value, found, err := inv.Binder.Lookup(ctx, xhandler.DataKey)
		if err != nil || !found {
			return nil, fmt.Errorf("child Data: %v", err)
		}
		data := value.(xhandler.Data)
		if err = data.Execute("INSERT INTO audit VALUES(2,?)", input.Header); err != nil {
			return nil, err
		}
		if f.flush {
			if err = data.Flush(ctx, ""); err != nil {
				return nil, err
			}
		}
		return &injectorChildOutput{Name: "matched"}, f.childErr
	})}
	reader := componentSpec("FinalizerRead", "GET", "/injector/read", []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}})
	reader.RootView = &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: "SELECT id, name FROM records WHERE id = :ID"}}
	ra, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: reader, InputType: reflect.TypeOf(injectorReadInput{}), OutputType: reflect.TypeOf(injectorReadOutput{}), DirectViewField: "Data"})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := ra.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: f.h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	f.rt, err = NewRuntime([]*registry.RegisteredComponent{f.parent, f.child, {Component: ra.Component, Input: ra.Input, OutputType: reflect.TypeOf(injectorReadOutput{}), Reader: execution}})
	if err != nil {
		t.Fatal(err)
	}
	f.output.hook = func(ctx context.Context, lookup xhandler.InjectorLookup) error {
		binder, err := lookup(ctx, xhandler.Route{Method: "GET", URL: "/injector/read"})
		if err != nil {
			return err
		}
		if err = binder.Bind(ctx, f.output); err != nil {
			return err
		}
		if len(f.output.Data) != 1 || f.output.Data[0].ID != 7 || f.output.Data[0].Name != "seven" {
			return fmt.Errorf("reader result: %+v", f.output.Data)
		}
		binder, err = lookup(ctx, xhandler.Route{Method: "POST", URL: "/injector/child/19?q=child", Scope: child.Key.Scope, Name: child.Key.Name})
		if err != nil {
			return err
		}
		if err = binder.Bind(ctx, f.output); err != nil {
			return err
		}
		_, found, err := binder.Lookup(ctx, xhandler.ResultKey)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("missing typed result")
		}
		return nil
	}
}
func (f *injectorFixture) execute(t *testing.T) (any, error) {
	t.Helper()
	req := httptest.NewRequest("POST", "/injector/parent?q=parent", nil)
	req.Header = http.Header{"X-Trace": []string{"current-trace"}, "Authorization": []string{"Bearer current-token"}}
	scope, err := requestprovider.New(req)
	if err != nil {
		t.Fatal(err)
	}
	return f.rt.ExecuteRoute(xmcp.WithContext(context.Background(), runtimeMCPContext{}), "POST", "/injector/parent", scope)
}
func (f *injectorFixture) count(t *testing.T) int {
	t.Helper()
	var n int
	if err := f.h.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestInjectorFinalizerConditionalSQLite(t *testing.T) {
	for _, test := range []struct {
		name                             string
		enabled, external, commit, flush bool
		failure                          string
		want                             int
	}{
		{name: "owned", enabled: true, want: 2},
		{name: "false branch", want: 1},
		{name: "imperative flush", enabled: true, flush: true, want: 2},
		{name: "caller commit", enabled: true, external: true, commit: true, want: 2},
		{name: "caller rollback", enabled: true, external: true},
		{name: "caller finalizer failure", enabled: true, external: true, failure: "finalizer"},
		{name: "caller flushed finalizer failure", enabled: true, external: true, flush: true, failure: "finalizer"},
		{name: "finalizer failure", enabled: true, flush: true, failure: "finalizer"},
		{name: "child failure", enabled: true, flush: true, failure: "child"},
		{name: "both errors preserved", enabled: true, failure: "both"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := &injectorFixture{}
			f.init(t, test.external)
			f.output.Enabled = test.enabled
			f.flush = test.flush
			childErr := errors.New("child failure")
			finalErr := errors.New("finalizer failure")
			operationErr := errors.New("operation failure")
			if test.failure == "child" {
				f.childErr = childErr
			}
			if test.failure == "finalizer" || test.failure == "both" {
				hook := f.output.hook
				f.output.hook = func(ctx context.Context, l xhandler.InjectorLookup) error {
					if err := hook(ctx, l); err != nil {
						return err
					}
					return finalErr
				}
			}
			if test.failure == "both" {
				f.operationErr = operationErr
			}
			result, err := f.execute(t)
			if test.failure == "" && err != nil {
				t.Fatal(err)
			}
			if test.failure == "child" && !errors.Is(err, childErr) {
				t.Fatalf("lost child: %v", err)
			}
			if (test.failure == "finalizer" || test.failure == "both") && !errors.Is(err, finalErr) {
				t.Fatalf("lost finalizer: %v", err)
			}
			if test.failure == "both" && !errors.Is(err, operationErr) {
				t.Fatalf("lost operation: %v", err)
			}
			if result != f.output || f.output.calls != 1 {
				t.Fatalf("result=%T calls=%d", result, f.output.calls)
			}
			if test.failure == "" && f.output.mcp != 1 || test.failure != "" && f.output.mcp != 0 {
				t.Fatalf("MCP calls=%d", f.output.mcp)
			}
			wantCalls := int32(0)
			if test.enabled {
				wantCalls = 1
				if f.output.Name != "matched" && test.failure != "child" {
					t.Fatalf("name=%s", f.output.Name)
				}
			}
			if !test.enabled && len(f.output.Data) != 0 {
				t.Fatal("false branch read executed")
			}
			if f.childCalls.Load() != wantCalls {
				t.Fatalf("calls=%d", f.childCalls.Load())
			}
			if f.tx != nil {
				var n int
				pending := 2
				if test.failure != "" && !test.flush {
					pending = 0
				}
				if err = f.tx.QueryRow("SELECT COUNT(*) FROM audit").Scan(&n); err != nil || n != pending {
					t.Fatalf("pending=%d err=%v", n, err)
				}
				if test.commit {
					err = f.tx.Commit()
				} else {
					err = f.tx.Rollback()
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if got := f.count(t); got != test.want {
				t.Fatalf("rows=%d want=%d", got, test.want)
			}
		})
	}
}

func TestInjectorFinalizerBoundariesSQLite(t *testing.T) {
	for _, mode := range []string{"missing", "name", "hidden", "recursive", "canceled", "swallowed child", "escaped"} {
		t.Run(mode, func(t *testing.T) {
			f := &injectorFixture{}
			f.init(t, false)
			var escaped xhandler.Binder
			var lookup xhandler.InjectorLookup
			f.output.hook = func(ctx context.Context, l xhandler.InjectorLookup) error {
				lookup = l
				target := xhandler.Route{Method: "POST", URL: "/injector/child/19?q=child"}
				switch mode {
				case "missing":
					target.URL = "/missing"
				case "name":
					target.Name = "Other"
				case "recursive":
					target.URL = "/injector/parent"
				case "canceled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				case "swallowed child":
					f.childErr = errors.New("swallowed child")
				case "hidden":
					f.rt.publicBundle, _ = route.NewBundle([]*spec.Component{f.parent.Component})
				}
				b, err := l(ctx, target)
				if err != nil {
					return err
				}
				escaped = b
				err = b.Bind(ctx, f.output)
				if mode == "swallowed child" {
					return nil
				}
				return err
			}
			_, err := f.execute(t)
			if mode == "escaped" {
				if err != nil {
					t.Fatal(err)
				}
				if err = escaped.Bind(context.Background(), f.output); err == nil || !strings.Contains(err.Error(), "closed") {
					t.Fatalf("escaped bind: %v", err)
				}
				if _, err = lookup(context.Background(), xhandler.Route{}); err == nil {
					t.Fatal("escaped lookup succeeded")
				}
				return
			}
			if err == nil {
				t.Fatal("expected failure")
			}
			if mode == "recursive" && !strings.Contains(err.Error(), "cycle") {
				t.Fatal(err)
			}
			if mode == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if f.count(t) != 0 {
				t.Fatal("failed finalization committed")
			}
		})
	}
}

func TestInjectorFinalizerConcurrentIsolationSQLite(t *testing.T) {
	// Each invocation carries its own output and route binder, even on one runtime.
	f := &injectorFixture{}
	f.init(t, false)
	f.parent.DataSource = nil
	type concurrentInput struct {
		ID int `parameter:"ID,kind=query,in=id,required"`
	}
	artifact := componentArtifact(t, f.parent.Component, reflect.TypeFor[concurrentInput](), reflect.TypeFor[injectorParentOutput]())
	f.parent.Input = artifact.Input
	for id := 20; id < 32; id++ {
		if _, err := f.h.DB.Exec("INSERT INTO records VALUES(?,?)", id, fmt.Sprint(id)); err != nil {
			t.Fatal(err)
		}
	}
	f.parent.Handler = rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
		o := &injectorParentOutput{ID: inv.Input.(*concurrentInput).ID, Enabled: true}
		o.hook = func(ctx context.Context, l xhandler.InjectorLookup) error {
			b, err := l(ctx, xhandler.Route{Method: "GET", URL: "/injector/read"})
			if err != nil {
				return err
			}
			return b.Bind(ctx, o)
		}
		return o, nil
	})
	// Runtime registrations are snapshots; construct a fresh runtime after fixture configuration.
	var err error
	f.rt, err = NewRuntime([]*registry.RegisteredComponent{f.parent, f.child, f.rt.registered[(spec.Key{Kind: spec.KindComponent, Scope: "example.com/components", Name: "FinalizerRead"}).String()]})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	failures := make(chan error, 12)
	for i := 0; i < 12; i++ {
		id := 20 + i
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := f.rt.ExecuteRoute(context.Background(), "POST", "/injector/parent", requestprovider.NewValues(requestprovider.WithQuery(url.Values{"id": {fmt.Sprint(id)}})))
			if err == nil {
				out := result.(*injectorParentOutput)
				if out.calls != 1 || len(out.Data) != 1 || out.Data[0].ID != id {
					err = fmt.Errorf("isolated output: %+v", out)
				}
			}
			failures <- err
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Error(err)
		}
	}
}

type injectorSuccessOutput struct {
	Name string
	hook func() error
}

func (o *injectorSuccessOutput) Finalize(context.Context) error { return o.hook() }

func (f *injectorFixture) rebuild(t *testing.T) {
	t.Helper()
	read := f.rt.registered[(spec.Key{Kind: spec.KindComponent, Scope: "example.com/components", Name: "FinalizerRead"}).String()]
	var err error
	f.rt, err = NewRuntime([]*registry.RegisteredComponent{f.parent, f.child, read})
	if err != nil {
		t.Fatal(err)
	}
}
func TestInjectorFinalizerChildSuccessWaitsForRootSQLite(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			f := &injectorFixture{}
			f.init(t, false)
			committed, success := false, 0
			f.parent.DataSource = dml.Source{DB: f.h.DB, OnCommit: func(context.Context) { committed = true }}
			child := f.child.Handler
			f.child.Handler = rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
				result, err := child.Execute(ctx, inv)
				if err != nil {
					return result, err
				}
				return &injectorSuccessOutput{Name: "matched", hook: func() error {
					success++
					if !committed {
						return errors.New("child success before root commit")
					}
					return nil
				}}, nil
			})
			hook := f.output.hook
			f.output.hook = func(ctx context.Context, l xhandler.InjectorLookup) error {
				if err := hook(ctx, l); err != nil {
					return err
				}
				if success != 0 {
					return errors.New("child finalized early")
				}
				if fail {
					return errors.New("parent veto")
				}
				return nil
			}
			f.rebuild(t)
			_, err := f.execute(t)
			if !fail && err != nil {
				t.Fatal(err)
			}
			if fail && err == nil {
				t.Fatal("expected veto")
			}
			want := 0
			if !fail {
				want = 1
			}
			if success != want || committed == fail {
				t.Fatalf("success=%d committed=%v", success, committed)
			}
		})
	}
}
func TestInjectorFinalizerPreparedFailureAndPanicSQLite(t *testing.T) {
	for _, mode := range []string{"prepare", "panic", "nil bind", "URL query authority"} {
		t.Run(mode, func(t *testing.T) {
			f := &injectorFixture{}
			f.init(t, false)
			if mode == "prepare" {
				f.parent.Handler = rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
					v, _, err := inv.Binder.Lookup(ctx, xhandler.DMLKey)
					if err != nil {
						return nil, err
					}
					err = v.(xhandler.DML).Execute("INSERT INTO absent_table VALUES(1)")
					return f.output, err
				})
				f.rebuild(t)
			}
			f.output.hook = func(ctx context.Context, l xhandler.InjectorLookup) error {
				if mode == "prepare" {
					return nil
				}
				if mode == "panic" {
					panic("finalizer panic")
				}
				target := xhandler.Route{Method: "POST", URL: "/injector/child/19?q=child"}
				if mode == "URL query authority" {
					target.URL = "/injector/child/19"
				}
				b, err := l(ctx, target)
				if err != nil {
					return err
				}
				if mode == "nil bind" {
					return b.Bind(ctx, nil)
				}
				return b.Bind(ctx, f.output)
			}
			result, err := f.execute(t)
			if err == nil {
				t.Fatal("expected failure")
			}
			if mode == "panic" {
				var recovered *dexec.PanicError
				if !errors.As(err, &recovered) || recovered.Cause() != "finalizer panic" || len(recovered.Stack()) == 0 {
					t.Fatalf("lost panic diagnostics: %v", err)
				}
			}
			if f.count(t) != 0 {
				t.Fatal("committed on failure")
			}
			if mode == "prepare" && result != nil {
				t.Fatalf("prepared failure retained result %T", result)
			}
			if f.output.calls != 1 {
				t.Fatalf("finalized %d times", f.output.calls)
			}
		})
	}
}

func TestInjectorFinalizerChildCancellationRollsBackSQLite(t *testing.T) {
	f := &injectorFixture{}
	f.init(t, false)
	operation, cancel := context.WithCancel(context.Background())
	defer cancel()
	child := f.child.Handler
	f.child.Handler = rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
		result, err := child.Execute(ctx, inv)
		cancel()
		return result, err
	})
	f.rebuild(t)
	f.output.hook = func(ctx context.Context, l xhandler.InjectorLookup) error {
		b, err := l(ctx, xhandler.Route{Method: "POST", URL: "/injector/child/19?q=child"})
		if err != nil {
			return err
		}
		return b.Bind(operation, f.output)
	}
	_, err := f.execute(t)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if f.count(t) != 0 {
		t.Fatal("canceled child committed")
	}
}

type injectorStatusError struct{}

func (*injectorStatusError) Error() string   { return "authorized safe message" }
func (*injectorStatusError) StatusCode() int { return 409 }
func TestInjectorFinalizerPreservesOriginalStatusSQLite(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(fmt.Sprint(panics), func(t *testing.T) {
			f := &injectorFixture{}
			f.init(t, false)
			cause := &injectorStatusError{}
			f.operationErr = cause
			f.output.hook = func(context.Context, xhandler.InjectorLookup) error {
				if panics {
					panic("failure")
				}
				return nil
			}
			_, err := f.execute(t)
			if !errors.Is(err, cause) {
				t.Fatalf("lost status cause: %v", err)
			}
			if !panics && err != cause {
				t.Fatalf("status-bearing error was wrapped: %T", err)
			}
			if panics && !strings.Contains(err.Error(), "panicked") {
				t.Fatalf("lost panic error: %v", err)
			}
			if f.count(t) != 0 || f.output.calls != 1 {
				t.Fatal("failed status lifecycle")
			}
		})
	}
}
