package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/viant/datly/sql/dml"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	runtimeauth "github.com/viant/datly/runtime/auth"
	rh "github.com/viant/datly/runtime/handler"
	hp "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	xh "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/response"
	"net/http/httptest"
)

type optionalReaderProbe struct {
	mu         sync.Mutex
	run        func(context.Context, xh.ComponentInvoker) error
	failure    error
	calls      int
	initialize bool
}

func (*optionalReaderProbe) Debug(string, ...any) {}
func (*optionalReaderProbe) Info(string, ...any)  {}
func (*optionalReaderProbe) Warn(string, ...any)  {}
func (*optionalReaderProbe) Error(string, ...any) {}

type optionalReaderInput struct {
	Logger xh.Logger `parameter:"Logger,kind=logger,required" json:"-"`
}

func (i *optionalReaderInput) Init(ctx context.Context, invoker xh.ComponentInvoker) error {
	p := i.Logger.(*optionalReaderProbe)
	if p.initialize {
		p.calls++
		return p.run(ctx, invoker)
	}
	return nil
}

type optionalReaderOutput struct {
	Data   []*bufferedCallRow `parameter:"Data,kind=output,in=view" json:"data"`
	Logger xh.Logger          `parameter:"Logger,kind=logger,required" json:"-"`
}

func (o *optionalReaderOutput) Finalize(ctx context.Context, invoker xh.ComponentInvoker) error {
	p := o.Logger.(*optionalReaderProbe)
	if p.initialize {
		return nil
	}
	p.calls++
	return p.run(ctx, invoker)
}

type optionalReaderErrorOutput optionalReaderOutput

func (o *optionalReaderErrorOutput) Finalize(ctx context.Context, invoker xh.ComponentInvoker, cause error) error {
	if cause != nil {
		return cause
	}
	p := o.Logger.(*optionalReaderProbe)
	p.calls++
	return p.run(ctx, invoker)
}

type optionalReaderFailureOutput optionalReaderOutput

func (o *optionalReaderFailureOutput) Finalize(ctx context.Context, _ error) error {
	p := o.Logger.(*optionalReaderProbe)
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	if p.run != nil {
		return p.run(ctx, nil)
	}
	return p.failure
}

type optionalReaderRequiredInput struct {
	Token string `parameter:"Token,kind=header,in=X-Token,required,errorCode=403"`
}
type optionalReaderInitFailureInput optionalReaderInput

func (i *optionalReaderInitFailureInput) Init(context.Context) error {
	return i.Logger.(*optionalReaderProbe).failure
}

func TestOptionalCanonicalReaderLifecycleSQLite(t *testing.T) {
	for _, phase := range []string{"init", "finalize", "error finalize"} {
		for _, mode := range []string{"caught", "returned", "wrapped", "mixed", "prior writer", "later writer", "later writer failure", "custom GET", "required binding", "input init", "output binding", "logger lookup", "provider collision", "JWT", "parent projection", "canceled", "deadline", "wrapped cancellation", "reader writer", "during cancellation", "panic"} {
			t.Run(phase+"/"+mode, func(t *testing.T) {
				ctx := t.Context()
				db := sqlite.New(t)
				if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)", "INSERT INTO records VALUES(99,'native read')", "CREATE TABLE sqlx_sequence_reservations(table_name TEXT PRIMARY KEY,value INTEGER NOT NULL CHECK(value >= 0))"); err != nil {
					t.Fatal(err)
				}
				cause := errors.New("missing taxonomy")
				fatal := errors.New("fatal writer")
				projection := &response.Error{Code: 403, Payload: map[string]any{"unavailable": true}, Cause: cause}
				failedInput := reflect.TypeFor[struct{}]()
				failedOutput := reflect.TypeFor[optionalReaderFailureOutput]()
				if mode == "required binding" {
					failedInput = reflect.TypeFor[optionalReaderRequiredInput]()
				}
				if mode == "reader writer" {
					failedInput = reflect.TypeFor[readerFinalizerInput]()
				}
				if mode == "input init" {
					failedInput = reflect.TypeFor[optionalReaderInitFailureInput]()
				}
				failed := readerFinalizerNative(t, db.DB, "OptionalFailure", "/optional-failure", failedInput, failedOutput)
				if mode == "JWT" {
					failed = optionalJWTReader(t, db.DB)
				}
				failedProbe := &optionalReaderProbe{failure: projection}
				failed.Capabilities.Logger = failedProbe
				if mode == "wrapped cancellation" {
					failedProbe.failure = &response.Error{Code: 403, Cause: context.Canceled}
				}
				if mode == "mixed" {
					failedProbe.failure = errors.Join(projection, fatal)
				}
				if mode == "provider collision" {
					failed.Providers = []locator.Provider{hp.Static(xh.ComponentInvokerKey, nil)}
				}
				if mode == "logger lookup" {
					failed.Capabilities.Logger = nil
					failed.Providers = []locator.Provider{hp.Named("logger", func(context.Context, reflect.Type, string) (any, bool, error) { return nil, false, projection })}
				}
				if mode == "output binding" {
					failed.Capabilities.Logger = nil
				}
				if mode == "custom GET" {
					failed.Handler = rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) { return nil, projection })
				}
				if mode == "panic" {
					failed.Handler = rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) { panic(projection) })
				}
				next := readerFinalizerNative(t, db.DB, "OptionalNext", "/optional-next", reflect.TypeFor[struct{}](), reflect.TypeFor[bufferedCallOutput]())
				commits := 0
				written := readerFinalizerWriter(t, db.DB, func(context.Context) { commits++ })
				if mode == "later writer failure" {
					written.Handler = &bufferedInjectorFaultNative{Handler: written.Handler.(*writer.Handler), cause: fatal}
				}
				outputType := reflect.TypeFor[optionalReaderOutput]()
				if phase == "error finalize" {
					outputType = reflect.TypeFor[optionalReaderErrorOutput]()
				}
				root := readerFinalizerNative(t, db.DB, "OptionalRoot", "/optional-root", reflect.TypeFor[optionalReaderInput](), outputType)
				var caught error
				var retained xh.ComponentInvoker
				nextCalls := 0
				rootProbe := &optionalReaderProbe{initialize: phase == "init"}
				rootProbe.run = func(hookCtx context.Context, invoker xh.ComponentInvoker) error {
					retained = invoker
					write := func() error {
						_, err := invoker.Invoke(context.Background(), readerFinalizerPublicRequest(written.Component, &bufferedCallInput{Rows: []*bufferedCallRow{{ID: 2, Label: "later write"}}}))
						return err
					}
					if mode == "prior writer" {
						if err := write(); err != nil {
							return err
						}
					}
					call := context.Background()
					if mode == "deadline" {
						call, _ = context.WithDeadline(call, time.Now().Add(-time.Second))
					}
					if mode == "reader writer" {
						_ = hookCtx
						failedProbe.run = func(childCtx context.Context, _ xh.ComponentInvoker) error {
							native := childCtx.Value(reflect.TypeFor[*readerFinalizerInput]()).(*readerFinalizerInput).Invoker
							_, err := native.Invoke(childCtx, readerFinalizerPublicRequest(written.Component, &bufferedCallInput{Rows: []*bufferedCallRow{{ID: 2, Label: "inside reader"}}}))
							if err != nil {
								return err
							}
							return projection
						}
					}
					if mode == "during cancellation" {
						var cancel context.CancelFunc
						call, cancel = context.WithCancel(call)
						failedProbe.run = func(context.Context, xh.ComponentInvoker) error { cancel(); return projection }
					}
					if mode == "canceled" {
						var cancel context.CancelFunc
						call, cancel = context.WithCancel(call)
						cancel()
					}
					req := readerFinalizerPublicRequest(failed.Component, nil)
					if mode == "JWT" {
						httpReq := httptest.NewRequest("GET", "/optional-jwt", nil)
						httpReq.Header.Set("Authorization", "Bearer invalid")
						scope, err := requestprovider.New(httpReq)
						if err != nil {
							return err
						}
						req.Providers = scope.Providers()
					}
					result, err := invoker.Invoke(call, req)
					caught = err
					if err == nil {
						return errors.New("optional reader must return its error")
					}
					if mode == "caught" || mode == "returned" || mode == "wrapped" || mode == "later writer" || mode == "later writer failure" {
						var body *response.Error
						if !errors.Is(err, cause) || !errors.As(err, &body) || body != projection || response.ErrorStatusCode(err, 500) != 403 || !reflect.DeepEqual(body.Payload, projection.Payload) {
							return fmt.Errorf("reader error contract lost: %w", err)
						}
						out, ok := result.(*optionalReaderFailureOutput)
						if !ok || len(out.Data) != 1 || out.Data[0].ID != 99 {
							return fmt.Errorf("SQL reader typed output lost: %T %+v", result, result)
						}
					}
					if mode == "parent projection" {
						return &response.Error{Code: 409, Payload: map[string]any{"parent": true}, Cause: err}
					}
					if mode == "returned" {
						return err
					}
					if mode == "wrapped" {
						return fmt.Errorf("callback returned: %w", err)
					}
					if mode == "caught" || mode == "later writer" || mode == "later writer failure" {
						nextCalls++
						v, err := invoker.Invoke(context.Background(), readerFinalizerPublicRequest(next.Component, nil))
						if err != nil {
							return err
						}
						if len(v.(*bufferedCallOutput).Data) != 1 {
							return errors.New("next read lost")
						}
						if strings.HasPrefix(mode, "later writer") {
							_ = write()
							var count int
							if err = db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records WHERE id=2").Scan(&count); err != nil || count != 0 {
								return fmt.Errorf("writer committed before root: %d %v", count, err)
							}
						}
					}
					return nil
				}
				root.Capabilities.Logger = rootProbe
				rt, err := NewRuntime([]*registry.RegisteredComponent{root, failed, next, written})
				if err != nil {
					t.Fatal(err)
				}
				defer rt.Shutdown(ctx)
				_, err = rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: readerFinalizerTarget(root.Component)})
				success := mode == "caught" || mode == "later writer"
				if (err == nil) != success {
					t.Fatalf("root error %v, caught %v", err, caught)
				}
				if rootProbe.calls != 1 || retained == nil {
					t.Fatalf("callback count %d", rootProbe.calls)
				}
				if _, escaped := retained.Invoke(ctx, readerFinalizerPublicRequest(next.Component, nil)); escaped == nil || !strings.Contains(escaped.Error(), "scope is closed") {
					t.Fatalf("retained lifecycle lease: %v", escaped)
				}
				if mode == "parent projection" {
					if response.ErrorStatusCode(err, 500) != 409 || !errors.Is(err, cause) {
						t.Fatalf("parent public projection lost %v", err)
					}
					body, _ := response.ErrorBody(err)
					if !reflect.DeepEqual(body, map[string]any{"parent": true}) {
						t.Fatalf("parent body %v", body)
					}
				}
				if success && nextCalls != 1 {
					t.Fatalf("next reads %d", nextCalls)
				}
				want := 0
				if mode == "later writer" {
					want = 1
				}
				if commits != want {
					t.Fatalf("commits %d want %d", commits, want)
				}
				var rows int
				if scanErr := db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records WHERE id=2").Scan(&rows); scanErr != nil || rows != want {
					t.Fatalf("rows %d want %d err %v", rows, want, scanErr)
				}
				if mode == "caught" {
					_, directErr := rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: readerFinalizerTarget(failed.Component)})
					if !errors.Is(directErr, projection) || response.ErrorStatusCode(directErr, 500) != 403 || failedProbe.calls != 2 {
						t.Fatalf("direct reader403 contract: calls %d error %v", failedProbe.calls, directErr)
					}
				}
				if mode == "returned" || mode == "wrapped" {
					if !errors.Is(err, projection) || response.ErrorStatusCode(err, 500) != 403 {
						t.Fatalf("returned error contract %v", err)
					}
				}
			})
		}
	}
}

// Open occurs after the existing canonical databaseUnit publication. The
// wrapper only pauses that real source; it introduces no separate owner.
type optionalBlockedSource struct {
	db      *sql.DB
	entered chan struct{}
	release chan struct{}
	failure error
}

func (s *optionalBlockedSource) InvocationKey() any { return s.db }
func (s *optionalBlockedSource) Open(ctx context.Context) (xh.Data, error) {
	close(s.entered)
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if s.failure != nil {
		return nil, s.failure
	}
	return (dml.Source{DB: s.db}).Open(ctx)
}
func TestOptionalReaderWriterPublicationSQLite(t *testing.T) {
	for _, failOpen := range []bool{false, true} {
		t.Run(fmt.Sprint(failOpen), func(t *testing.T) {
			ctx := t.Context()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)", "INSERT INTO records VALUES(99,'native read')", "CREATE TABLE sqlx_sequence_reservations(table_name TEXT PRIMARY KEY,value INTEGER NOT NULL)"); err != nil {
				t.Fatal(err)
			}
			cause := &response.Error{Code: 403, Cause: errors.New("reader403")}
			readerEntered := make(chan struct{})
			readerRelease := make(chan struct{})
			failed := readerFinalizerNative(t, db.DB, "PublicationReader", "/publication-reader", reflect.TypeFor[struct{}](), reflect.TypeFor[optionalReaderFailureOutput]())
			failed.Capabilities.Logger = &optionalReaderProbe{run: func(context.Context, xh.ComponentInvoker) error {
				close(readerEntered)
				select {
				case <-readerRelease:
				case <-time.After(5 * time.Second):
					return errors.New("reader wait timed out")
				}
				return cause
			}}
			source := &optionalBlockedSource{db: db.DB, entered: make(chan struct{}), release: make(chan struct{})}
			if failOpen {
				source.failure = errors.New("writer open failed")
			}
			written := readerFinalizerWriter(t, db.DB, func(context.Context) { t.Error("published writer committed after ignored reader failure") })
			written.DataSource = source
			a := componentArtifact(t, componentSpec("PublicationRoot", "POST", "/publication-root", nil), reflect.TypeFor[struct{}](), reflect.TypeFor[struct{}]())
			root := &registry.RegisteredComponent{Component: a.Component, Input: a.Input, OutputType: reflect.TypeFor[struct{}](), Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				value, _, err := inv.Binder.Lookup(ctx, xh.ComponentInvokerKey)
				if err != nil {
					return nil, err
				}
				native := value.(xh.ComponentInvoker)
				readerDone := make(chan error, 1)
				writerDone := make(chan error, 1)
				go func() {
					_, err := native.Invoke(ctx, readerFinalizerPublicRequest(failed.Component, nil))
					readerDone <- err
				}()
				select {
				case <-readerEntered:
				case <-time.After(5 * time.Second):
					return nil, errors.New("reader not entered")
				}
				go func() {
					_, err := native.Invoke(ctx, readerFinalizerPublicRequest(written.Component, &bufferedCallInput{Rows: []*bufferedCallRow{{ID: 2, Label: "overlapping writer"}}}))
					writerDone <- err
				}()
				select {
				case <-source.entered:
				case <-time.After(5 * time.Second):
					return nil, errors.New("writer not registered")
				}
				close(readerRelease)
				readerErr := <-readerDone
				close(source.release)
				writerErr := <-writerDone
				if !errors.Is(readerErr, cause) {
					return nil, fmt.Errorf("reader returned %v", readerErr)
				}
				if failOpen && !errors.Is(writerErr, source.failure) {
					return nil, fmt.Errorf("open returned %v", writerErr)
				}
				return &struct{}{}, nil
			})}
			rt, err := NewRuntime([]*registry.RegisteredComponent{root, failed, written})
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Shutdown(ctx)
			_, err = rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: readerFinalizerTarget(root.Component)})
			if !errors.Is(err, cause) {
				t.Fatalf("published writer did not retain reader veto: %v", err)
			}
			var rows int
			if scanErr := db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records WHERE id=2").Scan(&rows); scanErr != nil || rows != 0 {
				t.Fatalf("partial commit rows %d err %v", rows, scanErr)
			}
		})
	}
}

func optionalJWTReader(t *testing.T, db *sql.DB) *registry.RegisteredComponent {
	t.Helper()
	key := testharness.NewJWT(t)
	factory, err := runtimeauth.New(t.Context(), &runtimeauth.Config{JWTValidator: key.Config()})
	if err != nil {
		t.Fatal(err)
	}
	c := componentSpec("OptionalJWT", "GET", "/optional-jwt", []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}})
	c.RootView = &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id,label FROM records WHERE id=99"}}
	a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[injectorJWTInput](), OutputType: reflect.TypeFor[optionalReaderFailureOutput](), CodecFactory: factory})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := sqlreader.NewExecution(sqlreader.Config{Component: a.Component, InputType: reflect.TypeFor[injectorJWTInput](), OutputType: reflect.TypeFor[optionalReaderFailureOutput](), Plan: a.Reader, SQL: &dsql.SQLComponent{DB: db}})
	if err != nil {
		t.Fatal(err)
	}
	return &registry.RegisteredComponent{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[optionalReaderFailureOutput](), Reader: reader}
}

func TestOptionalReaderNestedLifecycleSQLite(t *testing.T) {
	for _, phase := range []string{"init", "finalize", "error finalize"} {
		t.Run(phase, func(t *testing.T) {
			ctx := t.Context()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)", "INSERT INTO records VALUES(99,'native read')"); err != nil {
				t.Fatal(err)
			}
			projection := &response.Error{Code: 403, Cause: errors.New("nested taxonomy miss")}
			failed := readerFinalizerNative(t, db.DB, "NestedOptionalFailure", "/nested-optional-failure", reflect.TypeFor[struct{}](), reflect.TypeFor[optionalReaderFailureOutput]())
			failureProbe := &optionalReaderProbe{failure: projection}
			failed.Capabilities.Logger = failureProbe
			outputType := reflect.TypeFor[optionalReaderOutput]()
			if phase == "error finalize" {
				outputType = reflect.TypeFor[optionalReaderErrorOutput]()
			}
			middle := readerFinalizerNative(t, db.DB, "NestedOptionalMiddle", "/nested-optional-middle", reflect.TypeFor[optionalReaderInput](), outputType)
			middleProbe := &optionalReaderProbe{initialize: phase == "init", run: func(_ context.Context, invoker xh.ComponentInvoker) error {
				_, err := invoker.Invoke(context.Background(), readerFinalizerPublicRequest(failed.Component, nil))
				if !errors.Is(err, projection) {
					return fmt.Errorf("nested child failure: %v", err)
				}
				return nil
			}}
			middle.Capabilities.Logger = middleProbe
			root := readerFinalizerNative(t, db.DB, "NestedOptionalRoot", "/nested-optional-root", reflect.TypeFor[optionalReaderInput](), reflect.TypeFor[optionalReaderOutput]())
			rootProbe := &optionalReaderProbe{run: func(_ context.Context, invoker xh.ComponentInvoker) error {
				value, err := invoker.Invoke(context.Background(), readerFinalizerPublicRequest(middle.Component, nil))
				if err != nil {
					return err
				}
				switch out := value.(type) {
				case *optionalReaderOutput:
					if len(out.Data) != 1 {
						return errors.New("missing nested rows")
					}
				case *optionalReaderErrorOutput:
					if len(out.Data) != 1 {
						return errors.New("missing nested error-aware rows")
					}
				default:
					return fmt.Errorf("nested typed output %T", value)
				}
				return nil
			}}
			root.Capabilities.Logger = rootProbe
			rt, err := NewRuntime([]*registry.RegisteredComponent{root, middle, failed})
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Shutdown(ctx)
			if _, err = rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: readerFinalizerTarget(root.Component)}); err != nil {
				t.Fatal(err)
			}
			if rootProbe.calls != 1 || middleProbe.calls != 1 || failureProbe.calls != 1 {
				t.Fatalf("callbacks root %d middle %d reader %d", rootProbe.calls, middleProbe.calls, failureProbe.calls)
			}
		})
	}
}

func TestOptionalReaderInjectedCallSQLite(t *testing.T) {
	ctx := t.Context()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)", "INSERT INTO records VALUES(99,'native read')"); err != nil {
		t.Fatal(err)
	}
	projection := &response.Error{Code: 403, Cause: errors.New("injected taxonomy miss")}
	failed := readerFinalizerNative(t, db.DB, "InjectedOptionalFailure", "/injected-optional-failure", reflect.TypeFor[struct{}](), reflect.TypeFor[optionalReaderFailureOutput]())
	probe := &optionalReaderProbe{failure: projection}
	failed.Capabilities.Logger = probe
	next := readerFinalizerNative(t, db.DB, "InjectedOptionalNext", "/injected-optional-next", reflect.TypeFor[struct{}](), reflect.TypeFor[bufferedCallOutput]())
	a := componentArtifact(t, componentSpec("InjectedOptionalRoot", "POST", "/injected-optional-root", nil), reflect.TypeFor[readerFinalizerInput](), reflect.TypeFor[bufferedCallOutput]())
	root := &registry.RegisteredComponent{Component: a.Component, Input: a.Input, OutputType: reflect.TypeFor[bufferedCallOutput](), Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
		invoker := inv.Input.(*readerFinalizerInput).Invoker
		value, err := invoker.Invoke(ctx, readerFinalizerPublicRequest(failed.Component, nil))
		if !errors.Is(err, projection) || len(value.(*optionalReaderFailureOutput).Data) != 1 {
			return nil, fmt.Errorf("reader contract: %v", err)
		}
		return invoker.Invoke(ctx, readerFinalizerPublicRequest(next.Component, nil))
	})}
	rt, err := NewRuntime([]*registry.RegisteredComponent{root, failed, next})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(ctx)
	result, err := rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: readerFinalizerTarget(root.Component)})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.(*bufferedCallOutput).Data) != 1 || probe.calls != 1 {
		t.Fatalf("reader result %+v calls %d", result, probe.calls)
	}
}

func TestOptionalReaderConcurrentRequestIsolationSQLite(t *testing.T) {
	ctx := t.Context()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)", "INSERT INTO records VALUES(99,'native read')", "CREATE TABLE sqlx_sequence_reservations(table_name TEXT PRIMARY KEY,value INTEGER NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	projection := &response.Error{Code: 403, Cause: errors.New("shared optional miss")}
	failed := readerFinalizerNative(t, db.DB, "ConcurrentOptionalFailure", "/concurrent-optional-failure", reflect.TypeFor[struct{}](), reflect.TypeFor[optionalReaderFailureOutput]())
	failedProbe := &optionalReaderProbe{failure: projection}
	failed.Capabilities.Logger = failedProbe
	written := readerFinalizerWriter(t, db.DB, func(context.Context) { t.Error("failed independent request committed") })
	roots := make([]*registry.RegisteredComponent, 2)
	probes := make([]*optionalReaderProbe, 2)
	for index := range roots {
		roots[index] = readerFinalizerNative(t, db.DB, fmt.Sprintf("ConcurrentRoot%d", index), fmt.Sprintf("/concurrent-root-%d", index), reflect.TypeFor[optionalReaderInput](), reflect.TypeFor[optionalReaderOutput]())
		probes[index] = &optionalReaderProbe{run: func(ctx context.Context, invoker xh.ComponentInvoker) error {
			if index == 1 {
				if _, err := invoker.Invoke(ctx, readerFinalizerPublicRequest(written.Component, &bufferedCallInput{Rows: []*bufferedCallRow{{ID: 2, Label: "independent request"}}})); err != nil {
					return err
				}
			}
			_, err := invoker.Invoke(ctx, readerFinalizerPublicRequest(failed.Component, nil))
			if !errors.Is(err, projection) {
				return fmt.Errorf("shared reader error lost: %v", err)
			}
			return nil
		}}
		roots[index].Capabilities.Logger = probes[index]
	}
	rt, err := NewRuntime([]*registry.RegisteredComponent{roots[0], roots[1], failed, written})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(ctx)
	start := make(chan struct{})
	results := []chan error{make(chan error, 1), make(chan error, 1)}
	for index := range roots {
		go func() {
			<-start
			_, err := rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: readerFinalizerTarget(roots[index].Component)})
			results[index] <- err
		}()
	}
	close(start)
	first, second := <-results[0], <-results[1]
	if first != nil || !errors.Is(second, projection) {
		t.Fatalf("request isolation: neutral %v writer %v", first, second)
	}
	if probes[0].calls != 1 || probes[1].calls != 1 || failedProbe.calls != 2 {
		t.Fatalf("request callbacks: %d %d reader %d", probes[0].calls, probes[1].calls, failedProbe.calls)
	}
	var rows int
	if err = db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records WHERE id=2").Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("partial write rows %d error %v", rows, err)
	}
}

type componentCompositeError struct{ Fact any }

func (e componentCompositeError) Error() string { return "composite reader failure" }
func TestCompositeReaderErrorRemainsStickySQLite(t *testing.T) {
	for _, fact := range []any{[]string{"private fact"}, map[string]int{"private fact": 1}, [1]any{[]string{"nested fact"}}} {
		t.Run(reflect.TypeOf(fact).String(), func(t *testing.T) {
			ctx := t.Context()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)", "INSERT INTO records VALUES(99,'native read')"); err != nil {
				t.Fatal(err)
			}
			cause := componentCompositeError{Fact: fact}
			projection := &response.Error{Code: 403, Payload: map[string]any{"unavailable": true}, Cause: cause}
			failed := readerFinalizerNative(t, db.DB, "CompositeFailure", "/composite-failure", reflect.TypeFor[struct{}](), reflect.TypeFor[optionalReaderFailureOutput]())
			failed.Capabilities.Logger = &optionalReaderProbe{failure: projection}
			root := readerFinalizerNative(t, db.DB, "CompositeRoot", "/composite-root", reflect.TypeFor[optionalReaderInput](), reflect.TypeFor[optionalReaderOutput]())
			calls := 0
			root.Capabilities.Logger = &optionalReaderProbe{run: func(ctx context.Context, invoker xh.ComponentInvoker) error {
				calls++
				result, err := invoker.Invoke(ctx, readerFinalizerPublicRequest(failed.Component, nil))
				if err == nil || !errors.Is(err, projection) || response.ErrorStatusCode(err, 500) != 403 {
					return fmt.Errorf("composite projection changed: %v", err)
				}
				out, ok := result.(*optionalReaderFailureOutput)
				if !ok || len(out.Data) != 1 {
					return fmt.Errorf("composite typed reader output lost: %T", result)
				}
				var captured componentCompositeError
				if !errors.As(err, &captured) || !reflect.DeepEqual(captured.Fact, fact) {
					return fmt.Errorf("composite underlying cause changed: %v", err)
				}
				return nil
			}}
			rt, err := NewRuntime([]*registry.RegisteredComponent{root, failed})
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Shutdown(ctx)
			_, err = rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: readerFinalizerTarget(root.Component)})
			var panicErr *dexec.PanicError
			var captured componentCompositeError
			if err == nil || errors.As(err, &panicErr) || !errors.Is(err, projection) || response.ErrorStatusCode(err, 500) != 403 || !errors.As(err, &captured) || !reflect.DeepEqual(captured.Fact, fact) {
				t.Fatalf("uncomparable cause did not remain sticky: %v", err)
			}
			if calls != 1 {
				t.Fatalf("callback calls %d", calls)
			}
		})
	}
}
