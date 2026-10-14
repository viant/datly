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
	"time"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	runtimeauth "github.com/viant/datly/runtime/auth"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
	sqlreader "github.com/viant/datly/sql/reader"
	"github.com/viant/sqlx/testutil/sqlfault"
	xh "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/response"
)

// Logger is a per-invocation injected test capability, so native SQL output
// allocation exercises the same binding path as the public component invoker.
type readerFinalizerProbe struct {
	calls int
	cause error
	run   func(context.Context, *readerFinalizerOutput, error) error
}

func (*readerFinalizerProbe) Debug(string, ...any) {}
func (*readerFinalizerProbe) Info(string, ...any)  {}
func (*readerFinalizerProbe) Warn(string, ...any)  {}
func (*readerFinalizerProbe) Error(string, ...any) {}

type readerFinalizerOutput struct {
	Data    []*bufferedCallRow  `parameter:"Data,kind=output,in=view" json:"data"`
	Invoker xh.ComponentInvoker `json:"-"`
	Logger  xh.Logger           `parameter:"Logger,kind=logger,required" json:"-"`
}

func (o *readerFinalizerOutput) Finalize(ctx context.Context, cause error) error {
	// The engine exposes its bound input on the typed context key. Reader
	// finalizers use the injected input capability after native output allocation.
	if input, ok := ctx.Value(reflect.TypeFor[*readerFinalizerInput]()).(*readerFinalizerInput); ok {
		o.Invoker = input.Invoker
	}
	if input, ok := ctx.Value(reflect.TypeFor[*readerFinalizerPriorInput]()).(*readerFinalizerPriorInput); ok {
		o.Invoker = input.Invoker
	}
	p, ok := o.Logger.(*readerFinalizerProbe)
	if !ok || cause == nil && o.Invoker == nil {
		return fmt.Errorf("reader finalizer capabilities missing: logger %T invoker %T", o.Logger, o.Invoker)
	}
	p.calls++
	p.cause = cause
	return p.run(ctx, o, cause)
}

type readerFinalizerInput struct {
	Invoker xh.ComponentInvoker `parameter:"Invoker,kind=component_invoker,required" json:"-"`
}

type readerFinalizerPriorInput struct {
	Invoker xh.ComponentInvoker `parameter:"Invoker,kind=component_invoker,required" json:"-"`
	Prior   *bufferedCallOutput `parameter:"Prior,kind=component,in=POST:/reader-finalizer-prior,required"`
}

type readerFinalizerBoundFailureInput struct {
	Failed *bufferedCallOutput `parameter:"Failed,kind=component,in=GET:/caught-reader-failure,required"`
}

func readerFinalizerTarget(c *spec.Component) dexec.ComponentTarget {
	return dexec.ComponentTarget{Component: c.Key, Route: spec.RouteRef{Method: c.Routes[0].Method, Path: c.Routes[0].Path}}
}

func readerFinalizerPublicRequest(c *spec.Component, input any) xh.ComponentRequest {
	return xh.ComponentRequest{Target: xh.ComponentTarget{Scope: c.Key.Scope, Name: c.Key.Name, Method: c.Routes[0].Method, Path: c.Routes[0].Path}, Input: input}
}

func readerFinalizerNative(t *testing.T, db *sql.DB, name, path string, inputType, outputType reflect.Type) *registry.RegisteredComponent {
	t.Helper()
	c := componentSpec(name, "GET", path, []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}})
	c.RootView = &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id,label FROM records WHERE id=99"}}
	a := componentArtifact(t, c, inputType, outputType)
	reader, err := sqlreader.NewExecution(sqlreader.Config{Component: a.Component, InputType: inputType, OutputType: outputType, Plan: a.Reader, SQL: &dsql.SQLComponent{DB: db}})
	if err != nil {
		t.Fatal(err)
	}
	return &registry.RegisteredComponent{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: outputType, Reader: reader}
}

func readerFinalizerWriter(t *testing.T, db *sql.DB, onCommit func(context.Context)) *registry.RegisteredComponent {
	t.Helper()
	c := componentSpec("ReaderFinalizerWriter", "PATCH", "/reader-finalizer-writer", nil)
	c.Settings = &spec.Settings{Mutation: "patch"}
	c.RootView = &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "label", Source: "label"}}}
	a := componentArtifact(t, c, reflect.TypeFor[bufferedCallInput](), reflect.TypeFor[bufferedCallOutput]())
	native, err := writer.New(a.Component, reflect.TypeFor[bufferedCallInput](), reflect.TypeFor[bufferedCallOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	views, err := a.NewViewProvider(bootstrap.ViewRuntimeConfig{SQL: &dsql.SQLComponent{DB: db}})
	if err != nil {
		t.Fatal(err)
	}
	return &registry.RegisteredComponent{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[bufferedCallOutput](), Handler: native, Providers: []locator.Provider{views}, DataSource: dml.Source{DB: db, OnCommit: onCommit}}
}

func TestCanonicalReaderErrorFinalizerCompositionSQLite(t *testing.T) {
	for _, mode := range []string{"read only", "writer success", "writer veto", "writer panic", "ignored writer failure", "writer preparation failure", "prior writer stays terminal", "custom root stays terminal"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)", "INSERT INTO records VALUES(99,'native read')", "CREATE TABLE sqlx_sequence_reservations(table_name TEXT PRIMARY KEY,value INTEGER NOT NULL CHECK(value >= 0))", "CREATE TABLE journal_order(seq INTEGER PRIMARY KEY AUTOINCREMENT,id INTEGER NOT NULL)", "CREATE TRIGGER record_order AFTER INSERT ON records BEGIN INSERT INTO journal_order(id) VALUES(new.id); END"); err != nil {
				t.Fatal(err)
			}
			writes := strings.Contains(mode, "writer") && mode != "prior writer stays terminal"
			terminal := strings.HasSuffix(mode, "stays terminal")
			sentinel := errors.New("reader composition veto")
			commits, helperCalls, nestedCalls, completions, priorCalls := 0, 0, 0, 0, 0
			onCommit := func(context.Context) { commits++ }
			child := readerFinalizerWriter(t, db.DB, onCommit)
			if mode == "ignored writer failure" {
				child.Handler = &bufferedInjectorFaultNative{Handler: child.Handler.(*writer.Handler), cause: sentinel}
			}
			nested := readerFinalizerNative(t, db.DB, "ReaderFinalizerNested", "/reader-finalizer-nested", reflect.TypeFor[struct{}](), reflect.TypeFor[bufferedCallOutput]())
			assertPending := func(ctx context.Context, want int) error {
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
				if err == nil && count != want {
					err = fmt.Errorf("physical actions before root completion: got %d want %d", count, want)
				}
				return err
			}
			helperSpec := componentSpec("ReaderFinalizerHelper", "POST", "/reader-finalizer-helper", nil)
			ha := componentArtifact(t, helperSpec, reflect.TypeFor[struct{}](), reflect.TypeFor[bufferedCallOutput]())
			helper := &registry.RegisteredComponent{Component: ha.Component, Input: ha.Input, Output: ha.Output, OutputType: reflect.TypeFor[bufferedCallOutput](), Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				helperCalls++
				value, found, err := inv.Binder.Lookup(ctx, dexec.ComponentInvokerKey)
				if err != nil || !found {
					return nil, fmt.Errorf("helper invoker: found %v error %v", found, err)
				}
				// Reuse the adapter that asserts both DI contracts, choosing public Invoke.
				invoker := bufferedTestInvoker(t, value, true)
				nestedCalls++
				read, err := invoker.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: readerFinalizerTarget(nested.Component), Input: &struct{}{}})
				if err != nil {
					return nil, err
				}
				out := read.(*bufferedCallOutput)
				if len(out.Data) != 1 || out.Data[0].ID != 99 || out.Data[0].Label != "native read" {
					return nil, fmt.Errorf("nested native reader output: %+v", out)
				}
				if !writes {
					return out, nil
				}
				value, found, err = inv.Binder.Lookup(ctx, xh.DataKey)
				if err != nil || !found {
					return nil, fmt.Errorf("helper data: found %v error %v", found, err)
				}
				data := value.(xh.Data)
				if err = data.Execute("INSERT INTO records VALUES(1,'helper before')"); err != nil {
					return nil, err
				}
				written, childErr := invoker.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: readerFinalizerTarget(child.Component), Input: &bufferedCallInput{Rows: []*bufferedCallRow{{ID: 2, Label: "writer child"}}}})
				if mode == "ignored writer failure" {
					if !errors.Is(childErr, sentinel) {
						return nil, fmt.Errorf("child failure lost: %w", childErr)
					}
					return out, nil
				}
				if childErr != nil {
					return nil, childErr
				}
				childOut := written.(*bufferedCallOutput)
				if len(childOut.Data) != 1 || childOut.Data[0].ID != 2 {
					return nil, fmt.Errorf("writer output: %+v", childOut)
				}
				if err = assertPending(ctx, 0); err != nil {
					return nil, err
				}
				statement := "INSERT INTO records VALUES(3,'helper after')"
				if mode == "writer preparation failure" {
					statement = "INSERT INTO missing_reader_finalizer_table VALUES(3)"
				}
				if err = data.Execute(statement); err != nil {
					return nil, err
				}
				return out, assertPending(ctx, 0)
			})}
			if writes {
				helper.DataSource = dml.Source{DB: db.DB, OnCommit: onCommit}
			}
			inputType := reflect.TypeFor[readerFinalizerInput]()
			var prior *registry.RegisteredComponent
			if mode == "prior writer stays terminal" {
				inputType = reflect.TypeFor[readerFinalizerPriorInput]()
				c := componentSpec("ReaderFinalizerPrior", "POST", "/reader-finalizer-prior", nil)
				a := componentArtifact(t, c, reflect.TypeFor[struct{}](), reflect.TypeFor[bufferedCallOutput]())
				prior = &registry.RegisteredComponent{Component: a.Component, Input: a.Input, OutputType: reflect.TypeFor[bufferedCallOutput](), DataSource: dml.Source{DB: db.DB, OnCommit: onCommit}, Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
					priorCalls++
					value, found, err := inv.Binder.Lookup(ctx, xh.DataKey)
					if err != nil || !found {
						return nil, fmt.Errorf("prior data: %v", err)
					}
					return &bufferedCallOutput{}, value.(xh.Data).Execute("INSERT INTO records VALUES(1,'prior writer')")
				})}
			}
			root := readerFinalizerNative(t, db.DB, "ReaderFinalizerRoot", "/reader-finalizer-root", inputType, reflect.TypeFor[readerFinalizerOutput]())
			if mode == "custom root stays terminal" {
				root.Handler = rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) { return &readerFinalizerOutput{}, nil })
			}
			var retained xh.ComponentInvoker
			var original context.Context
			var terminalAttempt error
			probe := &readerFinalizerProbe{run: func(ctx context.Context, o *readerFinalizerOutput, cause error) error {
				retained, original = o.Invoker, ctx
				if cause != nil {
					return fmt.Errorf("successful reader callback received cause: %w", cause)
				}
				if mode != "custom root stays terminal" && (len(o.Data) != 1 || o.Data[0].ID != 99) {
					return fmt.Errorf("owning native read output: %+v", o.Data)
				}
				result, err := o.Invoker.Invoke(context.Background(), readerFinalizerPublicRequest(helper.Component, &struct{}{}))
				if terminal {
					terminalAttempt = err
					return nil
				}
				if err != nil {
					return err
				}
				if result.(*bufferedCallOutput).Data[0].ID != 99 {
					return errors.New("helper result lost nested output")
				}
				if err = assertPending(ctx, 0); err != nil {
					return err
				}
				switch mode {
				case "writer veto":
					return sentinel
				case "writer panic":
					panic(sentinel)
				}
				return nil
			}}
			root.Capabilities.Logger = probe
			registered := []*registry.RegisteredComponent{root, helper, nested, child}
			if prior != nil {
				registered = append(registered, prior)
			}
			rt, err := NewRuntime(registered)
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Shutdown(ctx)
			var outcome xh.Outcome
			actual, err := rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: readerFinalizerTarget(root.Component), Completion: func(o xh.Outcome) { completions++; outcome = o }})
			failure := mode != "read only" && mode != "writer success"
			if probe.calls != 1 || completions != 1 || probe.cause != nil {
				t.Fatalf("finalizer/completion count or cause: calls %d completions %d cause %v outcome %v", probe.calls, completions, probe.cause, err)
			}
			if (err != nil) != failure || (outcome.Error != nil) != failure {
				t.Fatalf("completion outcome mismatch: error %v outcome %+v", err, outcome)
			}
			if mode == "writer veto" || mode == "ignored writer failure" {
				if !errors.Is(err, sentinel) {
					t.Fatalf("failure identity lost: %v", err)
				}
			}
			if mode == "writer panic" {
				var panicErr *dexec.PanicError
				if !errors.As(err, &panicErr) || panicErr.Cause() != sentinel {
					t.Fatalf("panic identity lost: %v", err)
				}
			}
			if mode == "writer preparation failure" && !strings.Contains(err.Error(), "missing_reader_finalizer_table") {
				t.Fatalf("preparation failure lost: %v", err)
			}
			wantCalls := 1
			if terminal {
				wantCalls = 0
				if terminalAttempt == nil {
					t.Errorf("terminal callback attempt missing: outcome %v", err)
				}
			}
			if helperCalls != wantCalls || nestedCalls != wantCalls {
				t.Fatalf("composition counts: helper %d nested %d want %d", helperCalls, nestedCalls, wantCalls)
			}
			if mode == "prior writer stays terminal" && priorCalls != 1 {
				t.Fatalf("bound writer dependency executions %d want 1", priorCalls)
			}
			if !failure && actual == nil {
				t.Fatal("successful reader output lost")
			}
			wantCommits := 0
			if mode == "writer success" {
				wantCommits = 1
			}
			if commits != wantCommits {
				t.Fatalf("root commit count %d want %d", commits, wantCommits)
			}
			rows, scanErr := db.DB.QueryContext(ctx, "SELECT id FROM journal_order ORDER BY seq")
			if scanErr != nil {
				t.Fatal(scanErr)
			}
			var ids []int
			for rows.Next() {
				var id int
				if scanErr = rows.Scan(&id); scanErr != nil {
					t.Fatal(scanErr)
				}
				ids = append(ids, id)
			}
			if scanErr = rows.Err(); scanErr != nil {
				t.Fatal(scanErr)
			}
			rows.Close()
			var wantIDs []int
			if mode == "writer success" {
				wantIDs = []int{1, 2, 3}
			}
			if !reflect.DeepEqual(ids, wantIDs) {
				t.Fatalf("root action order/rollback: ids %v want %v", ids, wantIDs)
			}
			for _, lateCtx := range []context.Context{original, context.Background()} {
				if retained == nil {
					t.Fatal("output invoker was not retained")
				}
				if _, lateErr := retained.Invoke(lateCtx, readerFinalizerPublicRequest(helper.Component, &struct{}{})); lateErr == nil {
					t.Fatal("retained invoker accepted call after root completion")
				}
			}
			if helperCalls != wantCalls || nestedCalls != wantCalls {
				t.Fatal("late retained invoker reached business")
			}
		})
	}
}

func TestCanonicalReaderErrorFinalizerFailureProjectionSQLite(t *testing.T) {
	for _, mode := range []string{"binding", "JWT", "native read"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)", "INSERT INTO records VALUES(99,'native read')"); err != nil {
				t.Fatal(err)
			}
			sentinel := errors.New("private native reader fault")
			readCalls := 0
			readDB := db.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
				if call.Phase == "query" && strings.Contains(call.SQL, "records") {
					readCalls++
					if mode == "native read" {
						return sentinel
					}
				}
				return nil
			})
			inputType := reflect.TypeFor[struct{}]()
			if mode == "binding" {
				inputType = reflect.TypeFor[outputCapabilityInput]()
			}
			if mode == "JWT" {
				inputType = reflect.TypeFor[injectorJWTInput]()
			}
			c := componentSpec("ReaderFinalizerFailure", "GET", "/reader-finalizer-failure", []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}})
			c.RootView = &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id,label FROM records WHERE id=99"}}
			build := bootstrap.ArtifactInput{Component: c, InputType: inputType, OutputType: reflect.TypeFor[readerFinalizerOutput]()}
			if mode == "JWT" {
				key := testharness.NewJWT(t)
				factory, err := runtimeauth.New(ctx, &runtimeauth.Config{JWTValidator: key.Config()})
				if err != nil {
					t.Fatal(err)
				}
				build.CodecFactory = factory
			}
			a, err := bootstrap.BuildArtifact(build)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := sqlreader.NewExecution(sqlreader.Config{Component: a.Component, InputType: inputType, OutputType: reflect.TypeFor[readerFinalizerOutput](), Plan: a.Reader, SQL: &dsql.SQLComponent{DB: readDB}})
			if err != nil {
				t.Fatal(err)
			}
			probe := &readerFinalizerProbe{run: func(_ context.Context, _ *readerFinalizerOutput, cause error) error {
				if cause == nil {
					return errors.New("failure callback missing cause")
				}
				return &response.Error{Code: 409, Payload: map[string]any{"projected": mode}, Cause: cause}
			}}
			root := &registry.RegisteredComponent{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[readerFinalizerOutput](), Reader: reader, Capabilities: rh.InvocationCapabilities{Logger: probe}}
			rt, err := NewRuntime([]*registry.RegisteredComponent{root})
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Shutdown(ctx)
			req := httptest.NewRequest("GET", "/reader-finalizer-failure", nil)
			if mode == "JWT" {
				req.Header.Set("Authorization", "Bearer invalid")
			}
			scope, err := requestprovider.New(req)
			if err != nil {
				t.Fatal(err)
			}
			value, err := rt.ExecuteRoute(ctx, "GET", "/reader-finalizer-failure", scope)
			if err == nil || probe.calls != 1 || probe.cause == nil {
				t.Fatalf("failure callback: value %T error %v calls %d cause %v", value, err, probe.calls, probe.cause)
			}
			if !errors.Is(err, probe.cause) {
				t.Fatalf("observed cause identity lost: cause %v error %v", probe.cause, err)
			}
			if mode == "native read" && !errors.Is(err, sentinel) {
				t.Fatalf("native cause identity lost: %v", err)
			}
			if mode != "native read" {
				var bindingErr *bindly.BindingError
				if !errors.As(probe.cause, &bindingErr) {
					t.Fatalf("binding/JWT cause changed: %T %v", probe.cause, probe.cause)
				}
				if readCalls != 0 {
					t.Fatalf("early binding failure reached native SQL: %d", readCalls)
				}
			}
			if response.ErrorStatusCode(err, 500) != 409 {
				t.Fatalf("projection status lost: %v", err)
			}
			body, explicit := response.ErrorBody(err)
			if !explicit || !reflect.DeepEqual(body, map[string]any{"projected": mode}) {
				t.Fatalf("projection body lost: %#v", body)
			}
		})
	}
}

// A source-less helper may handle an ordinary reader result without that result
// becoming a transaction veto. Writer enrollment and protected failures retain
// their original sticky semantics, including overlapping invocations.
func TestCanonicalReaderCaughtErrorRetirementSQLite(t *testing.T) {
	for _, mode := range []string{"caught 403", "propagated 403", "prior writer", "reader dependency writer", "caught then writer", "caught then writer failure", "writer enrolled before reader retirement", "latched protected rejection", "bound reader error", "caught reader cancellation", "caught reader deadline", "caught reader panic"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)", "INSERT INTO records VALUES(99,'native read')", "CREATE TABLE sqlx_sequence_reservations(table_name TEXT PRIMARY KEY,value INTEGER NOT NULL CHECK(value >= 0))", "CREATE TABLE journal_order(seq INTEGER PRIMARY KEY AUTOINCREMENT,id INTEGER NOT NULL)", "CREATE TRIGGER record_order AFTER INSERT ON records BEGIN INSERT INTO journal_order(id) VALUES(new.id); END"); err != nil {
				t.Fatal(err)
			}
			readerCause := errors.New("private optional reader failure")
			writerCause := errors.New("later writer failure")
			faultCause := error(readerCause)
			if mode == "caught reader cancellation" {
				faultCause = context.Canceled
			}
			if mode == "caught reader deadline" {
				faultCause = context.DeadlineExceeded
			}
			faultDB := db.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
				if call.Phase == "query" && strings.Contains(call.SQL, "records") {
					return faultCause
				}
				return nil
			})
			inputType := reflect.TypeFor[struct{}]()
			if mode == "reader dependency writer" {
				inputType = reflect.TypeFor[readerFinalizerPriorInput]()
			}
			failed := readerFinalizerNative(t, faultDB, "CaughtReaderFailure", "/caught-reader-failure", inputType, reflect.TypeFor[readerFinalizerOutput]())
			var projection *response.Error
			entered := make(chan struct{})
			release := make(chan struct{})
			failedProbe := &readerFinalizerProbe{run: func(_ context.Context, _ *readerFinalizerOutput, cause error) error {
				if !errors.Is(cause, faultCause) {
					return fmt.Errorf("reader cause identity lost: %w", cause)
				}
				if mode == "caught reader panic" {
					panic(readerCause)
				}
				projection = &response.Error{Code: 403, Payload: map[string]any{"unavailable": true}, Cause: cause}
				if mode == "writer enrolled before reader retirement" {
					close(entered)
					select {
					case <-release:
					case <-time.After(5 * time.Second):
						return errors.New("writer enrollment timed out")
					}
				}
				return projection
			}}
			failed.Capabilities.Logger = failedProbe
			next := readerFinalizerNative(t, db.DB, "CaughtReaderNext", "/caught-reader-next", reflect.TypeFor[struct{}](), reflect.TypeFor[bufferedCallOutput]())
			commits, priorCalls, helperCalls, caught, nextCalls, rootCompletions := 0, 0, 0, 0, 0, 0
			onCommit := func(context.Context) { commits++ }
			writerChild := readerFinalizerWriter(t, db.DB, onCommit)
			if mode == "caught then writer failure" {
				writerChild.Handler = &bufferedInjectorFaultNative{Handler: writerChild.Handler.(*writer.Handler), cause: writerCause}
			}
			priorSpec := componentSpec("CaughtReaderPrior", "POST", "/reader-finalizer-prior", nil)
			priorArtifact := componentArtifact(t, priorSpec, reflect.TypeFor[struct{}](), reflect.TypeFor[bufferedCallOutput]())
			var guardErr error
			prior := &registry.RegisteredComponent{Component: priorArtifact.Component, Input: priorArtifact.Input, OutputType: reflect.TypeFor[bufferedCallOutput](), DataSource: dml.Source{DB: db.DB, OnCommit: onCommit}, Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				priorCalls++
				value, found, err := inv.Binder.Lookup(ctx, xh.DataKey)
				if err != nil || !found {
					return nil, fmt.Errorf("prior writer data: %v", err)
				}
				data := value.(xh.Data)
				if err = data.Execute("INSERT INTO records VALUES(1,'prior writer')"); err != nil {
					return nil, err
				}
				if mode == "latched protected rejection" {
					// An unauthorized flush records a real protected-ledger veto.
					// Discarding its immediate error must not restore admission.
					guardErr = data.Flush(ctx, "")
					if guardErr == nil {
						return nil, fmt.Errorf("protected flush unexpectedly admitted: %v", guardErr)
					}
				}
				return &bufferedCallOutput{}, nil
			})}
			helperType := reflect.TypeFor[struct{}]()
			if mode == "bound reader error" {
				helperType = reflect.TypeFor[readerFinalizerBoundFailureInput]()
			}
			helperArtifact := componentArtifact(t, componentSpec("CaughtReaderHelper", "POST", "/caught-reader-helper", nil), helperType, reflect.TypeFor[bufferedCallOutput]())
			var caughtErr, nextErr error
			helper := &registry.RegisteredComponent{Component: helperArtifact.Component, Input: helperArtifact.Input, OutputType: reflect.TypeFor[bufferedCallOutput](), Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				helperCalls++
				value, found, err := inv.Binder.Lookup(ctx, dexec.ComponentInvokerKey)
				if err != nil || !found {
					return nil, fmt.Errorf("helper invoker: %v", err)
				}
				invoker := bufferedTestInvoker(t, value, true)
				call := func(c *registry.RegisteredComponent, input any) (any, error) {
					return invoker.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: readerFinalizerTarget(c.Component), Input: input})
				}
				write := func() error {
					_, err := call(writerChild, &bufferedCallInput{Rows: []*bufferedCallRow{{ID: 2, Label: "introduced after optional read"}}})
					return err
				}
				if mode == "prior writer" {
					if _, err = call(prior, &struct{}{}); err != nil {
						return nil, err
					}
				}
				if mode == "latched protected rejection" {
					_, attempted := call(prior, &struct{}{})
					if attempted != nil && !errors.Is(attempted, guardErr) {
						return nil, fmt.Errorf("unexpected protected child outcome: %v", attempted)
					}
				}
				if mode == "writer enrolled before reader retirement" {
					results := make(chan error, 1)
					go func() { _, err := call(failed, nil); results <- err }()
					select {
					case <-entered:
					case early := <-results:
						return nil, fmt.Errorf("reader completed before enrollment: %w", early)
					case <-time.After(5 * time.Second):
						return nil, errors.New("reader finalizer did not enter")
					}
					writerErr := write()
					close(release)
					caughtErr = <-results
					if writerErr != nil {
						return nil, writerErr
					}
				} else {
					_, caughtErr = call(failed, nil)
				}
				if caughtErr == nil {
					return nil, errors.New("optional reader unexpectedly succeeded")
				}
				caught++
				if mode == "propagated 403" {
					return nil, caughtErr
				}
				nextCalls++
				read, err := call(next, &struct{}{})
				nextErr = err
				if err != nil {
					return &bufferedCallOutput{}, nil
				}
				out := read.(*bufferedCallOutput)
				if len(out.Data) != 1 || out.Data[0].ID != 99 {
					return nil, fmt.Errorf("next native reader output: %+v", out)
				}
				if mode == "caught then writer" || mode == "caught then writer failure" {
					if writerErr := write(); writerErr != nil {
						if mode != "caught then writer failure" || !errors.Is(writerErr, writerCause) {
							return nil, writerErr
						}
						return out, nil // A caught writer failure still vetoes the root.
					}
					pending := 0
					tx, err := dexec.InvocationTransaction(ctx, db.DB)
					if err != nil {
						return nil, err
					}
					if tx != nil {
						err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM journal_order").Scan(&pending)
					} else {
						err = db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM journal_order").Scan(&pending)
					}
					if err != nil || pending != 0 {
						return nil, fmt.Errorf("introduced writer drained before root completion: count %d error %v", pending, err)
					}
				}
				return out, nil
			})}
			root := readerFinalizerNative(t, db.DB, "CaughtReaderRoot", "/caught-reader-root", reflect.TypeFor[readerFinalizerInput](), reflect.TypeFor[readerFinalizerOutput]())
			rootProbe := &readerFinalizerProbe{run: func(ctx context.Context, o *readerFinalizerOutput, cause error) error {
				if cause != nil {
					return fmt.Errorf("root read failed before composition: %w", cause)
				}
				_, err := o.Invoker.Invoke(ctx, readerFinalizerPublicRequest(helper.Component, nil))
				if mode == "bound reader error" {
					caughtErr = err
					caught++
					return nil
				}
				return err
			}}
			root.Capabilities.Logger = rootProbe
			rt, err := NewRuntime([]*registry.RegisteredComponent{root, helper, failed, next, prior, writerChild})
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Shutdown(ctx)
			var outcome xh.Outcome
			_, err = rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: readerFinalizerTarget(root.Component), Completion: func(o xh.Outcome) { rootCompletions++; outcome = o }})
			success := mode == "caught 403" || mode == "caught then writer"
			if (err == nil) != success || (outcome.Error == nil) != success {
				t.Fatalf("caught reader root outcome: error %v outcome %+v", err, outcome)
			}
			if rootProbe.calls != 1 || rootCompletions != 1 || caught != 1 {
				t.Fatalf("once-only root lifecycle: output %d completion %d caught %d error %v", rootProbe.calls, rootCompletions, caught, err)
			}
			if mode == "latched protected rejection" {
				if guardErr == nil || !errors.Is(err, guardErr) || priorCalls != 1 || failedProbe.calls != 0 {
					t.Fatalf("latched protected failure cleared: error %v prior calls %d reader finalizer calls %d", err, priorCalls, failedProbe.calls)
				}
			} else {
				if failedProbe.calls != 1 || !errors.Is(failedProbe.cause, faultCause) {
					t.Fatalf("child projection count/cause: count %d cause %v", failedProbe.calls, failedProbe.cause)
				}
				if mode == "caught reader panic" {
					var panicErr *dexec.PanicError
					if !errors.As(err, &panicErr) || panicErr.Cause() != readerCause {
						t.Fatalf("caught panic no longer sticky: %v", err)
					}
				} else if mode == "caught then writer failure" {
					if !errors.Is(err, writerCause) {
						t.Fatalf("caught writer failure no longer sticky: %v", err)
					}
				} else {
					if !errors.Is(caughtErr, projection) || response.ErrorStatusCode(caughtErr, 500) != 403 {
						t.Fatalf("reader error result identity/status changed: %v", caughtErr)
					}
					if !success && (!errors.Is(err, projection) || response.ErrorStatusCode(err, 500) != 403) {
						t.Fatalf("propagated reader error identity/status changed: %v", err)
					}
				}
			}
			if mode == "prior writer" || mode == "reader dependency writer" {
				if priorCalls != 1 {
					t.Fatalf("writer dependency executions %d want 1", priorCalls)
				}
			}
			if mode == "bound reader error" && helperCalls != 0 {
				t.Fatal("binding failure reached helper business")
			}
			if success && (nextCalls != 1 || nextErr != nil) {
				t.Fatalf("handled optional reader failure prevented next read: calls %d error %v", nextCalls, nextErr)
			}
			wantCommits, wantActions := 0, 0
			if mode == "caught then writer" {
				wantCommits, wantActions = 1, 1
			}
			if commits != wantCommits {
				t.Fatalf("shared root commit count %d want %d", commits, wantCommits)
			}
			actions := 0
			if scanErr := db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM journal_order").Scan(&actions); scanErr != nil {
				t.Fatal(scanErr)
			}
			if actions != wantActions {
				t.Fatalf("root commit/rollback actions %d want %d", actions, wantActions)
			}
		})
	}
}
