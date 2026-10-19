package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/engine"
	hp "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/sqlx"
	xh "github.com/viant/xdatly/handler"
)

type readOnlyGroupInput struct{ A, B *readOnlyGroupOutput }
type readOnlyBranchInput struct {
	Nested  *bufferedCallOutput    `parameter:",kind=component,in=GET:/read-only-nested"`
	Invoker xh.ComponentInvoker    `parameter:",kind=component_invoker"`
	Native  dexec.ComponentInvoker `parameter:",kind=component_invoker"`
	Binder  xh.Binder              `parameter:",kind=internal"`
}
type readOnlyWriterBranchInput struct {
	Nested *bufferedCallOutput `parameter:",kind=component,in=PATCH:/reader-finalizer-writer"`
}
type readOnlyGroupOutput struct {
	Data     []*bufferedCallRow `parameter:",kind=output,in=view" json:"data"`
	Logger   xh.Logger          `parameter:",kind=logger" json:"-"`
	Complete bool               `json:"complete"`
}

type readOnlyCommitOutput struct {
	Data []*bufferedCallRow `parameter:",kind=output,in=view"`
}

func (*readOnlyCommitOutput) Finalize(context.Context) error {
	return errors.New("post-commit callback must not execute early")
}

type readOnlyProbe struct {
	calls atomic.Int32
	run   func(context.Context, *readOnlyBranchInput, *readOnlyGroupOutput) error
}

type readOnlyCaptureReader struct{ dexec.Reader }

func (r readOnlyCaptureReader) Read(ctx context.Context, input any, binder xh.Binder, resolver sqlx.ParameterResolver) (any, error) {
	if branch, ok := input.(*readOnlyBranchInput); ok {
		branch.Binder = binder
	}
	return r.Reader.Read(ctx, input, binder, resolver)
}

func (*readOnlyProbe) Debug(string, ...any) {}
func (*readOnlyProbe) Info(string, ...any)  {}
func (*readOnlyProbe) Warn(string, ...any)  {}
func (*readOnlyProbe) Error(string, ...any) {}
func (o *readOnlyGroupOutput) Finalize(ctx context.Context, cause error) error {
	if cause != nil {
		return cause
	}
	i, ok := ctx.Value(reflect.TypeFor[*readOnlyBranchInput]()).(*readOnlyBranchInput)
	if !ok || i.Nested == nil || len(i.Nested.Data) != 1 {
		return errors.New("missing nested native reader output")
	}
	p := o.Logger.(*readOnlyProbe)
	p.calls.Add(1)
	if p.run != nil {
		if err := p.run(ctx, i, o); err != nil {
			return err
		}
	}
	o.Complete = true
	return nil
}

func newReadOnlyGroupRuntime(t *testing.T, db *sqlite.Harness, probes [2]*readOnlyProbe, edit func([]*registry.RegisteredComponent)) (*Runtime, *registry.RegisteredComponent, *atomic.Int32) {
	t.Helper()
	nested := readerFinalizerNative(t, db.DB, "ReadOnlyNested", "/read-only-nested", reflect.TypeFor[struct{}](), reflect.TypeFor[bufferedCallOutput]())
	a := readerFinalizerNative(t, db.DB, "ReadOnlyA", "/read-only-a", reflect.TypeFor[readOnlyBranchInput](), reflect.TypeFor[readOnlyGroupOutput]())
	b := readerFinalizerNative(t, db.DB, "ReadOnlyB", "/read-only-b", reflect.TypeFor[readOnlyBranchInput](), reflect.TypeFor[readOnlyGroupOutput]())
	a.Capabilities.Logger, b.Capabilities.Logger = probes[0], probes[1]
	a.Reader, b.Reader = readOnlyCaptureReader{a.Reader}, readOnlyCaptureReader{b.Reader}
	parameters := []*spec.Parameter{}
	for _, name := range []string{"A", "B"} {
		path := "/read-only-a"
		if name == "B" {
			path = "/read-only-b"
		}
		parameters = append(parameters, &spec.Parameter{Name: name, Source: spec.BindSource{Kind: "component", Name: "GET:" + path}, ResolutionGroup: &spec.ResolutionGroupSpec{Name: "reads", Mode: "dependencies"}})
	}
	artifact := componentArtifact(t, componentSpec("ReadOnlyRoot", "GET", "/read-only-root", parameters), reflect.TypeFor[readOnlyGroupInput](), reflect.TypeFor[componentParentOutput]())
	parentCalls := &atomic.Int32{}
	root := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[componentParentOutput](), DataSource: dml.Source{DB: db.DB}, Handler: rh.HandlerFunc(func(_ context.Context, invocation rh.Invocation) (any, error) {
		parentCalls.Add(1)
		i := invocation.Input.(*readOnlyGroupInput)
		if i.A == nil || i.B == nil || !i.A.Complete || !i.B.Complete {
			return nil, errors.New("parent executed before dependency finalizers")
		}
		return &componentParentOutput{Message: i.A.Data[0].Label + ":" + i.B.Data[0].Label}, nil
	})}
	writer := readerFinalizerWriter(t, db.DB, nil)
	registrations := []*registry.RegisteredComponent{root, a, b, nested, writer}
	if edit != nil {
		edit(registrations)
	}
	rt, err := NewRuntime(registrations)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	return rt, root, parentCalls
}

func readOnlyDatabase(t *testing.T) *sqlite.Harness {
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT)", "INSERT INTO records VALUES(99,'native')"))
	return db
}

func TestReadOnlyDependencyGroupRejectsCaughtEffects(t *testing.T) {
	for _, effect := range []string{"writer finalizer", "DML", "transaction start", "independent reader", "nested writer", "custom reader", "provider override", "independent metadata", "post-commit", "message bus", "raw connector", "canceled reader"} {
		t.Run(effect, func(t *testing.T) {
			db := readOnlyDatabase(t)
			var rejected atomic.Bool
			probe := &readOnlyProbe{run: func(ctx context.Context, input *readOnlyBranchInput, output *readOnlyGroupOutput) error {
				var err error
				switch effect {
				case "DML", "transaction start":
					key := xh.DataKey
					if effect == "transaction start" {
						key = xh.TransactionStarterKey
					}
					value, found, lookupErr := input.Binder.Lookup(ctx, key)
					if lookupErr != nil || !found {
						return fmt.Errorf("managed capability lookup: found=%v error=%v", found, lookupErr)
					}
					if effect == "DML" {
						err = value.(xh.Data).Execute("INSERT INTO records VALUES(1,'forbidden')")
					} else {
						err = value.(xh.TransactionStarter).Start(context.Background())
					}
				case "message bus", "raw connector":
					key := xh.MessageBusKey
					if effect == "raw connector" {
						key = xh.ConnectorKey
					}
					_, _, err = input.Binder.Lookup(ctx, key)
				case "canceled reader":
					call, cancel := context.WithCancel(ctx)
					cancel()
					_, err = input.Invoker.Invoke(call, xh.ComponentRequest{Target: xh.ComponentTarget{Scope: "example.com/components", Name: "ReadOnlyNested", Method: "GET", Path: "/read-only-nested"}})
				default:
					path, name, method := "/reader-finalizer-writer", "ReaderFinalizerWriter", "PATCH"
					if effect == "independent reader" {
						path, name, method = "/read-only-nested", "ReadOnlyNested", "GET"
					}
					if effect == "independent reader" {
						_, err = input.Native.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: "example.com/components", Name: name}, Route: spec.RouteRef{Method: method, Path: path}}, IndependentChildTransactions: true})
					} else {
						_, err = input.Invoker.Invoke(context.Background(), xh.ComponentRequest{Target: xh.ComponentTarget{Scope: "example.com/components", Name: name, Method: method, Path: path}, Input: &bufferedCallInput{Rows: []*bufferedCallRow{{ID: 1, Label: "forbidden"}}}})
					}
				}
				expected := engine.ErrReadOnlyDependency
				if effect == "canceled reader" {
					expected = context.Canceled
				}
				if !errors.Is(err, expected) {
					return fmt.Errorf("expected sticky read-only rejection, got %v", err)
				}
				rejected.Store(true)
				return nil // Deliberately ignore the immediate error.
			}}
			edit := func(registrations []*registry.RegisteredComponent) {
				switch effect {
				case "nested writer":
					replacement := readerFinalizerNative(t, db.DB, "ReadOnlyA", "/read-only-a", reflect.TypeFor[readOnlyWriterBranchInput](), reflect.TypeFor[readOnlyGroupOutput]())
					replacement.Capabilities.Logger = probe
					registrations[1] = replacement
				case "custom reader":
					registrations[1].Handler = rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) {
						return nil, errors.New("custom handler must not execute")
					})
				case "provider override":
					registrations[1].Providers = []locator.Provider{hp.Named("component", func(context.Context, reflect.Type, string) (any, bool, error) {
						return nil, false, errors.New("override must not execute")
					})}
				case "independent metadata":
					registrations[1].Component.Settings = &spec.Settings{IndependentChildTransactions: true}
				case "post-commit":
					registrations[1] = readerFinalizerNative(t, db.DB, "ReadOnlyA", "/read-only-a", reflect.TypeFor[struct{}](), reflect.TypeFor[readOnlyCommitOutput]())
				}
			}
			rt, root, calls := newReadOnlyGroupRuntime(t, db, [2]*readOnlyProbe{probe, {}}, edit)
			_, err := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: readerFinalizerTarget(root.Component)})
			expected := engine.ErrReadOnlyDependency
			if effect == "canceled reader" {
				expected = context.Canceled
			}
			require.ErrorIs(t, err, expected)
			require.Zero(t, calls.Load())
			if effect == "DML" || effect == "writer finalizer" || effect == "transaction start" {
				require.True(t, rejected.Load(), err)
			}
			var count int
			require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM records WHERE id=1").Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestReadOnlyDependencyGroupErrorsAreDeclarationOrdered(t *testing.T) {
	db := readOnlyDatabase(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	entered := make(chan string, 2)
	aRelease, bRelease, bReturned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	aError, bError := errors.New("actual A error"), errors.New("actual B error")
	a := &readOnlyProbe{run: func(ctx context.Context, _ *readOnlyBranchInput, _ *readOnlyGroupOutput) error {
		entered <- "A"
		select {
		case <-aRelease:
			return aError
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	b := &readOnlyProbe{run: func(ctx context.Context, _ *readOnlyBranchInput, _ *readOnlyGroupOutput) error {
		entered <- "B"
		select {
		case <-bRelease:
			close(bReturned)
			return bError
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	rt, root, calls := newReadOnlyGroupRuntime(t, db, [2]*readOnlyProbe{a, b}, nil)
	done := make(chan error, 1)
	go func() {
		_, err := rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: readerFinalizerTarget(root.Component)})
		done <- err
	}()
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(bRelease)
	select {
	case <-bReturned:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(aRelease)
	select {
	case err := <-done:
		var grouped *bindly.ResolutionGroupError
		require.ErrorAs(t, err, &grouped)
		require.Len(t, grouped.Failures(), 2)
		require.Equal(t, "A", grouped.Failures()[0].Path)
		require.Equal(t, "B", grouped.Failures()[1].Path)
		require.ErrorIs(t, err, aError)
		require.ErrorIs(t, err, bError)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.Zero(t, calls.Load())
}

func TestReadOnlyDependencyGroupPanicCancelsAndJoinsPeers(t *testing.T) {
	db := readOnlyDatabase(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	entered := make(chan struct{}, 2)
	trigger, cleanup, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	a := &readOnlyProbe{run: func(ctx context.Context, _ *readOnlyBranchInput, _ *readOnlyGroupOutput) error {
		entered <- struct{}{}
		select {
		case <-trigger:
			panic("private driver/hook panic")
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	b := &readOnlyProbe{run: func(ctx context.Context, _ *readOnlyBranchInput, _ *readOnlyGroupOutput) error {
		entered <- struct{}{}
		<-ctx.Done()
		close(cleanup)
		<-release
		return ctx.Err()
	}}
	rt, root, calls := newReadOnlyGroupRuntime(t, db, [2]*readOnlyProbe{a, b}, nil)
	done := make(chan error, 1)
	go func() {
		_, err := rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: readerFinalizerTarget(root.Component)})
		done <- err
	}()
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(trigger)
	select {
	case <-cleanup:
	case <-ctx.Done():
		t.Fatal("panic did not cancel its peer")
	}
	select {
	case err := <-done:
		t.Fatalf("panic returned before peer cleanup: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-done:
		var panicErr *dexec.PanicError
		require.ErrorAs(t, err, &panicErr)
		require.NotContains(t, err.Error(), "private driver/hook panic")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.Zero(t, calls.Load())
}

func TestReadOnlyDependencyUndeclaredBindingsStaySequential(t *testing.T) {
	db := readOnlyDatabase(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	entered := make(chan string, 2)
	aRelease, bRelease := make(chan struct{}), make(chan struct{})
	probe := func(name string, release <-chan struct{}) *readOnlyProbe {
		return &readOnlyProbe{run: func(ctx context.Context, _ *readOnlyBranchInput, _ *readOnlyGroupOutput) error {
			entered <- name
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}}
	}
	rt, root, calls := newReadOnlyGroupRuntime(t, db, [2]*readOnlyProbe{probe("A", aRelease), probe("B", bRelease)}, func(registrations []*registry.RegisteredComponent) {
		c := registrations[0].Component.Clone()
		for _, parameter := range c.Parameters {
			parameter.ResolutionGroup = nil
		}
		artifact := componentArtifact(t, c, reflect.TypeFor[readOnlyGroupInput](), reflect.TypeFor[componentParentOutput]())
		registrations[0].Component, registrations[0].Input = artifact.Component, artifact.Input
	})
	done := make(chan error, 1)
	go func() {
		_, err := rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: readerFinalizerTarget(root.Component)})
		done <- err
	}()
	select {
	case name := <-entered:
		require.Equal(t, "A", name)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case name := <-entered:
		t.Fatalf("undeclared sibling started concurrently: %s", name)
	default:
	}
	close(aRelease)
	select {
	case name := <-entered:
		require.Equal(t, "B", name)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(bRelease)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.EqualValues(t, 1, calls.Load())
}

func TestReadOnlyDependencyRetainedBinderCannotRegainWriterAuthority(t *testing.T) {
	db := readOnlyDatabase(t)
	var saved xh.Binder
	probe := &readOnlyProbe{run: func(_ context.Context, input *readOnlyBranchInput, _ *readOnlyGroupOutput) error {
		saved = input.Binder
		return nil
	}}
	var denied error
	rt, root, _ := newReadOnlyGroupRuntime(t, db, [2]*readOnlyProbe{probe, {}}, func(registrations []*registry.RegisteredComponent) {
		original := registrations[0].Handler
		registrations[0].Handler = rh.HandlerFunc(func(ctx context.Context, invocation rh.Invocation) (any, error) {
			value, found, err := saved.Lookup(context.Background(), xh.ComponentInvokerKey)
			if err != nil || !found {
				return nil, fmt.Errorf("retained invoker lookup: %v", err)
			}
			_, denied = value.(xh.ComponentInvoker).Invoke(context.Background(), xh.ComponentRequest{Target: xh.ComponentTarget{Scope: "example.com/components", Name: "ReaderFinalizerWriter", Method: "PATCH", Path: "/reader-finalizer-writer"}, Input: &bufferedCallInput{Rows: []*bufferedCallRow{{ID: 1, Label: "forbidden"}}}})
			return original.Execute(ctx, invocation) // Ignore the immediate rejection.
		})
	})
	_, err := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: readerFinalizerTarget(root.Component)})
	require.ErrorIs(t, denied, engine.ErrReadOnlyDependency)
	require.ErrorIs(t, err, engine.ErrReadOnlyDependency)
	var count int
	require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM records WHERE id=1").Scan(&count))
	require.Zero(t, count)
}

func TestReadOnlyDependencyGroupInheritedTransaction(t *testing.T) {
	db := readOnlyDatabase(t)
	tx, err := db.DB.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.Exec("UPDATE records SET label='uncommitted' WHERE id=99")
	require.NoError(t, err)
	rt, root, calls := newReadOnlyGroupRuntime(t, db, [2]*readOnlyProbe{{}, {}}, func(registrations []*registry.RegisteredComponent) {
		registrations[0].DataSource = dml.Source{DB: db.DB, Tx: tx}
	})
	result, err := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: readerFinalizerTarget(root.Component)})
	require.NoError(t, err)
	require.Equal(t, "uncommitted:uncommitted", result.(*componentParentOutput).Message)
	require.EqualValues(t, 1, calls.Load())
	var value string
	require.NoError(t, tx.QueryRow("SELECT label FROM records WHERE id=99").Scan(&value))
	require.Equal(t, "uncommitted", value)
	require.NoError(t, tx.Rollback())
	require.NoError(t, db.DB.QueryRow("SELECT label FROM records WHERE id=99").Scan(&value))
	require.Equal(t, "native", value)
}

func TestReadOnlyDependencyGroupCancellationJoinsFinalizers(t *testing.T) {
	db := readOnlyDatabase(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{}, 2)
	cleanup := make(chan struct{}, 2)
	release := make(chan struct{})
	probe := &readOnlyProbe{run: func(ctx context.Context, _ *readOnlyBranchInput, _ *readOnlyGroupOutput) error {
		entered <- struct{}{}
		<-ctx.Done()
		cleanup <- struct{}{}
		<-release
		return ctx.Err()
	}}
	rt, root, calls := newReadOnlyGroupRuntime(t, db, [2]*readOnlyProbe{probe, probe}, nil)
	done := make(chan error, 1)
	go func() {
		_, err := rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: readerFinalizerTarget(root.Component)})
		done <- err
	}()
	await := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("synchronized dependency event timed out")
		}
	}
	await(entered)
	await(entered)
	cancel()
	await(cleanup)
	await(cleanup)
	select {
	case err := <-done:
		t.Fatalf("returned before started work joined: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled group did not join")
	}
	require.Zero(t, calls.Load())
}
