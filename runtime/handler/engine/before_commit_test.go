package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	dsql "github.com/viant/datly/sql"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

type beforeCommitProbe struct {
	validate func(context.Context, xhandler.ComponentInvoker) error
}

func (p *beforeCommitProbe) BeforeCommit(ctx context.Context, invoker xhandler.ComponentInvoker) error {
	return p.validate(ctx, invoker)
}

var _ xhandler.BeforeCommitValidator = (*beforeCommitProbe)(nil)

func TestBeforeCommitValidationFailureRollsBackNativeWriter(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	calls := 0
	denied := errors.New("pending write rejected")
	output := &beforeCommitProbe{validate: func(context.Context, xhandler.ComponentInvoker) error { calls++; return denied }}
	input := testRouteInput(t, reflect.TypeOf(struct{}{}))
	_, err := New().Execute(ctx, Request{Input: input, OutputType: reflect.TypeOf(beforeCommitProbe{}), DataSource: sqldml.Source{DB: h.DB}, Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
		value, _, err := invocation.Binder.Lookup(ctx, xhandler.DataKey)
		if err != nil {
			return nil, err
		}
		if err = value.(xhandler.Data).Execute("INSERT INTO audit VALUES(1)"); err != nil {
			return nil, err
		}
		return output, nil
	})})
	if calls != 1 || !errors.Is(err, denied) {
		t.Errorf("before-commit calls=%d error=%v; want one rejected validation", calls, err)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{0}})
}

func TestBeforeCommitNativeReadsPendingRowsAndRetiresInvoker(t *testing.T) {
	ctx := context.WithValue(context.Background(), beforeCommitTestValue{}, "caller")
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "PRAGMA journal_mode=WAL", "CREATE TABLE audit(id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	connector := &dsql.SQLComponent{DB: h.DB}
	if err := connector.RegisterConnector("main", h.DB); err != nil {
		t.Fatal(err)
	}
	input := testRouteInput(t, reflect.TypeOf(struct{}{}))
	var retained xhandler.ComponentInvoker
	calls := 0
	output := &beforeCommitProbe{validate: func(ctx context.Context, invoker xhandler.ComponentInvoker) error {
		calls++
		retained = invoker
		if ctx.Value(beforeCommitTestValue{}) != "caller" {
			t.Fatal("caller values lost")
		}
		if tx, err := dexec.InvocationTransaction(ctx, h.DB); err != nil || tx != nil {
			t.Fatalf("callback inherited pending DB: %v %v", tx, err)
		}
		var outside int
		if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&outside); err != nil {
			return err
		}
		if outside != 0 {
			t.Fatalf("write committed before validation: %d", outside)
		}
		result, err := invoker.Invoke(ctx, xhandler.ComponentRequest{})
		if err != nil {
			return err
		}
		if result != 1 {
			t.Fatalf("pending reader saw %v", result)
		}
		return nil
	}}
	child := lifecycleInvoker(func(ctx context.Context, _ xhandler.ComponentRequest) (any, error) {
		if err := CheckComponentMutation(ctx, true); err != nil {
			return nil, err
		}
		return New().Execute(ctx, Request{Input: input, CanonicalReader: true, Capabilities: rhandler.InvocationCapabilities{Connector: connector}, Handler: rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
			connection, err := connector.Resolve(ctx, "main")
			if err != nil {
				return nil, err
			}
			if connection.Tx == nil {
				return nil, errors.New("missing pending transaction")
			}
			var total int
			err = connection.Tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&total)
			return total, err
		})})
	})
	_, err := New().Execute(ctx, Request{Input: input, OutputType: reflect.TypeOf(beforeCommitProbe{}), ComponentInvoker: handlerprovider.Static(xhandler.ComponentInvokerKey, child), DataSource: sqldml.Source{DB: h.DB}, Handler: rhandler.HandlerFunc(func(ctx context.Context, in rhandler.Invocation) (any, error) {
		value, _, err := in.Binder.Lookup(ctx, xhandler.DataKey)
		if err != nil {
			return nil, err
		}
		return output, value.(xhandler.Data).Execute("INSERT INTO audit VALUES(1)")
	})})
	if err != nil || calls != 1 {
		t.Fatalf("calls=%d error=%v", calls, err)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{1}})
	if _, err = retained.Invoke(ctx, xhandler.ComponentRequest{}); !errors.Is(err, ErrBeforeCommitReadOnly) {
		t.Fatalf("retained invoker escaped callback: %v", err)
	}
}

type beforeCommitTestValue struct{}

func TestBeforeCommitCaughtWriterAttemptStillRollsBack(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	input := testRouteInput(t, reflect.TypeOf(struct{}{}))
	ran := false
	output := &beforeCommitProbe{validate: func(ctx context.Context, invoker xhandler.ComponentInvoker) error {
		_, _ = invoker.Invoke(ctx, xhandler.ComponentRequest{})
		return nil
	}}
	child := lifecycleInvoker(func(ctx context.Context, _ xhandler.ComponentRequest) (any, error) {
		return New().Execute(ctx, Request{Input: input, Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) { ran = true; return nil, nil })})
	})
	_, err := New().Execute(ctx, Request{Input: input, OutputType: reflect.TypeOf(beforeCommitProbe{}), ComponentInvoker: handlerprovider.Static(xhandler.ComponentInvokerKey, child), DataSource: sqldml.Source{DB: h.DB}, Handler: rhandler.HandlerFunc(func(ctx context.Context, in rhandler.Invocation) (any, error) {
		value, _, err := in.Binder.Lookup(ctx, xhandler.DataKey)
		if err != nil {
			return nil, err
		}
		return output, value.(xhandler.Data).Execute("INSERT INTO audit VALUES(1)")
	})})
	if !errors.Is(err, ErrBeforeCommitReadOnly) || ran {
		t.Fatalf("write attempt allowed: ran=%t err=%v", ran, err)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{0}})
}

func TestBeforeCommitSkipsFailedDrainAndRollsBackCancellation(t *testing.T) {
	for _, mode := range []string{"drain-error", "cancel", "panic"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER CHECK(id>0))"); err != nil {
				t.Fatal(err)
			}
			calls := 0
			output := &beforeCommitProbe{validate: func(context.Context, xhandler.ComponentInvoker) error {
				calls++
				if mode == "panic" {
					panic("validation failed")
				}
				cancel()
				return nil
			}}
			statement := "INSERT INTO audit VALUES(1)"
			if mode == "drain-error" {
				statement = "INSERT INTO audit VALUES(-1)"
			}
			_, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(struct{}{})), OutputType: reflect.TypeOf(beforeCommitProbe{}), DataSource: sqldml.Source{DB: h.DB}, Handler: rhandler.HandlerFunc(func(ctx context.Context, in rhandler.Invocation) (any, error) {
				v, _, err := in.Binder.Lookup(ctx, xhandler.DataKey)
				if err != nil {
					return nil, err
				}
				return output, v.(xhandler.Data).Execute(statement)
			})})
			if err == nil {
				t.Fatal("failed validation/drain committed")
			}
			want := 1
			if mode == "drain-error" {
				want = 0
			}
			if calls != want {
				t.Fatalf("calls=%d want=%d", calls, want)
			}
			h.AssertQuery(t, context.Background(), sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{0}})
		})
	}
}

func TestBeforeCommitNestedOutputsRunOnceBeforeRootCommit(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	input := testRouteInput(t, reflect.TypeOf(struct{}{}))
	var order []string
	output := func(label string) *beforeCommitProbe {
		return &beforeCommitProbe{validate: func(context.Context, xhandler.ComponentInvoker) error { order = append(order, label); return nil }}
	}
	source := sqldml.Source{DB: h.DB}
	_, err := New().Execute(ctx, Request{Input: input, OutputType: reflect.TypeOf(beforeCommitProbe{}), DataSource: source, Handler: rhandler.HandlerFunc(func(ctx context.Context, in rhandler.Invocation) (any, error) {
		_, err := New().Execute(PrepareComponent(ctx, ComponentImperative, ""), Request{Input: input, OutputType: reflect.TypeOf(beforeCommitProbe{}), DataSource: source, Handler: rhandler.HandlerFunc(func(ctx context.Context, child rhandler.Invocation) (any, error) {
			v, _, err := child.Binder.Lookup(ctx, xhandler.DataKey)
			if err != nil {
				return nil, err
			}
			return output("child"), v.(xhandler.Data).Execute("INSERT INTO audit VALUES(1)")
		})})
		if err != nil {
			return nil, err
		}
		if len(order) != 0 {
			t.Fatal("child validation ran before root drain")
		}
		return output("root"), nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"child", "root"}) {
		t.Fatalf("validation order: %v", order)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{1}})
}

func TestBeforeCommitRejectsOwnerWithoutPreparation(t *testing.T) {
	ctx := context.Background()
	data := &panicFlushOnlyData{cause: "must not flush"}
	root := neutralDataScope()
	root.data = data
	calls := 0
	if err := root.registerBeforeCommit(ctx, nil, &beforeCommitProbe{validate: func(context.Context, xhandler.ComponentInvoker) error { calls++; return nil }}); err != nil {
		t.Fatal(err)
	}
	err := root.complete(ctx, nil)
	if err == nil || calls != 0 || data.flushes != 0 {
		t.Fatalf("unsupported owner: error=%v callbacks=%d flushes=%d", err, calls, data.flushes)
	}
}

func TestBeforeCommitCaughtForeignReaderStillRollsBack(t *testing.T) {
	ctx := context.Background()
	owner, foreign := sqlite.New(t), sqlite.New(t)
	if err := owner.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	input := testRouteInput(t, reflect.TypeOf(struct{}{}))
	connector := &dsql.SQLComponent{DB: foreign.DB}
	if err := connector.RegisterConnector("foreign", foreign.DB); err != nil {
		t.Fatal(err)
	}
	child := lifecycleInvoker(func(ctx context.Context, _ xhandler.ComponentRequest) (any, error) {
		return New().Execute(ctx, Request{Input: input, CanonicalReader: true, Handler: rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
			_, err := connector.Resolve(ctx, "foreign")
			return nil, err
		})})
	})
	output := &beforeCommitProbe{validate: func(ctx context.Context, invoker xhandler.ComponentInvoker) error {
		_, err := invoker.Invoke(ctx, xhandler.ComponentRequest{})
		if !errors.Is(err, ErrBeforeCommitReadOnly) {
			t.Errorf("foreign source error=%v", err)
		}
		return nil // Catching a forbidden read cannot permit commit.
	}}
	_, err := New().Execute(ctx, Request{Input: input, OutputType: reflect.TypeOf(beforeCommitProbe{}), DataSource: sqldml.Source{DB: owner.DB}, ComponentInvoker: handlerprovider.Static(xhandler.ComponentInvokerKey, child), Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
		value, _, err := invocation.Binder.Lookup(ctx, xhandler.DataKey)
		if err != nil {
			return nil, err
		}
		return output, value.(xhandler.Data).Execute("INSERT INTO audit VALUES(1)")
	})})
	if !errors.Is(err, ErrBeforeCommitReadOnly) {
		t.Fatalf("caught foreign reader allowed completion: %v", err)
	}
	owner.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{0}})
}

func TestBeforeCommitAuthorityDoesNotRestoreClosedWriter(t *testing.T) {
	ctx := context.Background()
	owner := neutralDataScope()
	permit := &beforeCommitFrame{owner: owner, active: true}
	read := context.WithValue(ctx, beforeCommitReadKey{}, permit)
	restore := RetainMutationAuthority(withDataScope(ctx, owner))
	restored := restore(read)
	if scope, _ := restored.Value(dataScopeContextKey{}).(*dataScope); scope != nil {
		t.Fatal("validation read regained closed writer ownership")
	}
	if beforeCommitRead(restored) != permit {
		t.Fatal("validation permission lost")
	}
	permit.active = false
	restored = restore(read)
	if err := CheckComponentMutation(restored, true); !errors.Is(err, ErrBeforeCommitReadOnly) {
		t.Fatalf("retired permission admitted a reader: %v", err)
	}
}

func TestBeforeCommitRejectsStreamingAndCompletionRequests(t *testing.T) {
	for name, request := range map[string]xhandler.ComponentRequest{
		"stream":     {Rows: &xhandler.RowConsumer{}},
		"completion": {Completion: func(xhandler.Outcome) { t.Error("completion callback executed") }},
	} {
		t.Run(name, func(t *testing.T) {
			permit := &beforeCommitFrame{active: true}
			_, err := permit.Invoke(context.Background(), request)
			if !errors.Is(err, ErrBeforeCommitReadOnly) {
				t.Fatalf("request admitted: %v", err)
			}
			if !errors.Is(permit.failure, ErrBeforeCommitReadOnly) {
				t.Fatal("violation was not retained")
			}
		})
	}
}
