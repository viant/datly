package engine

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
	"github.com/viant/xdatly/mbus"
)

func TestOutcomeFinalizerRejectsUnfinishedChildBeforeCommitSQLite(t *testing.T) {
	for _, stage := range []string{"before lookup", "queued", "flushed"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER)"); err != nil {
				t.Fatal(err)
			}
			ready, release, done := make(chan error, 1), make(chan struct{}), make(chan error, 1)
			input := testRouteInput(t, reflect.TypeOf(struct{}{}))
			child := &outcomeAwareHandler{execute: func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
				if stage == "before lookup" {
					ready <- nil
					<-release
				}
				value, _, err := invocation.Binder.Lookup(ctx, xhandler.DataKey)
				if err == nil {
					data := value.(xhandler.Data)
					err = data.Execute("INSERT INTO audit VALUES(1)")
					if err == nil && stage == "flushed" {
						err = data.Flush(ctx, "")
					}
				}
				if stage != "before lookup" {
					ready <- err
					<-release
				}
				return nil, err
			}, finalize: func(context.Context, rhandler.Invocation, any, xhandler.Outcome) error {
				return errors.New("unfinished child must not finalize")
			}}
			var report xhandler.Outcome
			root := &outcomeAwareHandler{execute: func(ctx context.Context, _ rhandler.Invocation) (any, error) {
				go func() {
					_, err := New().Execute(PrepareComponent(ctx, ComponentImperative, ""), Request{Input: input, DataSource: sqldml.Source{DB: h.DB}, Handler: child})
					done <- err
				}()
				return nil, <-ready
			}, finalize: func(_ context.Context, _ rhandler.Invocation, _ any, outcome xhandler.Outcome) error {
				report = outcome
				return nil
			}}
			_, err := New().Execute(ctx, Request{Input: input, Handler: root})
			close(release)
			childErr := <-done
			if stage == "before lookup" && (childErr == nil || !strings.Contains(childErr.Error(), "completion has already started")) {
				t.Fatalf("late child lookup=%v", childErr)
			}
			if err == nil || !strings.Contains(err.Error(), "before root completion") || report.CommitConfirmed() || report.Error == nil {
				t.Fatalf("error=%v outcome=%+v", err, report)
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{0}})
		})
	}
}

type outcomeAwareHandler struct {
	execute  rhandler.HandlerFunc
	finalize func(context.Context, rhandler.Invocation, any, xhandler.Outcome) error
}

func (h *outcomeAwareHandler) Execute(ctx context.Context, invocation rhandler.Invocation) (any, error) {
	return h.execute(ctx, invocation)
}
func (h *outcomeAwareHandler) FinalizeOutcome(ctx context.Context, invocation rhandler.Invocation, result any, outcome xhandler.Outcome) error {
	return h.finalize(ctx, invocation, result, outcome)
}

func TestOutcomeFinalizerPublishesOnlyConfirmedCommitSQLite(t *testing.T) {
	for _, test := range []struct {
		name, statement                    string
		handlerFail, external, publishFail bool
		wantState                          xhandler.TransactionState
		wantRows, pushes                   int
	}{
		{"success", "INSERT INTO audit VALUES(1)", false, false, false, xhandler.TransactionCommitted, 1, 1},
		{"handler failure", "INSERT INTO audit VALUES(1)", true, false, false, xhandler.TransactionRolledBack, 0, 0},
		{"prepare failure", "INSERT INTO audit VALUES(-1)", false, false, false, xhandler.TransactionRolledBack, 0, 0},
		{"commit unknown", "INSERT INTO child VALUES(99)", false, false, false, xhandler.TransactionCommitUnknown, 0, 0},
		{"caller pending", "INSERT INTO audit VALUES(1)", false, true, false, xhandler.TransactionCallerPending, 0, 0},
		{"no transaction", "", false, false, false, xhandler.TransactionNone, 0, 0},
		{"publication failure", "INSERT INTO audit VALUES(1)", false, false, true, xhandler.TransactionCommitted, 1, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := xmcp.WithContext(context.Background(), engineMCPContext{})
			h := sqlite.New(t)
			h.DB.SetMaxOpenConns(1)
			if err := h.ExecStatements(ctx, "PRAGMA foreign_keys=ON", "CREATE TABLE audit(id INTEGER CHECK(id>0))", "CREATE TABLE parent(id INTEGER PRIMARY KEY)", "CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)"); err != nil {
				t.Fatal(err)
			}
			var tx *sql.Tx
			var err error
			if test.external {
				tx, err = h.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
			}
			bus := &hookBus{}
			output := &mcpFinalizingOutput{}
			input := &mcpEngineInput{}
			calls := 0
			expected := errors.New("publication failed")
			handlerError := errors.New("handler failed")
			adapter := &outcomeAwareHandler{
				execute: func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
					if test.statement != "" {
						value, _, err := invocation.Binder.Lookup(ctx, xhandler.DataKey)
						if err != nil {
							return nil, err
						}
						data := value.(xhandler.Data)
						if err := data.Execute(test.statement); err != nil {
							return nil, err
						}
						if test.handlerFail {
							if err := data.Flush(ctx, ""); err != nil {
								return nil, err
							}
						}
					}
					if test.handlerFail {
						return output, handlerError
					}
					return output, nil
				},
				finalize: func(ctx context.Context, invocation rhandler.Invocation, result any, outcome xhandler.Outcome) error {
					calls++
					if result != output {
						t.Fatal("finalizer lost original result")
					}
					if outcome.State() != test.wantState {
						t.Fatalf("outcome=%+v state=%s want=%s", outcome, outcome.State(), test.wantState)
					}
					if outcome.CommitConfirmed() {
						h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{1}})
						service, _, err := invocation.Binder.Lookup(ctx, xhandler.MessageBusKey)
						if err != nil {
							return err
						}
						messageBus := service.(mbus.Service)
						if _, err := messageBus.Push(ctx, messageBus.Message("changed", nil)); err != nil {
							return err
						}
						if test.publishFail {
							return expected
						}
					}
					return nil
				},
			}
			_, err = New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(mcpEngineInput{})), BoundInput: input, DataSource: sqldml.Source{DB: h.DB, Tx: tx}, Capabilities: rhandler.InvocationCapabilities{MessageBus: bus}, Handler: adapter})
			if calls != 1 || bus.pushes != test.pushes || len(output.order) != 0 || !reflect.DeepEqual(input.order, []string{"init", "mcp"}) {
				t.Fatalf("calls=%d pushes=%d old hooks=%v init=%v", calls, bus.pushes, output.order, input.order)
			}
			if test.publishFail {
				var finalizeErr *FinalizationError
				if !errors.Is(err, expected) || !errors.As(err, &finalizeErr) || !finalizeErr.Outcome.CommitConfirmed() {
					t.Fatalf("postcommit classification=%v", err)
				}
			} else if (err != nil) != (test.handlerFail || test.wantState == xhandler.TransactionRolledBack || test.wantState == xhandler.TransactionCommitUnknown) {
				t.Fatalf("error=%v", err)
			}
			if tx != nil {
				if err := tx.Rollback(); err != nil {
					t.Fatalf("caller Tx completed prematurely: %v", err)
				}
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{test.wantRows}})
		})
	}
}

func TestOutcomeFinalizerWaitsForNeutralGenericRootSQLite(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "rollback"}[fail], func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER)"); err != nil {
				t.Fatal(err)
			}
			var order []string
			var reports []xhandler.Outcome
			child := &outcomeAwareHandler{execute: func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
				value, _, err := invocation.Binder.Lookup(ctx, xhandler.DataKey)
				if err != nil {
					return nil, err
				}
				data := value.(xhandler.Data)
				if err := data.Execute("INSERT INTO audit VALUES(1)"); err != nil {
					return nil, err
				}
				return 1, data.Flush(ctx, "")
			}, finalize: func(_ context.Context, _ rhandler.Invocation, _ any, outcome xhandler.Outcome) error {
				order = append(order, "child")
				reports = append(reports, outcome)
				return nil
			}}
			failure := errors.New("root failed")
			root := &outcomeAwareHandler{execute: func(ctx context.Context, _ rhandler.Invocation) (any, error) {
				result, err := New().Execute(PrepareComponent(ctx, ComponentImperative, ""), Request{Input: testRouteInput(t, reflect.TypeOf(struct{}{})), DataSource: sqldml.Source{DB: h.DB}, Handler: child})
				if err != nil {
					return nil, err
				}
				if len(order) != 0 {
					t.Fatal("child finalized before owning root")
				}
				if fail {
					return result, failure
				}
				return result, nil
			}, finalize: func(_ context.Context, _ rhandler.Invocation, _ any, outcome xhandler.Outcome) error {
				order = append(order, "root")
				reports = append(reports, outcome)
				return nil
			}}
			_, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(struct{}{})), Handler: root})
			if (err != nil) != fail {
				t.Fatalf("error=%v", err)
			}
			if !reflect.DeepEqual(order, []string{"child", "root"}) || len(reports) != 2 {
				t.Fatalf("finalizer order=%v", order)
			}
			for _, report := range reports {
				if report.CommitConfirmed() == fail {
					t.Fatalf("report=%+v", report)
				}
			}
			count := 1
			if fail {
				count = 0
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{count}})
		})
	}
}

func TestOutcomeFinalizerRunsOnInitializationFailure(t *testing.T) {
	for _, test := range []struct {
		name  string
		input any
		cause error
	}{{"ordinary init", &failingEngineInput{}, errEngineInputInit}, {"MCP init", &failingMCPInput{}, errMCPInputInit}} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			adapter := &outcomeAwareHandler{execute: func(context.Context, rhandler.Invocation) (any, error) {
				t.Fatal("failed input executed")
				return nil, nil
			}, finalize: func(_ context.Context, invocation rhandler.Invocation, result any, outcome xhandler.Outcome) error {
				calls++
				if invocation.Input != test.input || result != nil || !errors.Is(outcome.Error, test.cause) || outcome.CommitConfirmed() {
					t.Fatalf("early outcome=%+v", outcome)
				}
				return nil
			}}
			_, err := New().Execute(xmcp.WithContext(context.Background(), engineMCPContext{}), Request{Input: testRouteInput(t, reflect.TypeOf(test.input).Elem()), BoundInput: test.input, Handler: adapter})
			if !errors.Is(err, test.cause) || calls != 1 {
				t.Fatalf("calls=%d error=%v", calls, err)
			}
		})
	}
}

func TestOutcomeFinalizerRunsOnBindingFailure(t *testing.T) {
	type input struct{ ID int }
	calls := 0
	adapter := &outcomeAwareHandler{execute: func(context.Context, rhandler.Invocation) (any, error) {
		t.Fatal("binding failure executed")
		return nil, nil
	}, finalize: func(_ context.Context, invocation rhandler.Invocation, result any, outcome xhandler.Outcome) error {
		calls++
		if outcome.Error == nil || outcome.State() != xhandler.TransactionNone || result != nil || invocation.Input == nil {
			t.Fatalf("binding outcome=%+v", outcome)
		}
		return nil
	}}
	_, err := New().Execute(context.Background(), Request{Input: testRouteInput(t, reflect.TypeOf(input{}), bindly.BindingSpec{Path: "ID", Location: bindstate.Location{Kind: "query", In: "id"}}), Scope: testharness.Request{}.WithQuery(url.Values{"id": {"bad"}}), Handler: adapter})
	if err == nil || calls != 1 {
		t.Fatalf("calls=%d error=%v", calls, err)
	}
}

func TestOrdinarySourceLessParentKeepsChosenChildOwnershipSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	calls := 0
	child := &outcomeAwareHandler{execute: func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
		value, _, err := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
		if err != nil {
			return nil, err
		}
		return nil, value.(xhandler.DML).Execute("INSERT INTO audit VALUES(1)")
	}, finalize: func(_ context.Context, _ rhandler.Invocation, _ any, outcome xhandler.Outcome) error {
		calls++
		if !outcome.CommitConfirmed() {
			t.Fatalf("child-owned outcome=%+v", outcome)
		}
		return nil
	}}
	parent := rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
		_, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(struct{}{})), DataSource: sqldml.Source{DB: h.DB}, Handler: child})
		if calls != 1 {
			t.Fatal("ordinary source-less parent acquired a new coordinator")
		}
		return nil, err
	})
	if _, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(struct{}{})), Handler: parent}); err != nil {
		t.Fatal(err)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{1}})
}
