package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/sqlx/testutil/sqlfault"
	xh "github.com/viant/xdatly/handler"
)

// These are real native journals/transactions. The first unit's synchronous
// commit observer makes an ignored public drain attempt against the second.
// Only the genuine retained engine handle can then clean up denied completion.
// Caller-owned variants are unbuffered captured-guard controls because buffered
// cross-owner caller transactions are explicitly unsupported; their buffered
// rejection counterparts are TestOrderedJournalRejectsCallerOwnedCombinations.
func TestNativeAdmission49TwoUnitCleanup(t *testing.T) {
	for _, viaEngine := range []bool{false, true} {
		for _, caller := range []bool{false, true} {
			t.Run(fmt.Sprintf("engine=%t/callerSecond=%t", viaEngine, caller), func(t *testing.T) {
				ctx := t.Context()
				first, second := sqlite.New(t), sqlite.New(t)
				for _, h := range []*sqlite.Harness{first, second} {
					if e := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(NEW.id);END"); e != nil {
						t.Fatal(e)
					}
				}
				var prepares atomic.Int32
				secondDB := second.FaultDB(t, func(_ context.Context, c sqlfault.Call) error {
					if c.Phase == "prepare" && strings.HasPrefix(c.SQL, "INSERT INTO records") {
						prepares.Add(1)
					}
					return nil
				})
				var callerTx *sql.Tx
				if caller {
					var e error
					callerTx, e = secondDB.BeginTx(ctx, nil)
					if e != nil {
						t.Fatal(e)
					}
					t.Cleanup(func() { _ = callerTx.Rollback() })
					if _, e = callerTx.ExecContext(ctx, "INSERT INTO records VALUES(99)"); e != nil {
						t.Fatal(e)
					}
				}
				var root, later *dataScope
				var prepared *sql.Tx
				var ignored error
				var callbacks int
				var preparedRows, preparesAtCommit int
				firstSource := dml.Source{DB: first.DB, OnCommit: func(callbackContext context.Context) {
					callbacks++
					if root == nil || later == nil {
						t.Error("actual units missing at commit")
						return
					}
					native := later.data.(*dml.Data)
					_, prepared = native.InvocationTransaction()
					if prepared == nil {
						t.Error("second unit was not prepared before first commit")
						return
					}
					preparedRows = admission49Count(t, prepared)
					preparesAtCommit = int(prepares.Load())
					// Deliberately ignored. This must latch against the canonical issuer,
					// but must not seal, drain, complete or roll back either native unit.
					ignored = native.Flush(callbackContext, "")
					if !errors.Is(ignored, drainowner.ErrDrain) {
						t.Errorf("public attempt=%v", ignored)
					}
					if after := admission49Count(t, prepared); after != preparedRows {
						t.Errorf("public attempt changed rows %d -> %d", preparedRows, after)
					}
				}}
				secondSource := dml.Source{DB: secondDB, Tx: callerTx}
				queue := func(ctx context.Context, in rh.Invocation) (any, error) {
					root = mainScope(ctx)
					if caller {
						if e := root.registerExecutionGuard(ctx, func(context.Context) error { return nil }); e != nil {
							return nil, e
						}
					}
					cap, found, e := in.Binder.Lookup(ctx, xh.DataKey)
					if e != nil || !found {
						return nil, fmt.Errorf("root data found=%v: %w", found, e)
					}
					if e = cap.(xh.Data).Execute("INSERT INTO records VALUES(1)"); e != nil {
						return nil, e
					}
					relation := ComponentBufferedImperative
					if caller {
						relation = ComponentBinding
					}
					_, e = New().Execute(PrepareComponent(ctx, relation, ""), Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), DataSource: secondSource, Handler: rh.HandlerFunc(func(childContext context.Context, child rh.Invocation) (any, error) {
						value, ok, err := child.Binder.Lookup(childContext, xh.DataKey)
						if err != nil || !ok {
							return nil, fmt.Errorf("child data found=%v: %w", ok, err)
						}
						later = mainScope(childContext).unit
						return nil, value.(xh.Data).Execute("INSERT INTO records VALUES(2)")
					})})
					return "successful handler", e
				}
				var err error
				if viaEngine {
					result, e := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), BufferedComponentCalls: !caller, DataSource: firstSource, Handler: rh.HandlerFunc(queue)})
					err = e
					if result != nil {
						t.Errorf("completion failure published success=%v", result)
					}
				} else {
					// Existing native dataScope API exercises the same genuine attachment,
					// unit preflight and completion path without a synthesized activity.
					root, _ = invocationDataScope(ctx, firstSource)
					var enrollErr error
					if caller {
						enrollErr = root.registerExecutionGuard(ctx, func(context.Context) error { return nil })
					} else {
						enrollErr = root.enrollBufferedScope()
					}
					if enrollErr != nil {
						t.Fatal(enrollErr)
					}
					data, e := root.resolve(ctx)
					if e != nil {
						t.Fatal(e)
					}
					if e = data.Execute("INSERT INTO records VALUES(1)"); e != nil {
						t.Fatal(e)
					}
					child, _ := invocationDataScope(withDataScope(ctx, root), secondSource)
					childData, e := child.resolve(ctx)
					if e != nil {
						t.Fatal(e)
					}
					later = child.unit
					if e = childData.Execute("INSERT INTO records VALUES(2)"); e != nil {
						t.Fatal(e)
					}
					if sealer, ok := childData.(interface{ SealComponent() }); ok {
						sealer.SealComponent()
					}
					err = root.complete(ctx, nil)
				}
				if !errors.Is(err, drainowner.ErrDrain) || callbacks != 1 || !errors.Is(ignored, drainowner.ErrDrain) {
					t.Fatalf("root=%v observer=%d ignored=%v", err, callbacks, ignored)
				}
				if root == nil || later == nil || prepared == nil {
					t.Fatal("missing genuine prepared second unit")
				}
				wantPrepared := 1
				if caller {
					wantPrepared = 2
				}
				if preparedRows != wantPrepared {
					t.Fatalf("prepared second rows=%d want=%d", preparedRows, wantPrepared)
				}
				if int(prepares.Load()) != preparesAtCommit {
					t.Fatalf("denied completion reexecuted SQL: prepare %d -> %d", preparesAtCommit, prepares.Load())
				}
				firstNative, secondNative := root.data.(*dml.Data), later.data.(*dml.Data)
				if out := firstNative.TransactionOutcome(); out.State != xh.TransactionCommitted || out.Error != nil {
					t.Fatalf("first actual outcome rewritten=%+v", out)
				}
				if root.completionErr != nil {
					t.Fatalf("first unit completion rewritten=%v", root.completionErr)
				}
				if !errors.Is(later.completionErr, drainowner.ErrDrain) {
					t.Fatalf("second completion missing genuine denial=%v", later.completionErr)
				}
				if n := admission49Count(t, first.DB); n != 1 {
					t.Fatalf("first committed physical rows=%d", n)
				}
				if caller {
					if out := secondNative.TransactionOutcome(); out.State != xh.TransactionCallerPending {
						t.Fatalf("caller outcome=%+v", out)
					}
					if _, e := prepared.ExecContext(ctx, "INSERT INTO records VALUES(101)"); e != nil {
						t.Fatalf("caller TX unusable after framework abort: %v", e)
					}
					if n := admission49Count(t, prepared); n != 3 {
						t.Fatalf("caller prior+prepared+new rows=%d", n)
					}
					if e := callerTx.Rollback(); e != nil {
						t.Fatal(e)
					}
				} else {
					if out := secondNative.TransactionOutcome(); out.State != xh.TransactionRolledBack {
						t.Fatalf("denied completion left prepared TX=%+v", out)
					}
					if _, e := prepared.ExecContext(ctx, "INSERT INTO records VALUES(101)"); !errors.Is(e, sql.ErrTxDone) {
						t.Fatalf("prepared transaction still live=%v", e)
					}
				}
				if n := admission49Count(t, second.DB); n != 0 {
					t.Fatalf("second physical rows escaped=%d", n)
				}
				t.Logf("REAL_TWO_UNIT_DENIAL engine=%t caller=%t preparedRows=%d first=%s second=%s nativeInsertPrepares=%d ignored=%v", viaEngine, caller, preparedRows, firstNative.TransactionOutcome().State, secondNative.TransactionOutcome().State, prepares.Load(), ignored)
			})
		}
	}
}

func admission49Count(t *testing.T, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) int {
	t.Helper()
	var n int
	if e := q.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM audit").Scan(&n); e != nil {
		t.Fatal(e)
	}
	return n
}

// The native commit actually succeeds, then the test driver reports an
// uncertain acknowledgment. Engine must preserve commit_unknown, not infer an
// admission denial and attempt abort/commit again. This is real SQLite SQL/TX.
type admission49Connector struct {
	base               driver.Driver
	dsn                string
	commits, rollbacks *atomic.Int32
	cause              error
}

func (c *admission49Connector) Driver() driver.Driver { return c.base }
func (c *admission49Connector) Connect(context.Context) (driver.Conn, error) {
	v, e := c.base.Open(c.dsn)
	if e != nil {
		return nil, e
	}
	return &admission49Conn{Conn: v, c: c}, nil
}

type admission49Conn struct {
	driver.Conn
	c *admission49Connector
}

func (c *admission49Conn) Begin() (driver.Tx, error) {
	v, e := c.Conn.Begin()
	if e != nil {
		return nil, e
	}
	return &admission49Tx{Tx: v, c: c.c}, nil
}
func (c *admission49Conn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	if b, ok := c.Conn.(driver.ConnBeginTx); ok {
		v, e := b.BeginTx(ctx, o)
		if e != nil {
			return nil, e
		}
		return &admission49Tx{Tx: v, c: c.c}, nil
	}
	return c.Begin()
}

type admission49Tx struct {
	driver.Tx
	c *admission49Connector
}

func (t *admission49Tx) Commit() error {
	t.c.commits.Add(1)
	if e := t.Tx.Commit(); e != nil {
		return e
	}
	return t.c.cause
}
func (t *admission49Tx) Rollback() error { t.c.rollbacks.Add(1); return t.Tx.Rollback() }

func TestNativeAdmission49CommitUnknownNoRetry(t *testing.T) {
	h := sqlite.New(t)
	ctx := t.Context()
	dsn := filepath.Join(h.TempDir, "uncertain.db")
	var commits, rollbacks atomic.Int32
	cause := errors.New("real commit acknowledgment unknown")
	db := sql.OpenDB(&admission49Connector{base: h.DB.Driver(), dsn: dsn, commits: &commits, rollbacks: &rollbacks, cause: cause})
	t.Cleanup(func() { _ = db.Close() })
	if _, e := db.ExecContext(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)"); e != nil {
		t.Fatal(e)
	}
	var unit *dataScope
	result, e := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), BufferedComponentCalls: true, DataSource: dml.Source{DB: db}, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
		unit = mainScope(ctx)
		v, ok, e := in.Binder.Lookup(ctx, xh.DataKey)
		if e != nil || !ok {
			return nil, e
		}
		return "result", v.(xh.Data).Execute("INSERT INTO records VALUES(1)")
	})})
	if result != nil || !errors.Is(e, cause) || drainowner.IsAdmissionDenied(e) {
		t.Fatalf("actual commit misclassified result=%v error=%v", result, e)
	}
	if commits.Load() != 1 || rollbacks.Load() != 0 {
		t.Fatalf("actual commit retried/aborted commits=%d rollbacks=%d", commits.Load(), rollbacks.Load())
	}
	out := unit.data.(*dml.Data).TransactionOutcome()
	if out.State != xh.TransactionCommitUnknown || !errors.Is(out.Error, cause) {
		t.Fatalf("uncertain outcome rewritten=%+v", out)
	}
	var count int
	if e := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); e != nil || count != 1 {
		t.Fatalf("true physical commit rows=%d error=%v", count, e)
	}
	// A repeat of the genuine engine completion seam must be rejected before any
	// new native transaction operation, without rewriting the first outcome.
	if e = unit.completeUnit(ctx, nil); e == nil || drainowner.IsAdmissionDenied(e) {
		t.Fatalf("retired completion incorrectly classified=%v", e)
	}
	if commits.Load() != 1 || rollbacks.Load() != 0 || unit.data.(*dml.Data).TransactionOutcome().State != xh.TransactionCommitUnknown {
		t.Fatal("repeat changed uncertain native outcome")
	}
	t.Logf("REAL_COMMIT_UNKNOWN rows=%d commits=%d rollbacks=%d outcome=%s", count, commits.Load(), rollbacks.Load(), out.State)
}

// There is no subsequent native Completion to detect a failure published by
// the last (or sole) synchronous commit observer. The root must collect it after
// the loop while retaining every real committed first outcome.
func TestNativeAdmission49LastOrOnlyCommitObserver(t *testing.T) {
	for _, only := range []bool{false, true} {
		t.Run(fmt.Sprintf("only=%t", only), func(t *testing.T) {
			ctx := t.Context()
			first, last := sqlite.New(t), sqlite.New(t)
			for _, h := range []*sqlite.Harness{first, last} {
				if e := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(NEW.id);END"); e != nil {
					t.Fatal(e)
				}
			}
			var root, observerUnit *dataScope
			var ignored error
			callbacks := 0
			observerSource := dml.Source{DB: last.DB, OnCommit: func(ctx context.Context) {
				callbacks++
				if observerUnit == nil {
					t.Error("missing real observer unit")
					return
				}
				ignored = observerUnit.data.(*dml.Data).Flush(ctx, "")
			}}
			source := dml.Source{DB: first.DB}
			if only {
				source = observerSource
			}
			result, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), BufferedComponentCalls: true, DataSource: source, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
				root = mainScope(ctx)
				v, ok, e := in.Binder.Lookup(ctx, xh.DataKey)
				if e != nil || !ok {
					return nil, e
				}
				if e = v.(xh.Data).Execute("INSERT INTO records VALUES(1)"); e != nil {
					return nil, e
				}
				if only {
					observerUnit = root
					return "success", nil
				}
				_, e = New().Execute(PrepareComponent(ctx, ComponentBufferedImperative, ""), Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), DataSource: observerSource, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
					v, ok, e := in.Binder.Lookup(ctx, xh.DataKey)
					if e != nil || !ok {
						return nil, e
					}
					observerUnit = mainScope(ctx).unit
					return nil, v.(xh.Data).Execute("INSERT INTO records VALUES(2)")
				})})
				return "success", e
			})})
			if result != nil || !errors.Is(err, drainowner.ErrDrain) || !errors.Is(ignored, drainowner.ErrDrain) || callbacks != 1 {
				t.Fatalf("ignored last failure lost result=%v root=%v ignored=%v callbacks=%d", result, err, ignored, callbacks)
			}
			for _, unit := range []*dataScope{root, observerUnit} {
				if unit.completionErr != nil {
					t.Fatalf("committed unit completion rewritten=%v", unit.completionErr)
				}
				native := unit.data.(*dml.Data)
				out := native.TransactionOutcome()
				if out.State != xh.TransactionCommitted || out.Error != nil {
					t.Fatalf("actual committed outcome rewritten=%+v", out)
				}
				_, tx := native.InvocationTransaction()
				if tx == nil {
					t.Fatal("missing actual prior transaction")
				}
				if _, e := tx.ExecContext(ctx, "INSERT INTO records VALUES(101)"); !errors.Is(e, sql.ErrTxDone) {
					t.Fatalf("committed TX reopened=%v", e)
				}
			}
			if only {
				if n := admission49Count(t, last.DB); n != 1 {
					t.Fatalf("sole committed physical rows=%d", n)
				}
			} else {
				for _, db := range []*sql.DB{first.DB, last.DB} {
					if n := admission49Count(t, db); n != 1 {
						t.Fatalf("two committed physical rows=%d", n)
					}
				}
			}
			if e := observerUnit.completeUnit(ctx, nil); e == nil || drainowner.IsAdmissionDenied(e) {
				t.Fatalf("committed repeat gained cleanup classification=%v", e)
			}
			if callbacks != 1 || observerUnit.data.(*dml.Data).TransactionOutcome().State != xh.TransactionCommitted {
				t.Fatal("last callback or commit retried")
			}
			t.Logf("REAL_LAST_OBSERVER_FAILURE only=%t callbacks=%d retainedCommitted=true root=%v", only, callbacks, err)
		})
	}
}

// A malformed handle is not a same-call admission denial. The engine must not
// convert that failure into automatic abort authority or native effects.
func TestNativeAdmission49MalformedHandleNoAutomaticCleanup(t *testing.T) {
	ctx := t.Context()
	h := sqlite.New(t)
	if e := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(NEW.id);END"); e != nil {
		t.Fatal(e)
	}
	unit, _ := invocationDataScope(ctx, dml.Source{DB: h.DB})
	if e := unit.enrollBufferedScope(); e != nil {
		t.Fatal(e)
	}
	data, e := unit.resolve(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if e = data.Execute("INSERT INTO records VALUES(1)"); e != nil {
		t.Fatal(e)
	}
	if _, e = unit.beginProtectedCompletion(ctx); e != nil {
		t.Fatal(e)
	}
	if e = unit.prepareUnit(ctx); e != nil {
		t.Fatal(e)
	}
	native := unit.data.(*dml.Data)
	_, tx := native.InvocationTransaction()
	if tx == nil || admission49Count(t, tx) != 1 {
		t.Fatal("genuine transaction was not prepared")
	}
	genuine := unit.nativeHandle
	unit.nativeHandle = drainowner.Handle{} // malformed control, never an authority grant
	e = unit.completeUnit(ctx, nil)
	if !errors.Is(e, drainowner.ErrAttachment) || drainowner.IsAdmissionDenied(e) {
		t.Fatalf("malformed handle acquired denial marker=%v", e)
	}
	if native.TransactionOutcome().State != xh.TransactionUnknown || admission49Count(t, tx) != 1 {
		t.Fatal("malformed handle automatically cleaned native transaction")
	}
	// Restore only the genuine attachment returned by the real native constructor;
	// this explicit test cleanup is not an engine retry after malformed admission.
	unit.nativeHandle = genuine
	cleanup := errors.New("explicit malformed-witness cleanup")
	if e = unit.completeUnit(ctx, cleanup); !errors.Is(e, cleanup) {
		t.Fatalf("genuine abort cause lost=%v", e)
	}
	if native.TransactionOutcome().State != xh.TransactionRolledBack || admission49Count(t, h.DB) != 0 {
		t.Fatal("genuine explicit cleanup did not roll back")
	}
	if _, e = tx.ExecContext(ctx, "INSERT INTO records VALUES(101)"); !errors.Is(e, sql.ErrTxDone) {
		t.Fatalf("prepared TX remains dangling=%v", e)
	}
	t.Log("REAL_MALFORMED_NO_AUTO_CLEANUP native prepared state retained until explicit genuine abort")
}
