package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/sql/dml"
	xh "github.com/viant/xdatly/handler"
)

// Receiver is the actual source-returned owner/view. This source records the
// genuine Engine-selected scope, without intercepting or forging attachment.
type firstOpen49Source struct {
	data  xh.Data
	err   error
	scope *dataScope
	db    *sql.DB
}

func (s *firstOpen49Source) Open(ctx context.Context) (xh.Data, error) {
	s.scope = mainScope(ctx)
	return s.data, s.err
}
func (s *firstOpen49Source) InvocationKey() any { return s.db }

type firstOpen49Wrapped struct{ *dml.Data }
type firstOpen49Handler struct{ entered bool }

func (*firstOpen49Handler) RequiresPreBindingTransaction() bool { return true }
func (h *firstOpen49Handler) Execute(context.Context, rh.Invocation) (any, error) {
	h.entered = true
	return "unreachable business output", nil
}

// All foreign owners below have actual prior native ownership and pending
// original work. Copied/wrapped/borrowed views cannot transfer that ownership.
func TestNativeFirstOpen49BufferedForeignOwnerEngine(t *testing.T) {
	for _, mode := range []string{"open-error", "public-begin", "foreign-issuer", "copied", "wrapped", "borrowed"} {
		for _, caller := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/caller=%t", mode, caller), func(t *testing.T) {
				ctx := context.Background()
				h := sqlite.New(t)
				if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(new.id);END"); err != nil {
					t.Fatal(err)
				}
				var tx *sql.Tx
				var options []dml.Option
				if caller {
					var err error
					tx, err = h.DB.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = tx.Rollback() })
					options = append(options, dml.WithTx(tx))
					if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(99)"); err != nil {
						t.Fatal(err)
					}
				}
				native := dml.NewData(h.DB, options...)
				var foreignIssuer *drainowner.Invocation
				var foreignHandle drainowner.Handle
				if mode == "foreign-issuer" {
					foreignIssuer = drainowner.NewInvocation()
					var err error
					foreignHandle, err = drainowner.Claim(native, foreignIssuer)
					if err != nil {
						t.Fatal(err)
					}
					if err = foreignHandle.Attach(native, foreignIssuer); err != nil {
						t.Fatal(err)
					}
				} else if err := native.BeginInvocation(); err != nil {
					t.Fatal(err)
				}
				if err := native.Start(ctx); err != nil {
					t.Fatal(err)
				}
				if !caller {
					if _, err := native.ExecContext(ctx, "INSERT INTO records VALUES(99)"); err != nil {
						t.Fatal(err)
					}
				}
				if err := native.Execute("INSERT INTO records VALUES(2)"); err != nil {
					t.Fatal(err)
				}
				before := native.TransactionOutcome()
				_, beforeTx := native.InvocationTransaction()
				sentinel := errors.New("source returns actual foreign native plus failure")
				source := &firstOpen49Source{data: native, db: h.DB}
				switch mode {
				case "open-error":
					source.err = sentinel
				case "wrapped":
					source.data = &firstOpen49Wrapped{native}
				case "borrowed":
					source.data = native.ComponentData(dml.ComponentImperative, "")
				case "copied":
					copied := reflect.New(reflect.TypeOf(native).Elem())
					copied.Elem().Set(reflect.ValueOf(native).Elem())
					source.data = copied.Interface().(*dml.Data)
				}
				handler := &firstOpen49Handler{}
				var report xh.Outcome
				notified := false
				value, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), DataSource: source, Handler: handler, BufferedComponentCalls: true, Completion: func(o xh.Outcome) { notified = true; report = o.Clone() }})
				if err == nil || value != nil {
					t.Errorf("DESIRED_FOREIGN_OWNER_GAP: root published success value=%v err=%v", value, err)
				}
				if mode == "open-error" && !errors.Is(err, sentinel) {
					t.Errorf("actual Open cause identity lost: %v", err)
				}
				if handler.entered {
					t.Error("failed firstOpen entered business handler")
				}
				if source.scope == nil || source.scope.err == nil || source.scope.nativeCleanup {
					t.Fatalf("failed native ownership misclassified scope=%v", source.scope)
				}
				if !notified || report.Error == nil || len(report.Transactions) != 1 || report.Transactions[0].State != xh.TransactionUnknown || report.CommitConfirmed() {
					t.Errorf("DESIRED_FOREIGN_OWNER_GAP: root borrowed native report notified=%t outcome=%+v", notified, report)
				}
				after := native.TransactionOutcome()
				_, afterTx := native.InvocationTransaction()
				if !reflect.DeepEqual(before, after) || beforeTx != afterTx {
					t.Errorf("DESIRED_FOREIGN_OWNER_GAP: failed root rewrote foreign outcome/transaction before=%+v after=%+v", before, after)
				}
				if foreignIssuer != nil && !foreignHandle.Attached(native, foreignIssuer) {
					t.Error("DESIRED_FOREIGN_OWNER_GAP: failed root retired original foreign attachment")
				}
				// Queue admission, causal flushing and final completion stay with the
				// original owner after the failing protected root returns.
				if err := native.Execute("INSERT INTO records VALUES(3)"); err != nil {
					t.Fatalf("DESIRED_FOREIGN_OWNER_GAP: foreign original queue closed: %v", err)
				}
				if mode == "borrowed" {
					source.data.(*dml.Data).SealComponent()
				}
				if err := native.Flush(ctx, ""); err != nil {
					t.Fatalf("DESIRED_FOREIGN_OWNER_GAP: foreign original flush denied: %v", err)
				}
				var pending int
				if err := beforeTx.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&pending); err != nil || pending != 3 {
					t.Fatalf("foreign work failed before own completion rows=%d err=%v", pending, err)
				}
				if err := native.Complete(ctx, nil); err != nil {
					t.Fatalf("DESIRED_FOREIGN_OWNER_GAP: foreign original completion denied: %v", err)
				}
				outcome := native.TransactionOutcome()
				want := xh.TransactionCommitted
				if caller {
					want = xh.TransactionCallerPending
				}
				if outcome.State != want {
					t.Errorf("foreign first actual completion=%s want=%s", outcome.State, want)
				}
				if caller {
					if _, err := tx.ExecContext(ctx, "INSERT INTO records VALUES(101)"); err != nil {
						t.Fatalf("DESIRED_FOREIGN_OWNER_GAP: caller prior TX completed: %v", err)
					}
					if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&pending); err != nil || pending != 4 {
						t.Fatalf("caller prior/queued/new work rows=%d err=%v", pending, err)
					}
					if err := tx.Rollback(); err != nil {
						t.Fatal(err)
					}
				}
				var committed int
				if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&committed); err != nil {
					t.Fatal(err)
				}
				expected := 3
				if caller {
					expected = 0
				}
				if committed != expected {
					t.Errorf("actual foreign physical state=%d want=%d", committed, expected)
				}
				t.Logf("REAL_BUFFERED_FOREIGN_FIRST_OPEN mode=%s caller=%t rootErr=%v rootState=%s nativeCleanup=%t foreignBefore=%s foreignAfterFailedRoot=%s ownFinalState=%s ownCommittedRows=%d", mode, caller, err, report.State(), source.scope.nativeCleanup, before.State, after.State, outcome.State, committed)
			})
		}
	}
}

// Fresh native source admission remains a separate positive control: the
// Engine legitimately owns this attachment and publishes its actual outcome.
type firstOpen49FreshHandler struct{ entered bool }

func (*firstOpen49FreshHandler) RequiresPreBindingTransaction() bool { return true }
func (h *firstOpen49FreshHandler) Execute(ctx context.Context, inv rh.Invocation) (any, error) {
	h.entered = true
	value, found, err := inv.Binder.Lookup(ctx, xh.DataKey)
	if err != nil || !found {
		return nil, fmt.Errorf("actual data binding found=%t: %w", found, err)
	}
	if err := value.(xh.Data).Execute("INSERT INTO records VALUES(2)"); err != nil {
		return nil, err
	}
	return "fresh native output", nil
}
func TestNativeFirstOpen49FreshBufferedOwnerControl(t *testing.T) {
	for _, caller := range []bool{false, true} {
		t.Run(fmt.Sprintf("caller=%t", caller), func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(new.id);END"); err != nil {
				t.Fatal(err)
			}
			var tx *sql.Tx
			var options []dml.Option
			if caller {
				var err error
				tx, err = h.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = tx.Rollback() })
				options = append(options, dml.WithTx(tx))
				if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(99)"); err != nil {
					t.Fatal(err)
				}
			}
			native := dml.NewData(h.DB, options...)
			source := &firstOpen49Source{data: native, db: h.DB}
			handler := &firstOpen49FreshHandler{}
			var report xh.Outcome
			notified := false
			value, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), DataSource: source, Handler: handler, BufferedComponentCalls: true, Completion: func(o xh.Outcome) { notified = true; report = o.Clone() }})
			if err != nil || value != "fresh native output" || !handler.entered {
				t.Fatalf("fresh valid owner rejected value=%v err=%v business=%t", value, err, handler.entered)
			}
			want := xh.TransactionCommitted
			if caller {
				want = xh.TransactionCallerPending
			}
			if source.scope == nil || source.scope.err != nil || !source.scope.nativeCleanup {
				t.Fatal("fresh genuine owner lost original attachment proof")
			}
			if !notified || report.Error != nil || len(report.Transactions) != 1 || report.Transactions[0].State != want || native.TransactionOutcome().State != want {
				t.Fatalf("fresh truthful ownership report=%+v native=%+v", report, native.TransactionOutcome())
			}
			var n int
			if caller {
				if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&n); err != nil || n != 2 {
					t.Fatalf("actual caller rows=%d err=%v", n, err)
				}
				if _, err := tx.ExecContext(ctx, "INSERT INTO records VALUES(101)"); err != nil {
					t.Fatal("fresh caller tx not usable", err)
				}
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
			}
			if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&n); err != nil {
				t.Fatal(err)
			}
			count := 1
			if caller {
				count = 0
			}
			if n != count {
				t.Fatalf("fresh actual physical outcome rows=%d want=%d", n, count)
			}
			t.Logf("REAL_FRESH_BUFFERED_OWNER caller=%t reportState=%s nativeCleanup=%t committedRows=%d", caller, report.State(), source.scope.nativeCleanup, n)
		})
	}
}
