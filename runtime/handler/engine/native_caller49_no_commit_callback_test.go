package engine

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/sql/dml"
	xh "github.com/viant/xdatly/handler"
)

// Caller-owned transactions have no native OnCommit callback. This is a real
// pending/usable/rollback control, not a synthesized caller callback probe.
// The two-owner variant is now an unbuffered control; its former buffered
// compatibility is deliberately excluded and covered by the rejection
// counterpart TestOrderedJournalRejectsCallerOwnedCombinations.
func TestNativeCaller49PendingNeverInvokesOwnedCommitObserver(t *testing.T) {
	for _, only := range []bool{true, false} {
		t.Run(fmt.Sprintf("only=%t", only), func(t *testing.T) {
			ctx := t.Context()
			first, last := sqlite.New(t), sqlite.New(t)
			for _, db := range []*sql.DB{first.DB, last.DB} {
				for _, q := range []string{"CREATE TABLE records(id INTEGER PRIMARY KEY)", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(NEW.id);END"} {
					if _, e := db.ExecContext(ctx, q); e != nil {
						t.Fatal(e)
					}
				}
			}
			firstTX, e := first.DB.BeginTx(ctx, nil)
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { firstTX.Rollback() })
			lastTX, e := last.DB.BeginTx(ctx, nil)
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { lastTX.Rollback() })
			for _, tx := range []*sql.Tx{firstTX, lastTX} {
				if _, e = tx.ExecContext(ctx, "INSERT INTO records VALUES(99)"); e != nil {
					t.Fatal(e)
				}
			}
			callbackCount := 0
			observerSource := dml.Source{DB: last.DB, Tx: lastTX, OnCommit: func(context.Context) { callbackCount++ }}
			source := dml.Source{DB: first.DB, Tx: firstTX, OnCommit: func(context.Context) { callbackCount++ }}
			if only {
				source = observerSource
			}
			var root, observer *dataScope
			result, e := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), BufferedComponentCalls: only, DataSource: source, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
				v, ok, e := in.Binder.Lookup(ctx, xh.DataKey)
				if e != nil || !ok {
					return nil, e
				}
				root = mainScope(ctx)
				if e = v.(xh.Data).Execute("INSERT INTO records VALUES(1)"); e != nil {
					return nil, e
				}
				if only {
					observer = root
					return "success", nil
				}
				_, e = New().Execute(PrepareComponent(ctx, ComponentBinding, ""), Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), DataSource: observerSource, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
					v, ok, e := in.Binder.Lookup(ctx, xh.DataKey)
					if e != nil || !ok {
						return nil, e
					}
					observer = mainScope(ctx).unit
					return nil, v.(xh.Data).Execute("INSERT INTO records VALUES(1)")
				})})
				return "success", e
			})})
			if e != nil || result != "success" || callbackCount != 0 {
				t.Fatalf("caller completion result=%v error=%v ownedCommitCallbacks=%d", result, e, callbackCount)
			}
			units := []*dataScope{observer}
			if !only {
				units = append(units, root)
			}
			for _, unit := range units {
				actual := unit.data.(*dml.Data)
				if actual.TransactionOutcome().State != xh.TransactionCallerPending {
					t.Fatalf("caller outcome=%+v", actual.TransactionOutcome())
				}
				_, tx := actual.InvocationTransaction()
				if ids := terminal49IDs(t, tx); !reflect.DeepEqual(ids, []int{1, 99}) {
					t.Fatalf("caller physical rows=%v", ids)
				}
				if _, e = tx.ExecContext(ctx, "INSERT INTO records VALUES(101)"); e != nil {
					t.Fatalf("caller prior TX unusable: %v", e)
				}
			}
			if e = firstTX.Rollback(); e != nil {
				t.Fatal(e)
			}
			if e = lastTX.Rollback(); e != nil {
				t.Fatal(e)
			}
			for _, db := range []*sql.DB{first.DB, last.DB} {
				var rows, audit int
				if e = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&rows); e != nil {
					t.Fatal(e)
				}
				if e = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&audit); e != nil {
					t.Fatal(e)
				}
				if rows != 0 || audit != 0 {
					t.Fatalf("caller rollback changed physical rows=%d audit=%d", rows, audit)
				}
			}
			t.Logf("GENUINE_CALLER_PENDING only=%t ownedCommitCallbacks=%d callerUsable=true physicalRollbackRows=0 audit=0", only, callbackCount)
		})
	}
}
