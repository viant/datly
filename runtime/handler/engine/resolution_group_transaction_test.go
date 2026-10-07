package engine

import (
	"errors"
	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	"testing"
)

func TestResolutionGroupRealFailurePreservesCallerTransactionAndSkipsBuffer(t *testing.T) {
	ctx := t.Context()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(99,'caller prior')"); err != nil {
		t.Fatal(err)
	}
	scope := newDataScope(dml.Source{DB: h.DB, Tx: tx})
	if err = scope.enrollBufferedScope(ctx); err != nil {
		t.Fatal(err)
	}
	data, err := scope.resolve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = data.Execute("INSERT INTO records VALUES(101,'queued must not execute')"); err != nil {
		t.Fatal(err)
	}
	group, err := drainowner.OpenBindingGroup(scope.nativeInvocation, []drainowner.BindingGroupMember{{Path: "failed", Target: "GET:/failed"}, {Path: "peer", Target: "GET:/peer"}})
	if err != nil {
		t.Fatal(err)
	}
	failedCtx, err := group.Enter(ctx, "failed")
	if err != nil {
		t.Fatal(err)
	}
	if err = drainowner.ValidateBindingGroupTarget(failedCtx, "GET:/failed", true); err != nil {
		t.Fatal(err)
	}
	activity, err := scope.admitActivity(failedCtx)
	if err != nil {
		t.Fatal(err)
	}
	var absent int
	realCause := tx.QueryRowContext(failedCtx, "SELECT id FROM actual_missing_group_table").Scan(&absent)
	if realCause == nil {
		t.Fatal("real SQL failure required")
	}
	if err = scope.finishActivity(activity, realCause); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(drainowner.ProtectedFailure(scope.nativeInvocation), realCause) {
		t.Fatal("root failure deferred or removed")
	}
	if _, err = drainowner.RecordBindingGroupFailure(group, "failed", realCause, false); err != nil {
		t.Fatal(err)
	}
	peerCtx, err := group.Enter(ctx, "peer")
	if err != nil {
		t.Fatal(err)
	}
	if err = drainowner.ValidateBindingGroupTarget(peerCtx, "GET:/peer", true); err != nil {
		t.Fatal(err)
	}
	peerActivity, err := scope.admitActivity(peerCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.QueryRowContext(peerCtx, "SELECT count(*) FROM records WHERE id=99").Scan(&absent); err != nil || absent != 1 {
		t.Fatalf("same caller transaction peer count%d err%v", absent, err)
	}
	if err = scope.finishActivity(peerActivity, nil); err != nil {
		t.Fatal(err)
	}
	if err = group.Close(); err != nil {
		t.Fatal(err)
	}
	if err = completeDataScope(ctx, scope, true, realCause); !errors.Is(err, realCause) {
		t.Fatalf("completion lost real cause %v", err)
	}
	outcome := scope.completionOutcome()
	if len(outcome.Transactions) != 1 || outcome.Transactions[0].State != xhandler.TransactionCallerPending || outcome.CommitConfirmed() {
		t.Fatalf("caller ownership changed %+v", outcome)
	}
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM records").Scan(&absent); err != nil || absent != 1 {
		t.Fatalf("buffer executed or caller completed: count%d err%v", absent, err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(100,'caller still usable')"); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatalf("caller rollback lost authority %v", err)
	}
	if err = h.DB.QueryRowContext(ctx, "SELECT count(*) FROM records").Scan(&absent); err != nil || absent != 0 {
		t.Fatalf("explicit real rollback count%d err%v", absent, err)
	}
}
