package dml

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/response"
)

// A counting driver exercises the real sql.Tx boundary without a DB service.
type completionPanicDriver struct {
	beginPanic, execPanic, commitPanic, rollbackPanic any
	begins, execs, commits, rollbacks                 int
}

func (d *completionPanicDriver) Connect(context.Context) (driver.Conn, error) { return d, nil }
func (d *completionPanicDriver) Driver() driver.Driver                        { return d }
func (d *completionPanicDriver) Open(string) (driver.Conn, error)             { return d, nil }
func (d *completionPanicDriver) Close() error                                 { return nil }
func (d *completionPanicDriver) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (d *completionPanicDriver) Begin() (driver.Tx, error) {
	d.begins++
	if d.beginPanic != nil {
		panic(d.beginPanic)
	}
	return d, nil
}
func (d *completionPanicDriver) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	d.execs++
	if d.execPanic != nil {
		panic(d.execPanic)
	}
	return driver.RowsAffected(1), nil
}
func (d *completionPanicDriver) Commit() error {
	d.commits++
	if d.commitPanic != nil {
		panic(d.commitPanic)
	}
	return nil
}
func (d *completionPanicDriver) Rollback() error {
	d.rollbacks++
	if d.rollbackPanic != nil {
		panic(d.rollbackPanic)
	}
	return nil
}

func assertCompletionPanicPrivate(t *testing.T, err error, cause any) {
	t.Helper()
	var recovered *dexec.PanicError
	if !errors.As(err, &recovered) || recovered.Cause() != cause || len(recovered.Stack()) == 0 {
		t.Fatalf("missing panic diagnostics: %v", err)
	}
	if _, public := response.ErrorBody(err); public || response.ErrorStatusCode(err, 500) != 500 {
		t.Fatalf("panic acquired public response: %v", err)
	}
	encoded, marshalErr := json.Marshal(err)
	if marshalErr != nil || strings.Contains(string(encoded), "PRIVATE") || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatalf("panic diagnostics exposed: %s %v", encoded, err)
	}
}

func TestDataCompletionPanicBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name                                 string
		begin, flush, commit, rollback, post bool
		external, fail                       bool
		state                                xhandler.TransactionState
		execs, commits, rollbacks            int
	}{
		{name: "begin during completion flush", begin: true, state: xhandler.TransactionNone},
		{name: "flush rollback", flush: true, state: xhandler.TransactionRolledBack, execs: 1, rollbacks: 1},
		{name: "flush and rollback panic", flush: true, rollback: true, state: xhandler.TransactionRollbackUnknown, execs: 1, rollbacks: 1},
		{name: "commit no retry", commit: true, state: xhandler.TransactionCommitUnknown, execs: 1, commits: 1},
		{name: "rollback no retry", rollback: true, fail: true, state: xhandler.TransactionRollbackUnknown, rollbacks: 1},
		{name: "postcommit evidence", post: true, state: xhandler.TransactionCommitted, execs: 1, commits: 1},
		{name: "caller flush panic", flush: true, external: true, state: xhandler.TransactionCallerPending, execs: 1},
		{name: "caller failure", external: true, fail: true, state: xhandler.TransactionCallerPending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			private := &response.Error{Code: 400, Payload: "PRIVATE panic payload", Cause: errors.New("PRIVATE panic cause")}
			rollbackPrivate := errors.New("PRIVATE rollback panic")
			drv := &completionPanicDriver{}
			if tc.begin {
				drv.beginPanic = private
			}
			if tc.flush {
				drv.execPanic = private
			}
			if tc.commit {
				drv.commitPanic = private
			}
			if tc.rollback {
				drv.rollbackPanic = private
				if tc.flush {
					drv.rollbackPanic = rollbackPrivate
				}
			}
			db := sql.OpenDB(drv)
			t.Cleanup(func() { _ = db.Close() })
			data := NewData(db)
			if tc.external {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				data = NewData(db, WithTx(tx))
				t.Cleanup(func() { _ = tx.Rollback() })
			}
			observers := 0
			data.onCommit = func(context.Context) {
				observers++
				if tc.post {
					panic(private)
				}
			}
			if err := data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			if tc.fail {
				if err := data.Start(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err := data.Execute("INSERT INTO audit VALUES(1)"); err != nil {
				t.Fatal(err)
			}
			var cause error
			if tc.fail {
				cause = errors.New("operation failure")
			}
			err := data.Complete(ctx, cause)
			if tc.begin || tc.flush || tc.commit || tc.rollback || tc.post {
				assertCompletionPanicPrivate(t, err, private)
			} else if err == nil {
				t.Fatal("caller failure was swallowed")
			}
			if cause != nil && !errors.Is(err, cause) {
				t.Fatalf("lost operation failure: %v", err)
			}
			if tc.flush && tc.rollback {
				joined, ok := err.(interface{ Unwrap() []error })
				if !ok || len(joined.Unwrap()) != 2 {
					t.Fatalf("lost rollback panic: %v", err)
				}
				assertCompletionPanicPrivate(t, joined.Unwrap()[1], rollbackPrivate)
			}
			outcome := data.TransactionOutcome()
			if outcome.State != tc.state {
				t.Fatalf("outcome=%+v want=%s", outcome, tc.state)
			}
			if !tc.fail || tc.rollback {
				assertCompletionPanicPrivate(t, outcome.Error, private)
			}
			wantObservers := 0
			if tc.post {
				wantObservers = 1
			}
			if drv.begins != 1 || drv.execs != tc.execs || drv.commits != tc.commits || drv.rollbacks != tc.rollbacks || observers != wantObservers {
				t.Fatalf("begin=%d exec=%d commit=%d rollback=%d observer=%d", drv.begins, drv.execs, drv.commits, drv.rollbacks, observers)
			}
			// The existing admission guard rejects another completion, with no
			// mutation, native completion, observer, or outcome replay.
			if again := data.Complete(ctx, nil); !errors.Is(again, ErrInvocationCompleted) {
				t.Fatalf("repeat completion=%v", again)
			}
			if drv.begins != 1 || drv.execs != tc.execs || drv.commits != tc.commits || drv.rollbacks != tc.rollbacks || observers != wantObservers || data.TransactionOutcome().State != tc.state {
				t.Fatal("completion replayed")
			}
			if tc.external {
				drv.execPanic = nil
				if _, err := data.tx.ExecContext(ctx, "caller still owns transaction"); err != nil {
					t.Fatalf("caller transaction closed: %v", err)
				}
				if err := data.tx.Rollback(); err != nil {
					t.Fatal(err)
				}
				if drv.commits != 0 || drv.rollbacks != 1 || data.TransactionOutcome().State != xhandler.TransactionCallerPending {
					t.Fatal("caller ownership changed")
				}
			}
		})
	}
}

func TestDataPostCommitPanicPreservesPersistedRowSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	data := NewData(h.DB)
	private := errors.New("PRIVATE postcommit observer")
	observers := 0
	data.onCommit = func(context.Context) { observers++; panic(private) }
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	if err := data.Execute("INSERT INTO audit VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	err := data.Complete(ctx, nil)
	assertCompletionPanicPrivate(t, err, private)
	outcome := data.TransactionOutcome()
	if outcome.State != xhandler.TransactionCommitted || observers != 1 {
		t.Fatalf("outcome=%+v observers=%d", outcome, observers)
	}
	assertCompletionPanicPrivate(t, outcome.Error, private)
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{1}})
	if again := data.Complete(ctx, nil); !errors.Is(again, ErrInvocationCompleted) || observers != 1 {
		t.Fatalf("completion replayed: %v", again)
	}
}
