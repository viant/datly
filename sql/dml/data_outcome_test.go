package dml

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	xhandler "github.com/viant/xdatly/handler"
)

func TestManagedDataReportsTransactionOutcomeSQLite(t *testing.T) {
	for _, test := range []struct {
		name                                string
		start, write, flush, fail, external bool
		state                               xhandler.TransactionState
		count                               int
	}{
		{name: "no transaction", state: xhandler.TransactionNone},
		{name: "started empty commit", start: true, state: xhandler.TransactionCommitted},
		{name: "write commit", write: true, state: xhandler.TransactionCommitted, count: 1},
		{name: "failure before transaction", write: true, fail: true, state: xhandler.TransactionNone},
		{name: "rollback", write: true, flush: true, fail: true, state: xhandler.TransactionRolledBack},
		{name: "caller pending success", write: true, external: true, state: xhandler.TransactionCallerPending, count: 1},
		{name: "caller pending failure", write: true, flush: true, fail: true, external: true, state: xhandler.TransactionCallerPending},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			h.DB.SetMaxOpenConns(1)
			if err := h.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER)"); err != nil {
				t.Fatal(err)
			}
			var supplied *sql.Tx
			var options []Option
			if test.external {
				var err error
				supplied, err = h.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer supplied.Rollback()
				options = append(options, WithTx(supplied))
			}
			data := NewData(h.DB, options...)
			if err := data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			if test.start {
				if err := data.Start(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if test.write {
				if err := data.Execute("INSERT INTO audit VALUES(1)"); err != nil {
					t.Fatal(err)
				}
			}
			if test.flush {
				if err := data.Flush(ctx, ""); err != nil {
					t.Fatal(err)
				}
			}
			var cause error
			if test.fail {
				cause = errors.New("handler failure")
			}
			if err := data.Complete(ctx, cause); !errors.Is(err, cause) {
				t.Fatalf("completion=%v cause=%v", err, cause)
			}
			outcome := data.TransactionOutcome()
			if outcome.State != test.state {
				t.Fatalf("outcome=%+v want=%s", outcome, test.state)
			}
			if test.external {
				var err error
				if test.fail {
					err = supplied.Rollback()
				} else {
					err = supplied.Commit()
				}
				if err != nil {
					t.Fatalf("caller transaction was prematurely completed: %v", err)
				}
				if data.TransactionOutcome().State != xhandler.TransactionCallerPending {
					t.Fatal("Data inferred unobserved caller completion")
				}
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{test.count}})
			outcome.State = "changed"
			if data.TransactionOutcome().State != test.state {
				t.Fatal("outcome was not detached")
			}
		})
	}
}

func TestDataReportsUncertainCommitAndRollbackSQLite(t *testing.T) {
	for _, kind := range []string{"commit", "rollback"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			h.DB.SetMaxOpenConns(1)
			if err := h.ExecStatements(ctx, "PRAGMA foreign_keys=ON", "CREATE TABLE parent(id INTEGER PRIMARY KEY)", "CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)"); err != nil {
				t.Fatal(err)
			}
			data := NewData(h.DB)
			if err := data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			var cause error
			want := xhandler.TransactionCommitUnknown
			if kind == "commit" {
				if err := data.Execute("INSERT INTO child VALUES(99)"); err != nil {
					t.Fatal(err)
				}
				if err := data.PrepareCompletion(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := data.Start(ctx); err != nil {
					t.Fatal(err)
				}
				if err := data.tx.Rollback(); err != nil {
					t.Fatal(err)
				}
				cause = errors.New("rollback required")
				want = xhandler.TransactionRollbackUnknown
			}
			if err := data.Complete(ctx, cause); err == nil {
				t.Fatal("expected completion failure")
			}
			outcome := data.TransactionOutcome()
			if outcome.State != want || outcome.Error == nil {
				t.Fatalf("outcome=%+v want=%s", outcome, want)
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM child"}, []struct{ Total int }{{0}})
		})
	}
}
