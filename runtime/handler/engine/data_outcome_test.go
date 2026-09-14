package engine

import (
	"context"
	"database/sql"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

func TestDataScopeAggregatesRealDatabaseOutcomesSQLite(t *testing.T) {
	for _, test := range []struct {
		name            string
		childSQL        string
		external        bool
		states          []xhandler.TransactionState
		state           xhandler.TransactionState
		fail, confirmed bool
		firstCount      int
	}{
		{"committed", "INSERT INTO audit VALUES(1)", false, []xhandler.TransactionState{xhandler.TransactionCommitted, xhandler.TransactionCommitted}, xhandler.TransactionCommitted, false, true, 1},
		{"prepare failure", "INSERT INTO audit VALUES(-1)", false, []xhandler.TransactionState{xhandler.TransactionRolledBack, xhandler.TransactionRolledBack}, xhandler.TransactionRolledBack, true, false, 0},
		{"partial commit", "INSERT INTO child VALUES(99)", false, []xhandler.TransactionState{xhandler.TransactionCommitted, xhandler.TransactionCommitUnknown}, xhandler.TransactionPartial, true, false, 1},
		{"caller pending", "INSERT INTO audit VALUES(1)", true, []xhandler.TransactionState{xhandler.TransactionCommitted, xhandler.TransactionCallerPending}, xhandler.TransactionPartial, false, false, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			first, second := sqlite.New(t), sqlite.New(t)
			for _, h := range []*sqlite.Harness{first, second} {
				h.DB.SetMaxOpenConns(1)
				if err := h.ExecStatements(ctx, "PRAGMA foreign_keys=ON", "CREATE TABLE audit(id INTEGER CHECK(id>0))", "CREATE TABLE parent(id INTEGER PRIMARY KEY)", "CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)"); err != nil {
					t.Fatal(err)
				}
			}
			root, owned := invocationDataScope(ctx, sqldml.Source{DB: first.DB})
			if !owned {
				t.Fatal("missing root owner")
			}
			data, err := root.resolve(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := data.Execute("INSERT INTO audit VALUES(1)"); err != nil {
				t.Fatal(err)
			}
			var tx *sql.Tx
			if test.external {
				tx, err = second.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
			}
			child, childOwned := invocationDataScope(withDataScope(ctx, root), sqldml.Source{DB: second.DB, Tx: tx})
			if childOwned {
				t.Fatal("child became a completion owner")
			}
			childData, err := child.resolve(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := childData.Execute(test.childSQL); err != nil {
				t.Fatal(err)
			}
			if err := completeDataScope(ctx, child, false, nil); err != nil {
				t.Fatal(err)
			}
			if child.completionOutcome().CommitConfirmed() {
				t.Fatal("nested completion claimed commit before root")
			}
			err = completeDataScope(ctx, root, true, nil)
			if (err != nil) != test.fail {
				t.Fatalf("complete=%v", err)
			}
			outcome := root.completionOutcome()
			if outcome.State() != test.state || outcome.CommitConfirmed() != test.confirmed || len(outcome.Transactions) != 2 {
				t.Fatalf("aggregate=%+v state=%s confirmed=%v", outcome, outcome.State(), outcome.CommitConfirmed())
			}
			for index, want := range test.states {
				if outcome.Transactions[index].State != want || outcome.Transactions[index].Unit != index {
					t.Fatalf("unit=%+v want=%s", outcome.Transactions[index], want)
				}
			}
			outcome.Transactions[0].State = "mutated"
			if root.completionOutcome().Transactions[0].State != test.states[0] {
				t.Fatal("aggregate snapshot shares mutable entries")
			}
			if tx != nil {
				if err := tx.Rollback(); err != nil {
					t.Fatalf("caller transaction completed early: %v", err)
				}
			}
			first.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{test.firstCount}})
		})
	}
}

type unreportedData struct{ xhandler.Data }

func TestCustomDataFlushSuccessIsNotCommitEvidenceSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	source := &staticDataSource{data: unreportedData{Data: sqldml.NewData(h.DB)}}
	root, owned := invocationDataScope(ctx, source)
	data, err := root.resolve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := data.Execute("INSERT INTO audit VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	if err := completeDataScope(ctx, root, owned, nil); err != nil {
		t.Fatal(err)
	}
	outcome := root.completionOutcome()
	if outcome.State() != xhandler.TransactionUnknown || outcome.CommitConfirmed() {
		t.Fatalf("unsupported owner inferred commit: %+v", outcome)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{1}})
}

func TestFailedOwnershipCannotReusePriorCommitOutcomeSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	data := sqldml.NewData(h.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	if err := data.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := data.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
	root, owned := invocationDataScope(ctx, &staticDataSource{data: data})
	if _, err := root.resolve(ctx); err == nil {
		t.Fatal("completed Data was reused")
	}
	if err := completeDataScope(ctx, root, owned, nil); err == nil {
		t.Fatal("failed ownership returned success")
	}
	if outcome := root.completionOutcome(); outcome.State() != xhandler.TransactionUnknown || outcome.CommitConfirmed() {
		t.Fatalf("prior commit leaked: %+v", outcome)
	}
}
