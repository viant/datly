package engine

import (
	"context"
	"database/sql"
	"errors"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	"testing"
)

// Deliberately hide native optional lifecycle methods behind public Data.
type guardedOwnerForward struct {
	xhandler.Data
	native *dml.Data
}

func (d *guardedOwnerForward) BeginInvocation() error { return d.native.BeginInvocation() }
func (d *guardedOwnerForward) PrepareCompletion(ctx context.Context) error {
	return d.native.PrepareCompletion(ctx)
}
func (d *guardedOwnerForward) Complete(ctx context.Context, e error) error {
	return d.native.Complete(ctx, e)
}
func (d *guardedOwnerForward) ComponentData(relation, order string) xhandler.Data {
	return d.native.ComponentData(relation, order)
}

type validForwardingOwner struct{ *dml.Data }

type registrationOnlyOwner struct{ *guardedOwnerForward }

func (d *registrationOnlyOwner) RegisterExecutionGuard(f func(context.Context) error) error {
	return d.native.RegisterExecutionGuard(f)
}

type validationOnlyOwner struct{ *guardedOwnerForward }

func (d *validationOnlyOwner) ValidateExecutionGuards(ctx context.Context) error {
	return d.native.ValidateExecutionGuards(ctx)
}

type guardOwnerSource struct {
	data xhandler.Data
	db   *sql.DB
}

func (s *guardOwnerSource) Open(context.Context) (xhandler.Data, error) { return s.data, nil }
func (s *guardOwnerSource) InvocationKey() any                          { return s.db }

func TestGuardRegistrationRequiresActualUnitOwnerLifecycle(t *testing.T) {
	for _, mode := range []string{"registration-only", "validation-only", "child-capable-owner-incapable", "valid-forwarding-owner"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)", "INSERT INTO records VALUES(1)"); err != nil {
				t.Fatal(err)
			}
			native := dml.NewData(db.DB)
			base := &guardedOwnerForward{Data: native, native: native}
			var owner xhandler.Data
			switch mode {
			case "registration-only":
				owner = &registrationOnlyOwner{base}
			case "validation-only":
				owner = &validationOnlyOwner{base}
			case "child-capable-owner-incapable":
				owner = base
			default:
				owner = &validForwardingOwner{native}
			}
			root, _ := invocationDataScope(ctx, &guardOwnerSource{data: owner, db: db.DB})
			data, err := root.resolve(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = data.Execute("DELETE FROM records WHERE id=1"); err != nil {
				t.Fatal(err)
			}
			scope := root
			if mode == "child-capable-owner-incapable" {
				scope, _ = invocationDataScope(withDataScope(ctx, root), &guardOwnerSource{data: owner, db: db.DB})
				child, err := scope.resolve(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, ok := child.(guardedInvocationData); !ok {
					t.Fatal("fixture child did not expose native capability")
				}
			}
			calls := 0
			err = scope.registerExecutionGuard(ctx, func(context.Context) error { calls++; return nil })
			if mode == "valid-forwarding-owner" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("partial/child owner admitted captured guard")
			}
			if complete := root.complete(ctx, err); mode == "valid-forwarding-owner" && complete != nil {
				t.Fatal(complete)
			}
			var count int
			if err = db.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if mode == "valid-forwarding-owner" {
				if count != 0 || calls == 0 {
					t.Fatalf("count=%d checks=%d", count, calls)
				}
			} else if count != 1 || calls != 0 {
				t.Fatalf("partial owner escaped rollback count=%d checks=%d", count, calls)
			}
		})
	}
}

func TestGuardedCompletionRejectsRetainedLaterUnitWork(t *testing.T) {
	ctx := context.Background()
	first, second := sqlite.New(t), sqlite.New(t)
	for _, db := range []*sql.DB{first.DB, second.DB} {
		for _, q := range []string{"CREATE TABLE records(id INTEGER PRIMARY KEY)", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(NEW.id);END"} {
			if _, err := db.Exec(q); err != nil {
				t.Fatal(err)
			}
		}
	}
	var later *dml.Data
	var root *dataScope
	var restore func(context.Context) context.Context
	calls := 0
	failures := map[string]error{}
	source := dml.Source{DB: first.DB, OnCommit: func(ctx context.Context) {
		calls++
		failures["queue"] = later.Execute("INSERT INTO records VALUES(2)")
		failures["allocate"] = later.Allocate(ctx, "records", &struct {
			ID int `sqlx:"id,primaryKey=true"`
		}{ID: 2}, "ID")
		failures["reserve"] = later.Reserve(ctx, "records", &struct {
			ID int `sqlx:"id,primaryKey=true"`
		}{ID: 2}, "ID")
		failures["start"] = later.Start(ctx)
		_, failures["sql"] = later.ExecContext(ctx, "INSERT INTO records VALUES(2)")
		failures["register"] = later.RegisterExecutionGuard(func(context.Context) error { return nil })
		failures["component"] = later.ComponentData(dml.ComponentImperative, "").Execute("INSERT INTO records VALUES(2)")
		_, failures["resolve"] = root.resolve(ctx)
		failures["invoker-writer"] = CheckComponentMutation(restore(context.Background()), false)
		failures["invoker-reader"] = CheckComponentMutation(restore(context.Background()), true)
	}}
	root, _ = invocationDataScope(ctx, source)
	firstData, err := root.resolve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = firstData.Execute("INSERT INTO records VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	child, _ := invocationDataScope(withDataScope(ctx, root), dml.Source{DB: second.DB})
	childData, err := child.resolve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	later = child.unit.data.(*dml.Data)
	if err = childData.Execute("INSERT INTO records VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	if err = child.registerExecutionGuard(ctx, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	restore = RetainMutationAuthority(withDataScope(ctx, root))
	if err = root.complete(ctx, nil); err == nil {
		t.Fatal("ignored protected terminal attempts permitted root success")
	}
	if calls != 1 || len(failures) != 10 {
		t.Fatalf("callbacks=%d attempts=%d", calls, len(failures))
	}
	for name, e := range failures {
		if e == nil {
			t.Fatalf("retained %s admitted after preflight", name)
		}
		if name != "resolve" && name != "invoker-reader" && name != "invoker-writer" && !errors.Is(e, dml.ErrMutationAdmissionClosed) {
			t.Fatalf("%s=%v", name, e)
		}
	}
	for index, db := range []*sql.DB{first.DB, second.DB} {
		var rows, audit int
		if err = db.QueryRow("SELECT COUNT(*) FROM records WHERE id=1").Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if err = db.QueryRow("SELECT COUNT(*) FROM audit").Scan(&audit); err != nil {
			t.Fatal(err)
		}
		want := 1
		if index == 1 {
			want = 0
		}
		if rows != want || audit != want {
			t.Fatalf("late work escaped unit=%d rows=%d audit=%d want=%d", index, rows, audit, want)
		}
	}
	outcome := root.completionOutcome()
	if outcome.Error == nil || len(outcome.Transactions) != 2 || outcome.Transactions[0].State != xhandler.TransactionCommitted || outcome.Transactions[1].State != xhandler.TransactionRolledBack || outcome.CommitConfirmed() {
		t.Fatalf("partial native outcomes not truthful: %+v", outcome)
	}
}

type ordinaryCompletionArgumentOwner struct {
	*dml.Data
	cause error
}

func (d *ordinaryCompletionArgumentOwner) Complete(ctx context.Context, cause error) error {
	d.cause = cause
	return d.Data.Complete(ctx, cause)
}
func TestOrdinaryCompletionPreservesErrorArgumentIdentity(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	owner := &ordinaryCompletionArgumentOwner{Data: dml.NewData(db.DB)}
	root, _ := invocationDataScope(ctx, &guardOwnerSource{data: owner, db: db.DB})
	data, err := root.resolve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = data.Execute("INSERT INTO records VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("ordinary handler failure")
	if err = root.complete(ctx, cause); !errors.Is(err, cause) {
		t.Fatalf("completion lost cause: %v", err)
	}
	if owner.cause != cause {
		t.Fatalf("ordinary owner received a wrapped error: original=%T actual=%T", cause, owner.cause)
	}
	var rows int
	if err = db.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("ordinary rollback rows=%d err=%v", rows, err)
	}
}
