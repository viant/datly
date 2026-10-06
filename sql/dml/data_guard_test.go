package dml

import (
	"context"
	"errors"
	"github.com/viant/datly/internal/testharness"
	"testing"
)

func TestCapturedExecutionGuardRetainedBeyondChildSealAndPreparation(t *testing.T) {
	for _, prepare := range []bool{false, true} {
		t.Run(map[bool]string{false: "before first flush", true: "after preparation"}[prepare], func(t *testing.T) {
			ctx := context.Background()
			db := testharness.NewSQLiteHarness(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)", "INSERT INTO records VALUES(1),(2)", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER deleted AFTER DELETE ON records BEGIN INSERT INTO audit VALUES(OLD.id);END"); err != nil {
				t.Fatal(err)
			}
			data := NewData(db.DB)
			if err := data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			child := data.ComponentData(ComponentImperative, "").(*Data)
			changed := false
			expected := errors.New("captured identity changed")
			if err := child.RegisterExecutionGuard(func(context.Context) error {
				if changed {
					return expected
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := child.Execute("DELETE FROM records WHERE id=1"); err != nil {
				t.Fatal(err)
			}
			child.SealComponent()
			if prepare {
				if err := data.PrepareCompletion(ctx); err != nil {
					t.Fatal(err)
				}
			}
			changed = true
			if err := data.Complete(ctx, nil); !errors.Is(err, expected) {
				t.Fatalf("completion=%v", err)
			}
			var rows, audits int
			if err := db.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if err := db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&audits); err != nil {
				t.Fatal(err)
			}
			if rows != 2 || audits != 0 {
				t.Fatalf("rows=%d audit=%d", rows, audits)
			}
		})
	}
}

func TestCapturedExecutionGuardFailureIsSticky(t *testing.T) {
	for _, mode := range []string{"error", "panic", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			db := testharness.NewSQLiteHarness(t)
			ctx := context.Background()
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)"); err != nil {
				t.Fatal(err)
			}
			data := NewData(db.DB)
			if err := data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			calls := 0
			if err := data.RegisterExecutionGuard(func(context.Context) error {
				calls++
				if mode == "panic" {
					panic("guard panic")
				}
				return errors.New("guard failure")
			}); err != nil {
				t.Fatal(err)
			}
			if err := data.Execute("INSERT INTO records VALUES(1)"); err != nil {
				t.Fatal(err)
			}
			if mode == "cancel" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if err := data.Flush(ctx, ""); err == nil {
				t.Fatal("flush admitted failing guard")
			}
			if err := data.Complete(context.Background(), nil); err == nil {
				t.Fatal("caught guard failure was cleared")
			}
			var rows int
			if err := db.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&rows); err != nil || rows != 0 {
				t.Fatalf("rows=%d error=%v", rows, err)
			}
			if mode != "cancel" && calls != 1 {
				t.Fatalf("failed callback replayed %d times", calls)
			}
		})
	}
}

func TestGuardedMutationAdmissionPreservesPendingJournal(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(NEW.id);END"); err != nil {
		t.Fatal(err)
	}
	data := NewData(db.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	child := data.ComponentData(ComponentImperative, "").(*Data)
	if err := child.Execute("INSERT INTO records VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	checks := 0
	if err := child.RegisterExecutionGuard(func(context.Context) error { checks++; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := data.CloseMutationAdmission(); err != nil {
		t.Fatal(err)
	}
	if err := data.CloseMutationAdmission(); err != nil {
		t.Fatal(err)
	}
	type row struct {
		ID int `sqlx:"id,primaryKey=true"`
	}
	value := &row{ID: 2}
	for name, run := range map[string]func() error{
		"insert": func() error { return child.Insert("records", value) }, "update": func() error { return child.Update("records", value) }, "delete": func() error { return child.Delete("records", value) }, "execute": func() error { return data.Execute("INSERT INTO records VALUES(2)") },
		"allocate": func() error { return child.Allocate(ctx, "records", value, "ID") }, "reserve": func() error { return child.Reserve(ctx, "records", value, "ID") }, "start": func() error { return child.Start(ctx) },
		"register": func() error { return child.RegisterExecutionGuard(func(context.Context) error { return nil }) },
		"sql-exec": func() error { _, err := child.ExecContext(ctx, "INSERT INTO records VALUES(2)"); return err },
		"sql-query": func() error {
			rs, err := child.QueryContext(ctx, "INSERT INTO records VALUES(2) RETURNING id")
			if rs != nil {
				rs.Close()
			}
			return err
		},
		"sql-queryrow": func() error {
			var n int
			return child.QueryRowContext(ctx, "INSERT INTO records VALUES(2) RETURNING id").Scan(&n)
		},
		"new-component": func() error {
			return data.ComponentData(ComponentImperative, "").Execute("INSERT INTO records VALUES(2)")
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := run(); !errors.Is(err, ErrMutationAdmissionClosed) {
				t.Fatalf("closed admission=%v", err)
			}
		})
	}
	if len(data.markers) != 1 {
		t.Fatal("late component admitted to journal")
	}
	if err := data.ValidateExecutionGuards(ctx); err != nil {
		t.Fatal(err)
	}
	if err := data.PrepareCompletion(ctx); err != nil {
		t.Fatal(err)
	}
	if err := data.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
	var rows, audits int
	if err := db.DB.QueryRow("SELECT COUNT(*) FROM records WHERE id=1").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err := db.DB.QueryRow("SELECT COUNT(*) FROM audit WHERE id=1").Scan(&audits); err != nil {
		t.Fatal(err)
	}
	var extra int
	if err := db.DB.QueryRow("SELECT COUNT(*) FROM records WHERE id<>1").Scan(&extra); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || audits != 1 || extra != 0 || checks < 2 {
		t.Fatalf("rows=%d audit=%d extra=%d checks=%d", rows, audits, extra, checks)
	}
}
