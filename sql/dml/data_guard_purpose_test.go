package dml

import (
	"context"
	"errors"
	"testing"

	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/testharness"
)

func TestBoundGuardNativeTerminalBoundaries(t *testing.T) {
	for _, mode := range []string{"prepare", "complete", "closed", "empty", "already-drained"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db := testharness.NewSQLiteHarness(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)"); err != nil {
				t.Fatal(err)
			}
			d := NewData(db.DB)
			if err := d.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			binding := drainowner.NewGuardBinding()
			calls := 0
			var retained context.Context
			incomplete := errors.New("incomplete source program")
			if err := d.RegisterBoundExecutionGuard(func(call context.Context) error {
				calls++
				retained = call
				terminal, err := drainowner.GuardPurpose(call, binding)
				if err != nil {
					return err
				}
				if terminal {
					return incomplete
				}
				return nil
			}, binding); err != nil {
				t.Fatal(err)
			}
			if calls != 0 {
				t.Fatal("registration invoked callback")
			}
			if mode != "empty" {
				if err := d.Execute("INSERT INTO records VALUES(1)"); err != nil {
					t.Fatal(err)
				}
			}
			if err := d.ValidateExecutionGuards(ctx); err != nil {
				t.Fatalf("in-progress check: %v", err)
			}
			if _, err := drainowner.GuardPurpose(retained, binding); !errors.Is(err, drainowner.ErrGuardScope) {
				t.Fatal("scope survived callback", err)
			}
			if mode == "already-drained" {
				if err := d.Flush(ctx, ""); err != nil {
					t.Fatal(err)
				}
			}
			var err error
			switch mode {
			case "prepare":
				err = d.PrepareCompletion(ctx)
			case "closed":
				if err = d.CloseMutationAdmission(); err != nil {
					t.Fatal(err)
				}
				err = d.ValidateExecutionGuards(ctx)
			default:
				err = d.Complete(ctx, nil)
			}
			if !errors.Is(err, incomplete) {
				t.Fatalf("terminal boundary: %v", err)
			}
			if mode == "prepare" || mode == "closed" {
				if err = d.Complete(ctx, nil); !errors.Is(err, incomplete) {
					t.Fatal("sticky failure lost", err)
				}
			}
			var rows int
			if err := db.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&rows); err != nil || rows != 0 {
				t.Fatalf("rollback rows=%d err=%v", rows, err)
			}
		})
	}
}

func TestBoundGuardScopeClosedOnCallbackFailure(t *testing.T) {
	for _, mode := range []string{"error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			db := testharness.NewSQLiteHarness(t)
			d := NewData(db.DB)
			if err := d.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			binding := drainowner.NewGuardBinding()
			var retained context.Context
			if err := d.RegisterBoundExecutionGuard(func(ctx context.Context) error {
				retained = ctx
				if mode == "panic" {
					panic("guard scope cleanup fixture")
				}
				return errors.New("guard scope error fixture")
			}, binding); err != nil {
				t.Fatal(err)
			}
			if err := d.ValidateExecutionGuards(context.Background()); err == nil {
				t.Fatal("failure ignored")
			}
			if _, err := drainowner.GuardPurpose(retained, binding); !errors.Is(err, drainowner.ErrGuardScope) {
				t.Fatal("failure retained live scope", err)
			}
			_ = d.Complete(context.Background(), errors.New("cleanup"))
		})
	}
}

func TestBoundGuardOrderedEmptyCompletion(t *testing.T) {
	ctx := context.Background()
	issuer := drainowner.NewInvocation()
	owners := []*Data{NewData(nil), NewData(nil)}
	for _, owner := range owners {
		handle, err := drainowner.Claim(owner, issuer)
		if err != nil {
			t.Fatal(err)
		}
		if err = handle.Attach(owner, issuer); err != nil {
			t.Fatal(err)
		}
		if err = handle.BindFrame(owner, owner, issuer, issuer.RootFrame()); err != nil {
			t.Fatal(err)
		}
	}
	if err := issuer.EnableJournal(); err != nil {
		t.Fatal(err)
	}
	binding := drainowner.NewGuardBinding()
	called := false
	incomplete := errors.New("ordered source execution incomplete")
	if err := owners[0].RegisterBoundExecutionGuard(func(ctx context.Context) error {
		called = true
		terminal, err := drainowner.GuardPurpose(ctx, binding)
		if err != nil {
			return err
		}
		if !terminal {
			t.Fatal("ordered completion was not terminal")
		}
		return incomplete
	}, binding); err != nil {
		t.Fatal(err)
	}
	drainowner.SealFrame(issuer.RootFrame())
	if ordered, err := issuer.FreezeJournal(); err != nil || !ordered {
		t.Fatalf("ordered=%v error=%v", ordered, err)
	}
	if !drainowner.OrderedJournal(owners[0]) {
		t.Fatal("ordered native branch not exercised")
	}
	if err := owners[0].Complete(ctx, nil); !errors.Is(err, incomplete) || !called {
		t.Fatalf("callback=%v completion=%v", called, err)
	}
	_ = owners[1].Complete(ctx, incomplete)
}

func TestBoundGuardCallerOwnedTransactionRemainsPending(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	d := NewData(db.DB, WithTx(tx))
	if err = d.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	binding := drainowner.NewGuardBinding()
	incomplete := errors.New("caller source execution incomplete")
	if err = d.RegisterBoundExecutionGuard(func(ctx context.Context) error {
		terminal, err := drainowner.GuardPurpose(ctx, binding)
		if err != nil {
			return err
		}
		if terminal {
			return incomplete
		}
		return nil
	}, binding); err != nil {
		t.Fatal(err)
	}
	if err = d.Execute("INSERT INTO records VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	if err = d.Flush(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if err = d.Complete(ctx, nil); !errors.Is(err, incomplete) {
		t.Fatal(err)
	}
	var rows int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("caller transaction lost: rows=%d error=%v", rows, err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err = db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("caller rollback: rows=%d error=%v", rows, err)
	}
}

func TestOrdinaryGuardPreparationTimingAndContextPreserved(t *testing.T) {
	db := testharness.NewSQLiteHarness(t)
	data := NewData(db.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	if err := data.RegisterExecutionGuard(func(call context.Context) error {
		calls++
		if call != ctx {
			t.Fatal("ordinary context replaced")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("ordinary callback invoked on registration")
	}
	if err := data.PrepareCompletion(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("ordinary preparation callback count %d", calls)
	}
	if err := data.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
}
