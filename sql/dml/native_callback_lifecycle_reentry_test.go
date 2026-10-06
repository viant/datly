package dml

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/testharness/sqlite"
	xhandler "github.com/viant/xdatly/handler"
)

func TestNativeProtectedCommitCallbackLifecycleRejectsPromptly(t *testing.T) {
	for _, method := range []string{"BeginInvocation", "ValidateExecutionGuards", "CloseMutationAdmission"} {
		for _, borrowed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/borrowed=%t", method, borrowed), func(t *testing.T) {
				ctx := context.Background()
				h := sqlite.New(t)
				if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(new.id);END"); err != nil {
					t.Fatal(err)
				}
				var native, target *Data
				var rejection error
				callbacks := 0
				native = NewData(h.DB, WithCommitObserver(func(context.Context) {
					callbacks++
					switch method {
					case "BeginInvocation":
						rejection = target.BeginInvocation()
					case "ValidateExecutionGuards":
						rejection = target.ValidateExecutionGuards(ctx)
					case "CloseMutationAdmission":
						rejection = target.CloseMutationAdmission()
					}
				}))
				i := drainowner.NewInvocation()
				handle, err := drainowner.Claim(native, i)
				if err != nil {
					t.Fatal(err)
				}
				if err = handle.Attach(native, i); err != nil {
					t.Fatal(err)
				}
				target = native
				if borrowed {
					target = native.ComponentData(ComponentImperative, "").(*Data)
				}
				if err = target.Execute("INSERT INTO records VALUES(1)"); err != nil {
					t.Fatal(err)
				}
				if err = drainowner.EnrollActivities(i); err != nil {
					t.Fatal(err)
				}
				// Genuine engine-style preflight and closure remain usable outside a drain.
				if err = native.ValidateExecutionGuards(ctx); err != nil {
					t.Fatal(err)
				}
				if err = native.CloseMutationAdmission(); err != nil {
					t.Fatal(err)
				}
				if err = drainowner.CloseActivities(i); err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { done <- handle.Call(ctx, native, i, drainowner.Completion, nil) }()
				select {
				case err = <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("same-owner native commit callback self-waited")
				}
				if callbacks != 1 || !errors.Is(rejection, drainowner.ErrDrain) || !errors.Is(drainowner.ProtectedFailure(i), drainowner.ErrDrain) {
					t.Fatalf("callback=%d rejection=%v ledger=%v", callbacks, rejection, drainowner.ProtectedFailure(i))
				}
				if outcome := native.TransactionOutcome(); outcome.State != xhandler.TransactionCommitted || outcome.Error != nil {
					t.Fatalf("first outcome rewritten: %+v", outcome)
				}
				for _, table := range []string{"records", "audit"} {
					var count int
					if err = h.DB.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 1 {
						t.Fatalf("%s=%d error=%v", table, count, err)
					}
				}
			})
		}
	}
}
