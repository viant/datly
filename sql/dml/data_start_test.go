package dml

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	xhandler "github.com/viant/xdatly/handler"
)

func TestTransactionStartSharesOwnerSQLite(t *testing.T) {
	for _, external := range []bool{false, true} {
		for _, fail := range []bool{false, true} {
			t.Run(fmt.Sprintf("external=%v/failure=%v", external, fail), func(t *testing.T) {
				ctx := context.Background()
				h := sqlite.New(t)
				if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)"); err != nil {
					t.Fatal(err)
				}
				var supplied *sql.Tx
				var options []Option
				if external {
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
				if err := data.Start(ctx); err != nil {
					t.Fatal(err)
				}
				transaction := data.tx
				if transaction == nil || external && transaction != supplied {
					t.Fatal("wrong transaction owner")
				}
				child := data.ComponentData(ComponentImperative, "child")
				if err := child.(xhandler.TransactionStarter).Start(ctx); err != nil {
					t.Fatal(err)
				}
				if err := data.Start(ctx); err != nil || data.tx != transaction {
					t.Fatalf("repeated startup replaced transaction: %v", err)
				}
				if err := data.Execute("INSERT INTO records VALUES(1)"); err != nil {
					t.Fatal(err)
				}
				if err := child.Execute("INSERT INTO records VALUES(2)"); err != nil {
					t.Fatal(err)
				}
				child.(*Data).SealComponent()
				var cause error
				if fail {
					cause = errors.New("handler failed")
				}
				if err := data.Complete(ctx, cause); !errors.Is(err, cause) {
					t.Fatalf("completion=%v want=%v", err, cause)
				}
				if external {
					var count int
					if err := supplied.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil {
						t.Fatalf("caller transaction was completed: %v", err)
					}
					want := 2
					if fail {
						want = 0
					}
					if count != want {
						t.Fatalf("caller transaction rows=%d want=%d", count, want)
					}
					if err := supplied.Rollback(); err != nil {
						t.Fatal(err)
					}
				}
				want := 0
				if !external && !fail {
					want = 2
				}
				h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM records"}, []struct {
					Total int `sqlx:"total"`
				}{{want}})
				if err := data.Start(ctx); !errors.Is(err, ErrInvocationCompleted) {
					t.Fatalf("completed startup=%v", err)
				}
			})
		}
	}
}

func TestTransactionStartRejectsUnmanagedAndCanceled(t *testing.T) {
	h := sqlite.New(t)
	data := NewData(h.DB)
	if err := data.Start(context.Background()); err == nil || data.tx != nil {
		t.Fatal("unmanaged startup opened transaction")
	}
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := data.Start(ctx); !errors.Is(err, context.Canceled) || data.tx != nil {
		t.Fatalf("canceled startup=%v", err)
	}
	if err := data.Complete(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestTransactionStartRejectsSealedAndFailedFrames(t *testing.T) {
	h := sqlite.New(t)
	data := NewData(h.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	child := data.ComponentData(ComponentImperative, "closed").(*Data)
	child.SealComponent()
	if err := child.Start(context.Background()); !errors.Is(err, ErrComponentSealed) || data.tx != nil {
		t.Fatalf("sealed frame startup=%v tx=%v", err, data.tx)
	}
	failure := errors.New("prior operation failed")
	data.markFailed(failure)
	if err := data.Start(context.Background()); !errors.Is(err, ErrInvocationFailed) || !errors.Is(err, failure) || data.tx != nil {
		t.Fatalf("failed invocation startup=%v tx=%v", err, data.tx)
	}
	if err := data.Complete(context.Background(), nil); !errors.Is(err, failure) {
		t.Fatalf("completion=%v", err)
	}
	if err := (*Data)(nil).Start(context.Background()); err == nil {
		t.Fatal("nil Data startup accepted")
	}
	if err := data.Start(nil); err == nil {
		t.Fatal("nil context startup accepted")
	}
}
