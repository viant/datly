package dml

import (
	"context"
	"database/sql"
	"errors"
	"github.com/viant/datly/internal/testharness"
	xhandler "github.com/viant/xdatly/handler"
	"testing"
)

func TestCapturedGuardEnrollmentRejectsPriorStreamingUse(t *testing.T) {
	for _, api := range []string{"rows", "row"} {
		for _, state := range []string{"open", "consumed", "closed", "errored", "cancelled"} {
			if api == "row" && state == "closed" {
				continue
			}
			t.Run(api+"/"+state, func(t *testing.T) {
				ctx := context.Background()
				db := testharness.NewSQLiteHarness(t)
				if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(NEW.id);END"); err != nil {
					t.Fatal(err)
				}
				data := NewData(db.DB)
				if err := data.BeginInvocation(); err != nil {
					t.Fatal(err)
				}
				if err := data.Execute("INSERT INTO records VALUES(1)"); err != nil {
					t.Fatal(err)
				}
				query := "INSERT INTO records VALUES(2) RETURNING id"
				if state == "errored" {
					query = "SELECT id FROM missing_records"
				}
				queryCtx := ctx
				if state == "cancelled" {
					var cancel context.CancelFunc
					queryCtx, cancel = context.WithCancel(ctx)
					cancel()
				}
				var rows *sql.Rows
				var row *sql.Row
				var initial error
				if api == "rows" {
					rows, initial = data.QueryContext(queryCtx, query)
					if rows != nil {
						defer rows.Close()
						if state == "consumed" {
							var n int
							if !rows.Next() {
								t.Fatal(rows.Err())
							}
							if err := rows.Scan(&n); err != nil {
								t.Fatal(err)
							}
							rows.Close()
						} else if state == "closed" {
							rows.Close()
						}
					}
				} else {
					row = data.QueryRowContext(queryCtx, query)
					initial = row.Err()
					if state == "consumed" {
						var n int
						initial = row.Scan(&n)
					}
				}
				if (state == "errored" || state == "cancelled") && initial == nil {
					t.Fatal("fixture streaming error not reached")
				}
				if err := data.EnableCapturedExecutionGuards(); !errors.Is(err, ErrGuardedStreamingQuery) {
					t.Fatalf("prior streaming use enrolled: %v", err)
				}
				checks := 0
				if err := data.RegisterExecutionGuard(func(context.Context) error { checks++; return nil }); err == nil {
					t.Fatal("failed enrollment cleared")
				}
				if rows != nil {
					rows.Close()
				}
				if row != nil && state == "open" {
					var ignored int
					_ = row.Scan(&ignored)
				}
				expected := error(ErrGuardedStreamingQuery)
				if state == "cancelled" {
					expected = context.Canceled
				}
				if err := data.Complete(ctx, nil); !errors.Is(err, expected) {
					t.Fatalf("caught enrollment failure lost its native cause: %v", err)
				}
				var records, audits int
				if err := db.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&records); err != nil {
					t.Fatal(err)
				}
				if err := db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&audits); err != nil {
					t.Fatal(err)
				}
				if records != 0 || audits != 0 || checks != 0 {
					t.Fatalf("records=%d audit=%d guards=%d", records, audits, checks)
				}
			})
		}
	}
}

func TestCapturedGuardEnrollmentRejectsBothAPIsBeforeDispatch(t *testing.T) {
	for _, api := range []string{"rows", "row"} {
		t.Run(api, func(t *testing.T) {
			ctx := context.Background()
			db := testharness.NewSQLiteHarness(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			data := NewData(db.DB)
			if err := data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			if err := data.EnableCapturedExecutionGuards(); err != nil {
				t.Fatal(err)
			}
			if err := data.EnableCapturedExecutionGuards(); err != nil {
				t.Fatal(err)
			}
			// A missing table must never become the reported driver error.
			var err error
			if api == "rows" {
				rs, e := data.QueryContext(ctx, "INSERT INTO nonexistent VALUES(2) RETURNING id")
				err = e
				if rs != nil {
					rs.Close()
					t.Fatal("rejected query returned rows")
				}
			} else {
				var id int
				err = data.QueryRowContext(ctx, "INSERT INTO nonexistent VALUES(2) RETURNING id").Scan(&id)
			}
			if !errors.Is(err, ErrGuardedStreamingQuery) {
				t.Fatalf("streaming SQL reached driver: %v", err)
			}
			if err = data.Complete(ctx, nil); !errors.Is(err, ErrGuardedStreamingQuery) {
				t.Fatalf("caught query error was cleared: %v", err)
			}
		})
	}
}

func TestCapturedGuardEnrollmentKeepsCallerTransactionPending(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	data := NewData(db.DB, WithTx(tx))
	if err = data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	var id int
	if err = data.QueryRowContext(ctx, "SELECT 1").Scan(&id); err != nil {
		t.Fatal(err)
	}
	if err = data.EnableCapturedExecutionGuards(); !errors.Is(err, ErrGuardedStreamingQuery) {
		t.Fatalf("enrollment=%v", err)
	}
	if err = data.Complete(ctx, nil); !errors.Is(err, ErrGuardedStreamingQuery) {
		t.Fatalf("completion=%v", err)
	}
	if data.TransactionOutcome().State != xhandler.TransactionCallerPending {
		t.Fatal(data.TransactionOutcome())
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(9)"); err != nil {
		t.Fatalf("framework completed caller TX: %v", err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var n int
	if err = db.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&n); err != nil || n != 0 {
		t.Fatalf("caller rollback=%d/%v", n, err)
	}
}
