package dml

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestOptionalStatementResultPolicy(t *testing.T) {
	for _, optional := range []bool{false, true} {
		for _, name := range []string{"positive", "zero", "negative", "result error", "exec error", "overflow", "commit error", "caller failure", "caller transaction"} {
			t.Run(map[bool]string{false: "strict/", true: "optional/"}[optional]+name, func(t *testing.T) {
				ctx := context.Background()
				p := &statementResultDriver{id: 42}
				failure := errors.New(name)
				switch name {
				case "zero":
					p.id = 0
				case "negative":
					p.id = -4
				case "result error":
					p.resultErr = failure
				case "exec error":
					p.execErr = failure
				case "overflow":
					p.id = 128
				case "commit error":
					p.commitErr = failure
				}
				db := sql.OpenDB(p)
				defer db.Close()
				var opts []Option
				var tx *sql.Tx
				if name == "caller transaction" {
					var err error
					tx, err = db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					opts = append(opts, WithTx(tx))
				}
				data := NewData(db, opts...)
				if err := data.BeginInvocation(); err != nil {
					t.Fatal(err)
				}
				id := int8(-9)
				queue := data.ExecuteWithResult
				if optional {
					queue = data.ExecuteWithOptionalResult
				}
				args := []any{"original"}
				if err := queue("INSERT INTO records(name) VALUES (?)", &id, args...); err != nil {
					t.Fatal(err)
				}
				args[0] = "changed"
				if id != -9 || len(p.queries) != 0 {
					t.Fatal("eager result/execution")
				}
				var cause error
				if name == "caller failure" {
					cause = failure
				}
				err := data.Complete(ctx, cause)
				unavailable := name == "result error" || name == "zero" || name == "negative"
				wantErr := name == "exec error" || name == "overflow" || name == "commit error" || name == "caller failure" || name == "result error" && !optional
				if (err != nil) != wantErr {
					t.Fatalf("unexpected error: %v", err)
				}
				wantID := int8(42)
				if name == "zero" {
					wantID = 0
				}
				if name == "negative" {
					wantID = -4
				}
				if optional && unavailable || name == "exec error" || name == "overflow" || name == "caller failure" || name == "result error" {
					wantID = -9
				}
				if id != wantID {
					t.Fatalf("id %d want %d", id, wantID)
				}
				wantCalls := 1
				if name == "exec error" || name == "caller failure" {
					wantCalls = 0
				}
				if p.resultCalls != wantCalls {
					t.Fatalf("result calls %d", p.resultCalls)
				}
				if name != "caller failure" && (len(p.queries) != 1 || p.args[0][0].Value != "original") {
					t.Fatal("SQL/arguments changed")
				}
				if name == "caller transaction" {
					if p.commits != 0 || p.rollbacks != 0 {
						t.Fatal("completed caller transaction")
					}
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
				} else if !wantErr {
					if p.commits != 1 || p.rollbacks != 0 {
						t.Fatal("successful result did not commit")
					}
				} else if name != "commit error" && name != "caller failure" {
					if p.rollbacks != 1 || p.commits != 0 {
						t.Fatal("failure did not rollback")
					}
				}
				count := len(p.queries)
				_ = data.Complete(ctx, nil)
				if len(p.queries) != count || p.resultCalls != wantCalls {
					t.Fatal("completion repeated SQL/result")
				}
				if err := queue("unused", &id); err == nil {
					t.Fatal("closed queue admitted")
				}
			})
		}
	}
}

func TestOptionalStatementResultDestinationValidation(t *testing.T) {
	data := NewData(nil)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	for _, dest := range []any{nil, (*int)(nil), new(uint), new(*int), new(string)} {
		if err := data.ExecuteWithOptionalResult("unused", dest); err == nil || len(data.queue) != 0 {
			t.Fatal("wrong destination admitted")
		}
	}
}

func TestOptionalStatementResultCancellation(t *testing.T) {
	p := &statementResultDriver{id: 42}
	db := sql.OpenDB(p)
	defer db.Close()
	data := NewData(db)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	id := int64(-1)
	if err := data.ExecuteWithOptionalResult("INSERT INTO records(name) VALUES (?)", &id, "name"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := data.Complete(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	if id != -1 || len(p.queries) != 0 || p.resultCalls != 0 {
		t.Fatal("canceled invocation executed")
	}
}
