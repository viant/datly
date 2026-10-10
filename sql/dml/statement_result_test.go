package dml

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	xhandler "github.com/viant/xdatly/handler"
)

// A per-test connector observes exactly which transaction/result APIs the queue
// uses. It deliberately rejects RowsAffected: identity must not depend on it.
type statementResultDriver struct {
	queries                         []string
	args                            [][]driver.NamedValue
	id                              int64
	execErr, resultErr, commitErr   error
	resultCalls, commits, rollbacks int
}

func (p *statementResultDriver) Connect(context.Context) (driver.Conn, error) { return p, nil }
func (p *statementResultDriver) Driver() driver.Driver                        { return p }
func (p *statementResultDriver) Open(string) (driver.Conn, error)             { return p, nil }
func (p *statementResultDriver) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (p *statementResultDriver) Close() error              { return nil }
func (p *statementResultDriver) Begin() (driver.Tx, error) { return p, nil }
func (p *statementResultDriver) Commit() error             { p.commits++; return p.commitErr }
func (p *statementResultDriver) Rollback() error           { p.rollbacks++; return nil }
func (p *statementResultDriver) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	p.queries = append(p.queries, query)
	p.args = append(p.args, append([]driver.NamedValue(nil), args...))
	return p, p.execErr
}
func (p *statementResultDriver) LastInsertId() (int64, error) {
	p.resultCalls++
	return p.id, p.resultErr
}
func (p *statementResultDriver) RowsAffected() (int64, error) {
	return 0, errors.New("unexpected RowsAffected")
}

func TestBufferedStatementResultTransactionFailures(t *testing.T) {
	const statement = "INSERT INTO records(name) VALUES (?) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)"
	for _, name := range []string{"success", "exec error", "result error", "overflow", "commit error", "caller transaction"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			p := &statementResultDriver{id: 42}
			failure := errors.New(name)
			switch name {
			case "exec error":
				p.execErr = failure
			case "result error":
				p.resultErr = failure
			case "overflow":
				p.id = 128
			case "commit error":
				p.commitErr = failure
			}
			db := sql.OpenDB(p)
			defer db.Close()
			var options []Option
			var caller *sql.Tx
			if name == "caller transaction" {
				var err error
				caller, err = db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				options = append(options, WithTx(caller))
				defer caller.Rollback()
			}
			d := NewData(db, options...)
			if err := d.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			id := int8(-1)
			args := []any{"first"}
			if err := d.ExecuteWithResult(statement, &id, args...); err != nil {
				t.Fatal(err)
			}
			args[0] = "changed"
			if id != -1 || len(p.queries) != 0 {
				t.Fatal("queue executed eagerly")
			}
			err := d.Complete(ctx, nil)
			if name == "success" || name == "caller transaction" {
				if err != nil || id != 42 {
					t.Fatalf("id=%d err=%v", id, err)
				}
			} else if err == nil {
				t.Fatal("failure reported success")
			}
			if name == "exec error" || name == "result error" || name == "overflow" {
				if id != -1 || p.rollbacks != 1 || p.commits != 0 {
					t.Fatalf("failed transaction: id=%d probe=%+v", id, p)
				}
			}
			if len(p.queries) != 1 || p.queries[0] != statement || len(p.args[0]) != 1 || p.args[0][0].Value != "first" {
				t.Fatalf("statement/arguments changed: %+v", p)
			}
			wantResult := 1
			if name == "exec error" {
				wantResult = 0
			}
			if p.resultCalls != wantResult {
				t.Fatalf("result calls=%d", p.resultCalls)
			}
			if name == "commit error" && d.TransactionOutcome().State != xhandler.TransactionCommitUnknown {
				t.Fatalf("outcome=%+v", d.TransactionOutcome())
			}
			if name == "caller transaction" {
				if p.commits != 0 || p.rollbacks != 0 {
					t.Fatal("caller transaction completed by Data")
				}
				if err := caller.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			// Completion is terminal: another completion must never repeat SQL.
			_ = d.Complete(ctx, nil)
			if len(p.queries) != 1 {
				t.Fatal("statement replayed")
			}
		})
	}
}

func TestBufferedStatementResultSharesJournalSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	d := NewData(h.DB)
	if err := d.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	var first, child, last int64
	if err := d.ExecuteWithResult("INSERT INTO records(name) VALUES (?)", &first, "before"); err != nil {
		t.Fatal(err)
	}
	c := d.ComponentData(ComponentImperative, "").(*Data)
	if err := c.ExecuteWithResult("INSERT INTO records(name) VALUES (?)", &child, "child"); err != nil {
		t.Fatal(err)
	}
	c.SealComponent()
	if err := d.ExecuteWithResult("INSERT INTO records(name) VALUES (?)", &last, "after"); err != nil {
		t.Fatal(err)
	}
	if first != 0 || child != 0 || last != 0 {
		t.Fatal("identity assigned before drain")
	}
	if err := d.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual([]int64{first, child, last}, []int64{1, 2, 3}) {
		t.Fatalf("journal IDs %d,%d,%d", first, child, last)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT name FROM records ORDER BY id"}, []struct{ Name string }{{"before"}, {"child"}, {"after"}})
}

func TestBufferedStatementResultDownstreamRollbackSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT)"); err != nil {
		t.Fatal(err)
	}
	d := NewData(h.DB)
	if err := d.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	var id int64
	if err := d.ExecuteWithResult("INSERT INTO records DEFAULT VALUES", &id); err != nil {
		t.Fatal(err)
	}
	if err := d.Execute("INSERT INTO missing_table DEFAULT VALUES"); err != nil {
		t.Fatal(err)
	}
	if err := d.Complete(ctx, nil); err == nil {
		t.Fatal("downstream failure reported success")
	}
	if d.TransactionOutcome().State != xhandler.TransactionRolledBack {
		t.Fatalf("outcome=%+v", d.TransactionOutcome())
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM records"}, []struct{ Total int }{{0}})
}

func TestBufferedStatementResultRejectsInvalidAndClosedDestinations(t *testing.T) {
	var nilInt *int
	for _, dest := range []any{nil, nilInt, 1, new(uint64), new(string)} {
		d := NewData(nil)
		if err := d.ExecuteWithResult("unused", dest); err == nil || len(d.queue) != 0 {
			t.Fatalf("admitted invalid destination %T", dest)
		}
	}
	for _, state := range []string{"sealed", "completed", "closed"} {
		d := NewData(nil)
		if err := d.BeginInvocation(); err != nil {
			t.Fatal(err)
		}
		switch state {
		case "sealed":
			d.SealComponent()
		case "completed":
			if err := d.Complete(context.Background(), nil); err != nil {
				t.Fatal(err)
			}
		case "closed":
			if err := d.CloseMutationAdmission(); err != nil {
				t.Fatal(err)
			}
		}
		id := int64(17)
		if err := d.ExecuteWithResult("unused", &id); err == nil || id != 17 || len(d.queue) != 0 {
			t.Fatalf("admitted %s result", state)
		}
	}
}

func TestBufferedStatementResultSignedConversionBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name     string
		dest     any
		min, max int64
	}{
		{"int8", new(int8), -128, 127},
		{"int16", new(int16), -32768, 32767},
		{"int32", new(int32), -2147483648, 2147483647},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateLastInsertID(tc.dest); err != nil {
				t.Fatal(err)
			}
			for _, id := range []int64{tc.min, tc.max} {
				if err := assignLastInsertID(tc.dest, id); err != nil || reflect.ValueOf(tc.dest).Elem().Int() != id {
					t.Fatalf("exact boundary %d: %v", id, err)
				}
			}
			for _, id := range []int64{tc.min - 1, tc.max + 1} {
				if err := assignLastInsertID(tc.dest, id); err == nil || reflect.ValueOf(tc.dest).Elem().Int() != tc.max {
					t.Fatalf("overflow mutated destination: %d, %v", id, err)
				}
			}
		})
	}
	var wide int64
	for _, id := range []int64{-9223372036854775808, 9223372036854775807} {
		if err := assignLastInsertID(&wide, id); err != nil || wide != id {
			t.Fatalf("int64 boundary %d: %v", id, err)
		}
	}
}
