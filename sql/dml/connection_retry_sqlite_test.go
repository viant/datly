package dml

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	sqlite3 "github.com/mattn/go-sqlite3"
	xhandler "github.com/viant/xdatly/handler"
)

type retryRecord struct {
	ID    int    `sqlx:"id,primaryKey"`
	Value string `sqlx:"value"`
}

func TestConnectionRetryPreservesCallerTransactionSQLite(t *testing.T) {
	for _, faults := range []int{1, 2} {
		t.Run(fmt.Sprintf("faults=%d", faults), func(t *testing.T) {
			var attempts atomic.Int32
			driverName := fmt.Sprintf("datly_retry_caller_%d", retryDriverCounter.Add(1))
			sql.Register(driverName, &sqlite3.SQLiteDriver{ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				return conn.RegisterFunc("insert_fault", func(value string) (int, error) {
					if value == "fault" && int(attempts.Add(1)) <= faults {
						return 0, errors.New("driver: invalid connection")
					}
					return 1, nil
				}, false)
			}})
			db, err := sql.Open(driverName, filepath.Join(t.TempDir(), "caller.sqlite")+"?_journal_mode=WAL")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, statement := range []string{"CREATE TABLE records(id INTEGER PRIMARY KEY,value TEXT NOT NULL)", "INSERT INTO records VALUES(99,'sentinel')", "CREATE TRIGGER fault BEFORE INSERT ON records BEGIN SELECT insert_fault(NEW.value); END"} {
				if _, err = db.Exec(statement); err != nil {
					t.Fatal(err)
				}
			}
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			// Caller work is deliberately invisible to Datly's mutation journal.
			if _, err = tx.Exec("INSERT INTO records VALUES(7,'caller')"); err != nil {
				t.Fatal(err)
			}
			data := NewData(db, WithTx(tx))
			if err = data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			if err = data.Insert("records", &retryRecord{2, "fault"}); err != nil {
				t.Fatal(err)
			}
			err = data.Complete(context.Background(), nil)
			if (err != nil) != (faults == 2) || attempts.Load() != 2 {
				t.Fatalf("failure=%v attempts=%d", err, attempts.Load())
			}
			if data.TransactionOutcome().State != xhandler.TransactionCallerPending {
				t.Fatal("caller ownership lost")
			}
			var inside, outside int
			if err = tx.QueryRow("SELECT COUNT(*) FROM records").Scan(&inside); err != nil {
				t.Fatal(err)
			}
			if err = db.QueryRow("SELECT COUNT(*) FROM records").Scan(&outside); err != nil {
				t.Fatal(err)
			}
			wantInside := 2
			if faults == 1 {
				wantInside = 3
			}
			if inside != wantInside || outside != 1 {
				t.Fatalf("caller/committed rows=%d/%d", inside, outside)
			}
			if _, err = tx.Exec("INSERT INTO records VALUES(8,'after')"); err != nil {
				t.Fatal("Datly closed caller transaction", err)
			}
			if err = tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err = db.QueryRow("SELECT COUNT(*) FROM records").Scan(&outside); err != nil || outside != 1 {
				t.Fatalf("rollback rows=%d error=%v", outside, err)
			}
			if db.Stats().InUse != 0 {
				t.Fatal("connection leak")
			}
		})
	}
}

var retryDriverCounter atomic.Int32

func TestConnectionRetryAfterPartialBatchDeliverySQLite(t *testing.T) {
	type input struct {
		faultIndex  int
		priorDelete bool
	}
	type expected struct {
		failed     bool
		faultCalls int32
		rows       int
	}
	type useCase struct {
		desc     string
		input    input
		expected expected
	}
	for _, tc := range []useCase{
		{"fault before delivered rows recovers", input{0, false}, expected{false, 2, 103}},
		{"partial first chunk then failure rolls back every row", input{100, false}, expected{true, 1, 2}},
		{"partial delivery with prior delete rolls back the complete journal", input{100, true}, expected{true, 1, 2}},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			var faultCalls, triggerCalls atomic.Int32
			name := fmt.Sprintf("datly_retry_partial_%d", retryDriverCounter.Add(1))
			sql.Register(name, &sqlite3.SQLiteDriver{ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				return conn.RegisterFunc("insert_fault", func(value string) (int, error) {
					triggerCalls.Add(1)
					if value == "fault" && faultCalls.Add(1) == 1 {
						return 0, errors.New("driver: invalid connection")
					}
					return 1, nil
				}, false)
			}})
			db, err := sql.Open(name, filepath.Join(t.TempDir(), "partial.sqlite")+"?_journal_mode=WAL")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, statement := range []string{"CREATE TABLE records(id INTEGER PRIMARY KEY,value TEXT NOT NULL)", "INSERT INTO records VALUES(1,'original'),(99,'sentinel')", "CREATE TRIGGER fault BEFORE INSERT ON records BEGIN SELECT insert_fault(NEW.value); END"} {
				if _, err = db.Exec(statement); err != nil {
					t.Fatal(err)
				}
			}
			data := NewData(db)
			if err = data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			if tc.input.priorDelete {
				if err = data.Delete("records", &retryRecord{1, "original"}); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 101; i++ {
				value := fmt.Sprintf("safe-%d", i)
				if i == tc.input.faultIndex {
					value = "fault"
				}
				if err = data.Insert("records", &retryRecord{1000 + i, value}); err != nil {
					t.Fatal(err)
				}
			}
			err = data.Complete(context.Background(), nil)
			if (err != nil) != tc.expected.failed {
				t.Fatalf("completion=%v", err)
			}
			if tc.expected.failed && !strings.Contains(err.Error(), "UNIQUE constraint failed") {
				t.Fatalf("expected replay duplicate after partial delivery: %v", err)
			}
			if faultCalls.Load() != tc.expected.faultCalls || triggerCalls.Load() != 102 {
				t.Fatalf("fault/row attempts=%d/%d", faultCalls.Load(), triggerCalls.Load())
			}
			rows, err := db.Query("SELECT id,value FROM records ORDER BY id")
			if err != nil {
				t.Fatal(err)
			}
			var actual []string
			for rows.Next() {
				var id int
				var value string
				if err = rows.Scan(&id, &value); err != nil {
					t.Fatal(err)
				}
				actual = append(actual, fmt.Sprintf("%d:%s", id, value))
			}
			if err = rows.Err(); err != nil {
				t.Fatal(err)
			}
			if err = rows.Close(); err != nil {
				t.Fatal(err)
			}
			wanted := []string{"1:original", "99:sentinel"}
			if !tc.expected.failed {
				for i := 0; i < 101; i++ {
					value := fmt.Sprintf("safe-%d", i)
					if i == tc.input.faultIndex {
						value = "fault"
					}
					wanted = append(wanted, fmt.Sprintf("%d:%s", 1000+i, value))
				}
			}
			if len(actual) != tc.expected.rows || fmt.Sprint(actual) != fmt.Sprint(wanted) {
				t.Fatalf("complete persisted rows=%v", actual)
			}
			if db.Stats().InUse != 0 {
				t.Fatal("partial-batch transaction leak")
			}
		})
	}
}

func TestConnectionRetrySQLitePersistenceAndCleanup(t *testing.T) {
	type input struct {
		prior   string
		faults  int
		message string
	}
	type expected struct {
		attempts int32
		failed   bool
		rows     string
	}
	type useCase struct {
		desc     string
		input    input
		expected expected
	}
	cases := []useCase{
		{"first eligible insert", input{"", 1, "driver: invalid connection"}, expected{2, false, "[1:original 2:fault 99:sentinel]"}},
		{"prior successful insert suppresses retry", input{"insert", 1, "driver: invalid connection"}, expected{1, true, "[1:original 99:sentinel]"}},
		{"prior successful update suppresses retry", input{"update", 1, "driver: invalid connection"}, expected{1, true, "[1:original 99:sentinel]"}},
		{"prior delete remains committed with recovered insert", input{"delete", 1, "driver: invalid connection"}, expected{2, false, "[2:fault 99:sentinel]"}},
		{"two faults exhaust retry and rollback", input{"", 2, "driver: invalid connection"}, expected{2, true, "[1:original 99:sentinel]"}},
		{"nonrecoverable insert is not replayed", input{"", 1, "permanent source error"}, expected{1, true, "[1:original 99:sentinel]"}},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			var attempts atomic.Int32
			driverName := fmt.Sprintf("datly_retry_%d", retryDriverCounter.Add(1))
			sql.Register(driverName, &sqlite3.SQLiteDriver{ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				return conn.RegisterFunc("insert_fault", func(value string) (int, error) {
					if value == "fault" && int(attempts.Add(1)) <= tc.input.faults {
						return 0, errors.New(tc.input.message)
					}
					return 1, nil
				}, false)
			}})
			db, err := sql.Open(driverName, filepath.Join(t.TempDir(), "retry.sqlite")+"?_journal_mode=WAL&_busy_timeout=200")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(4)
			for _, statement := range []string{"CREATE TABLE records(id INTEGER PRIMARY KEY,value TEXT NOT NULL)", "INSERT INTO records VALUES(1,'original'),(99,'sentinel')", "CREATE TRIGGER fault BEFORE INSERT ON records BEGIN SELECT insert_fault(NEW.value); END"} {
				if _, err = db.Exec(statement); err != nil {
					t.Fatal(err)
				}
			}
			data := NewData(db)
			if err = data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			switch tc.input.prior {
			case "insert":
				err = data.Insert("records", &retryRecord{3, "prior"})
			case "update":
				err = data.Update("records", &retryRecord{1, "updated"})
			case "delete":
				err = data.Delete("records", &retryRecord{1, "original"})
			}
			if err != nil {
				t.Fatal(err)
			}
			// Separate tables prevent planner coalescing prior-success and failing inserts.
			if tc.input.prior == "insert" {
				if err = data.Flush(context.Background(), "records"); err != nil {
					t.Fatal(err)
				}
			}
			if err = data.Insert("records", &retryRecord{2, "fault"}); err != nil {
				t.Fatal(err)
			}
			err = data.Complete(context.Background(), nil)
			if (err != nil) != tc.expected.failed {
				t.Fatalf("failure=%v, expected failed=%v", err, tc.expected.failed)
			}
			if attempts.Load() != tc.expected.attempts {
				t.Fatalf("attempts%d,expected%d", attempts.Load(), tc.expected.attempts)
			}
			rows, err := db.Query("SELECT id,value FROM records ORDER BY id")
			if err != nil {
				t.Fatal(err)
			}
			var state []string
			for rows.Next() {
				var id int
				var value string
				if err = rows.Scan(&id, &value); err != nil {
					t.Fatal(err)
				}
				state = append(state, fmt.Sprintf("%d:%s", id, value))
			}
			if err = rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			if fmt.Sprint(state) != tc.expected.rows {
				t.Fatalf("persisted%s,expected%s", state, tc.expected.rows)
			}
			if db.Stats().InUse != 0 {
				t.Fatal("transaction/connection remains leased after completion")
			}
		})
	}
}
