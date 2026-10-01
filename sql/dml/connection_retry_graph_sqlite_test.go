package dml

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
)

import sqlite3 "github.com/mattn/go-sqlite3"

type retryAllocatedRecord struct {
	ID    int64  `sqlx:"id,primaryKey,autoincrement"`
	Value string `sqlx:"value"`
}

func TestConnectionRetryRetainsPrestartedSequenceAndNestedJournalSQLite(t *testing.T) {
	type input struct{ allocate, nested, cancel, finalizerFail bool }
	type expected struct {
		calls    int32
		failed   bool
		inserted bool
	}
	type useCase struct {
		desc     string
		input    input
		expected expected
	}
	for _, tc := range []useCase{
		{"prestarted owned transaction", input{}, expected{2, false, true}},
		{"allocated identity survives same transaction retry", input{allocate: true}, expected{2, false, true}},
		{"child journal shares recovered root transaction", input{nested: true}, expected{2, false, true}},
		{"cancellation suppresses second insert attempt", input{cancel: true}, expected{1, true, false}},
		{"finalizer failure rolls back recovered child and parent SQL", input{nested: true, finalizerFail: true}, expected{2, true, false}},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var calls atomic.Int32
			name := fmt.Sprintf("datly_retry_graph_%d", retryDriverCounter.Add(1))
			sql.Register(name, &sqlite3.SQLiteDriver{ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				return conn.RegisterFunc("insert_fault", func(value string) (int, error) {
					if value == "fault" && calls.Add(1) == 1 {
						if tc.input.cancel {
							cancel()
						}
						return 0, errors.New("driver: invalid connection")
					}
					return 1, nil
				}, false)
			}})
			db, err := sql.Open(name, filepath.Join(t.TempDir(), "graph.sqlite")+"?_journal_mode=WAL")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, statement := range []string{"CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,value TEXT NOT NULL)", "INSERT INTO records VALUES(1,'original'),(99,'sentinel')", "CREATE TRIGGER fault BEFORE INSERT ON records BEGIN SELECT insert_fault(NEW.value); END"} {
				if _, err = db.Exec(statement); err != nil {
					t.Fatal(err)
				}
			}
			data := NewData(db)
			if err = data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			// Transaction lifetime is deliberately independent from the operation
			// cancellation, so explicit completion must own cleanup synchronously.
			if err = data.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			_, initialTx := data.InvocationTransaction()
			row := &retryAllocatedRecord{ID: 100, Value: "fault"}
			if tc.input.allocate {
				row.ID = 0
				if err = data.Allocate(ctx, "records", row, "ID"); err != nil {
					t.Fatal(err)
				}
				if row.ID != 100 {
					t.Fatalf("allocated ID=%d", row.ID)
				}
			}
			writer := data
			if tc.input.nested {
				if err = data.Execute("UPDATE records SET value='updated' WHERE id=1"); err != nil {
					t.Fatal(err)
				}
				writer = data.ComponentData(ComponentImperative, "").(*Data)
			}
			if err = writer.Insert("records", row); err != nil {
				t.Fatal(err)
			}
			if writer != data {
				writer.SealComponent()
			}
			var cause error
			if tc.input.finalizerFail {
				if err = data.PrepareCompletion(ctx); err != nil {
					t.Fatal(err)
				}
				cause = errors.New("declared output finalizer failed")
			}
			err = data.Complete(ctx, cause)
			if (err != nil) != tc.expected.failed || calls.Load() != tc.expected.calls {
				t.Fatalf("error=%v calls=%d", err, calls.Load())
			}
			if cause != nil && !errors.Is(err, cause) {
				t.Fatal("finalizer cause lost")
			}
			_, finalTx := data.InvocationTransaction()
			if finalTx != initialTx || row.ID != 100 {
				t.Fatal("retry replaced transaction or assigned identity")
			}
			rows, err := db.Query("SELECT id,value FROM records ORDER BY id")
			if err != nil {
				t.Fatal(err)
			}
			var actual []string
			for rows.Next() {
				var id int64
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
			if tc.expected.inserted {
				if tc.input.nested {
					wanted[0] = "1:updated"
				}
				wanted = append(wanted, "100:fault")
			}
			if !reflect.DeepEqual(actual, wanted) {
				t.Fatalf("persisted=%v expected=%v", actual, wanted)
			}
			if db.Stats().InUse != 0 {
				t.Fatal("retry graph leaked transaction")
			}
		})
	}
}
