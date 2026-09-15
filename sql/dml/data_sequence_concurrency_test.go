package dml

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
)

type concurrentSequenceRow struct {
	ID   int64  `sqlx:"id,primaryKey,autoincrement"`
	Name string `sqlx:"name"`
}

var sequenceSchemas = []struct{ name, ddl string }{
	{"autoincrement", "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL)"},
	{"integer", "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT NOT NULL)"},
	{"composite", "CREATE TABLE records(id INTEGER NOT NULL UNIQUE,name TEXT NOT NULL,tenant_id INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(tenant_id,id))"},
}

// Two independent pools and invocation owners start deferred transactions before
// allocation. The first writer pauses after stable IDs/FKs but before Queue.
// SQLite's second writer must wait until completion, not receive the same ID.
func TestSequenceConcurrentInvocationOwners(t *testing.T) {
	for _, journal := range []string{"DELETE", "WAL"} {
		for _, schema := range sequenceSchemas {
			for _, external := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/caller=%v", journal, schema.name, external), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					dsn := filepath.Join(t.TempDir(), "concurrent.db") + "?_busy_timeout=3000&_foreign_keys=on"
					h := sqlite.New(t, sqlite.WithDSN(dsn))
					other := sqlite.New(t, sqlite.WithDSN(dsn))
					if err := h.ExecStatements(ctx, "PRAGMA journal_mode="+journal, schema.ddl, "CREATE TABLE children(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER NOT NULL REFERENCES records(id))"); err != nil {
						t.Fatal(err)
					}
					type outcome struct {
						id  int64
						err error
					}
					begun := make(chan struct{}, 2)
					start := make(chan struct{})
					allocated := make(chan int64, 2)
					release := make(chan struct{})
					done := make(chan outcome, 2)
					worker := func(db *sql.DB, name string) {
						var tx *sql.Tx
						var err error
						var opts []Option
						if external {
							tx, err = db.BeginTx(ctx, nil)
							if err != nil {
								done <- outcome{err: err}
								return
							}
							defer tx.Rollback()
							opts = append(opts, WithTx(tx))
						}
						data := NewData(db, opts...)
						err = data.BeginInvocation()
						if err == nil {
							err = data.Start(ctx)
						}
						begun <- struct{}{}
						<-start
						row := &concurrentSequenceRow{Name: name}
						if err == nil {
							err = data.Reserve(ctx, "records", &concurrentSequenceRow{ID: 90}, "ID")
						}
						if err == nil {
							err = data.Allocate(ctx, "records", row, "ID")
						}
						if err == nil {
							allocated <- row.ID
							<-release
							err = data.Insert("records", row)
							type child struct {
								ID       int64 `sqlx:"id,primaryKey,autoincrement"`
								ParentID int64 `sqlx:"parent_id"`
							}
							c := &child{ParentID: row.ID}
							if err == nil {
								err = data.Allocate(ctx, "children", c, "ID")
							}
							if err == nil {
								err = data.Insert("children", c)
							}
						}
						err = data.Complete(ctx, err)
						if external && err == nil {
							err = tx.Commit()
						}
						done <- outcome{row.ID, err}
					}
					go worker(h.DB, "first")
					go worker(other.DB, "second")
					for i := 0; i < 2; i++ {
						select {
						case <-begun:
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
					}
					close(start)
					var first int64
					select {
					case first = <-allocated:
					case result := <-done:
						t.Fatalf("allocation failed: %+v", result)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					select {
					case id := <-allocated:
						t.Errorf("second writer escaped before commit: first=%d second=%d", first, id)
					case result := <-done:
						t.Errorf("writer failed while first pending: %+v", result)
					case <-time.After(100 * time.Millisecond):
					}
					var n int
					if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&n); err != nil || n != 0 {
						t.Fatalf("committed placeholders: %d %v", n, err)
					}
					close(release)
					seen := map[int64]bool{}
					for i := 0; i < 2; i++ {
						select {
						case result := <-done:
							if result.err != nil || result.id == 0 || seen[result.id] {
								t.Fatalf("invalid completion: %+v", result)
							}
							seen[result.id] = true
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
					}
					if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM children c JOIN records r ON c.parent_id=r.id").Scan(&n); err != nil || n != 2 {
						t.Fatalf("FK reconciliation: %d %v", n, err)
					}
				})
			}
		}
	}
}

func TestSequenceCallerWriteRollbackAndFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			h := sqlite.New(t)
			h.DB.SetMaxOpenConns(1)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL)"); err != nil {
				t.Fatal(err)
			}
			tx, err := h.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err = tx.ExecContext(ctx, "INSERT INTO records(name) VALUES('caller')"); err != nil {
				t.Fatal(err)
			}
			data := NewData(h.DB, WithTx(tx))
			if err = data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			row := &concurrentSequenceRow{Name: "next"}
			if err = data.Allocate(ctx, "records", row, "ID"); err != nil || row.ID != 2 {
				t.Fatalf("caller write allocation: %+v %v", row, err)
			}
			if err = data.Insert("records", row); err != nil {
				t.Fatal(err)
			}
			var cause error
			if fail {
				cause = errors.New("finalization failure")
			}
			err = data.Complete(ctx, cause)
			if !errors.Is(err, cause) {
				t.Fatalf("completion=%v", err)
			}
			var n int
			if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM records").Scan(&n); err != nil {
				t.Fatalf("caller tx completed unexpectedly: %v", err)
			}
			want := 2
			if fail {
				want = 1
			}
			if n != want {
				t.Fatalf("rows=%d want=%d", n, want)
			}
			if err = tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err = h.DB.QueryRowContext(ctx, "SELECT count(*) FROM records").Scan(&n); err != nil || n != 0 {
				t.Fatalf("rollback rows=%d err=%v", n, err)
			}
		})
	}
}

func TestSequenceStandaloneCallerTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	h := sqlite.New(t)
	h.DB.SetMaxOpenConns(1)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT INTO records(name) VALUES('caller')"); err != nil {
		t.Fatal(err)
	}
	data := NewData(h.DB, WithTx(tx))
	row := &concurrentSequenceRow{Name: "next"}
	if err = data.Allocate(ctx, "records", row, "ID"); err != nil || row.ID != 2 {
		t.Fatalf("standalone caller allocation: %+v %v", row, err)
	}
	if err = data.Insert("records", row); err != nil {
		t.Fatal(err)
	}
	if err = data.Flush(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var n int
	if err = h.DB.QueryRowContext(ctx, "SELECT count(*) FROM records").Scan(&n); err != nil || n != 0 {
		t.Fatalf("caller rollback lost: %d %v", n, err)
	}
}
