package dml

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
)

type reservationRow struct {
	ID   int64  `sqlx:"id,primaryKey=true"`
	Name string `sqlx:"name"`
}

// A selected slice reuses the native allocator; it does not clone its rows.
// Distinct ignored slots stay untouched, while a shared pointer remains shared.
// Source-phase execution must account for this before publishing a reached group.
func TestManagedSelectedAllocationPreservesNativeHolderAliases(t *testing.T) {
	for _, alias := range []bool{false, true} {
		t.Run(map[bool]string{false: "distinct ignored holder", true: "ignored alias"}[alias], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT)"); err != nil {
				t.Fatal(err)
			}
			data := NewData(h.DB)
			if err := data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			defer data.Complete(context.Background(), fmt.Errorf("test cleanup"))
			reached := &reservationRow{Name: "reached"}
			ignored := &reservationRow{Name: "ignored"}
			if alias {
				ignored = reached
			}
			successor := &reservationRow{Name: "successor"}
			holder := []*reservationRow{ignored, reached, successor}
			if err := data.Allocate(ctx, "records", []*reservationRow{holder[1]}, "ID"); err != nil {
				t.Fatal(err)
			}
			if reached.ID != 1 || successor.ID != 0 || holder[1] != reached || holder[2] != successor {
				t.Fatalf("selected allocation changed unrelated slots: reached=%d successor=%d", reached.ID, successor.ID)
			}
			wantIgnored := int64(0)
			if alias {
				wantIgnored = reached.ID
			}
			if ignored.ID != wantIgnored {
				t.Fatalf("native alias semantics changed: ignored=%d want=%d", ignored.ID, wantIgnored)
			}
			if err := data.Insert("records", []*reservationRow{reached}); err != nil {
				t.Fatal(err)
			}
			// Allocating the next reached group must not drain the prior INSERT.
			if err := data.Allocate(ctx, "records", []*reservationRow{successor}, "ID"); err != nil {
				t.Fatal(err)
			}
			if successor.ID != 2 {
				t.Fatalf("successor allocation lost the shared native sequence: %d", successor.ID)
			}
			var count int
			if err := data.tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 0 {
				t.Fatalf("allocation drained buffered INSERT: count=%d err=%v", count, err)
			}
			if err := data.Complete(ctx, fmt.Errorf("abort reached groups")); err == nil {
				t.Fatal("failed invocation reported success")
			}
			if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 0 {
				t.Fatalf("failed invocation persisted product rows: count=%d err=%v", count, err)
			}
		})
	}
}

func TestManagedReservationsSeparatePhysicalTableFromCustomSequenceSQLite(t *testing.T) {
	type customRow struct {
		ID int64 `sqlx:"id,primaryKey=true,sequence=custom_sequence"`
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	data := NewData(h.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	defer data.Complete(context.Background(), fmt.Errorf("test cleanup"))
	first, second := &customRow{}, &reservationRow{Name: "ordinary"}
	if err := data.Allocate(ctx, "records", first, "ID"); err != nil {
		t.Fatal(err)
	}
	if err := data.Allocate(ctx, "RECORDS", second, "ID"); err != nil {
		t.Fatal(err)
	}
	if first.ID != 1 || second.ID != 2 {
		t.Fatalf("IDs=%d,%d", first.ID, second.ID)
	}
	if err := data.Insert("records", first); err != nil {
		t.Fatal(err)
	}
	if err := data.Insert("records", second); err != nil {
		t.Fatal(err)
	}
	if err := data.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id FROM records ORDER BY id"}, []struct{ ID int64 }{{1}, {2}})
}

func TestManagedReservationSharesParentTransactionAndSeesExecutedRowsSQLite(t *testing.T) {
	for _, external := range []bool{false, true} {
		t.Run(map[bool]string{false: "owned", true: "caller transaction"}[external], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			h := sqlite.New(t)
			h.DB.SetMaxOpenConns(1)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT)"); err != nil {
				t.Fatal(err)
			}
			var options []Option
			if external {
				tx, err := h.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				options = append(options, WithTx(tx))
			}
			data := NewData(h.DB, options...)
			if err := data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			defer data.Complete(context.Background(), fmt.Errorf("test cleanup"))
			if err := data.Start(ctx); err != nil {
				t.Fatal(err)
			}
			child := data.ComponentData(ComponentImperative, "").(*Data)
			first, second := &reservationRow{Name: "first"}, &reservationRow{Name: "second"}
			if err := data.Allocate(ctx, "records", first, "ID"); err != nil {
				t.Fatal(err)
			}
			if err := child.Execute("INSERT INTO records(id,name) VALUES(50,'already executed')"); err != nil {
				t.Fatal(err)
			}
			if err := child.Flush(ctx, ""); err != nil {
				t.Fatal(err)
			}
			if err := child.Allocate(ctx, "records", second, "ID"); err != nil {
				t.Fatal(err)
			}
			if first.ID != 1 || second.ID != 51 || child.owner().tx != data.tx {
				t.Fatalf("IDs=%d,%d shared tx=%v", first.ID, second.ID, child.owner().tx == data.tx)
			}
			if err := data.Insert("records", first); err != nil {
				t.Fatal(err)
			}
			if err := child.Insert("records", second); err != nil {
				t.Fatal(err)
			}
			child.SealComponent()
			if err := data.Complete(ctx, nil); err != nil {
				t.Fatal(err)
			}
			if external {
				if err := data.tx.Rollback(); err != nil {
					t.Fatal(err)
				}
				h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS count FROM records"}, []struct{ Count int }{{0}})
			} else {
				h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id FROM records ORDER BY id"}, []struct{ ID int }{{1}, {50}, {51}})
			}
		})
	}
}

func TestConcurrentComponentReservationsAreUniqueWithinDataSQLite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	data := NewData(h.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	defer data.Complete(context.Background(), fmt.Errorf("test cleanup"))
	const count = 12
	rows := make([]*reservationRow, count)
	children := make([]*Data, count)
	for i := range rows {
		rows[i] = &reservationRow{Name: fmt.Sprint(i)}
		children[i] = data.ComponentData(ComponentImperative, "").(*Data)
	}
	errors := make(chan error, count)
	var group sync.WaitGroup
	for i := range rows {
		group.Add(1)
		go func(i int) { defer group.Done(); errors <- children[i].Allocate(ctx, "records", rows[i], "ID") }(i)
	}
	group.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	ids := make([]int, len(rows))
	for i, row := range rows {
		ids[i] = int(row.ID)
		if err := children[i].Insert("records", row); err != nil {
			t.Fatal(err)
		}
		children[i].SealComponent()
	}
	sort.Ints(ids)
	for i, id := range ids {
		if id != i+1 {
			t.Fatalf("reserved IDs=%v", ids)
		}
	}
	if err := data.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS count FROM records"}, []struct{ Count int }{{count}})
}

func TestManagedAllocationsReserveDisjointRangesBeforeQueueSQLite(t *testing.T) {
	for _, nested := range []bool{false, true} {
		t.Run(map[bool]string{false: "same frame", true: "nested same table"}[nested], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT)"); err != nil {
				t.Fatal(err)
			}
			data := NewData(h.DB)
			if err := data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			defer data.Complete(context.Background(), fmt.Errorf("test cleanup"))
			child := data
			if nested {
				child = data.ComponentData(ComponentImperative, "").(*Data)
			}
			first := []*reservationRow{{Name: "first"}, {Name: "second"}}
			second := []*reservationRow{{Name: "third"}, {Name: "fourth"}}
			if err := data.Allocate(ctx, "records", first, "ID"); err != nil {
				t.Fatal(err)
			}
			if err := child.Allocate(ctx, "records", second, "ID"); err != nil {
				t.Fatal(err)
			}
			if first[0].ID != 1 || first[1].ID != 2 || second[0].ID != 3 || second[1].ID != 4 {
				t.Fatalf("overlapping reservations before queue: first=%d,%d second=%d,%d", first[0].ID, first[1].ID, second[0].ID, second[1].ID)
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS count FROM records"}, []struct{ Count int }{{0}})
		})
	}
}

func TestManagedReservationsUseSQLiteCanonicalTableIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h := sqlite.New(t)
	h.DB.SetMaxOpenConns(1)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	data := NewData(h.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	defer data.Complete(context.Background(), fmt.Errorf("test cleanup"))
	var rows []*reservationRow
	for _, table := range []string{"records", "RECORDS", `"records"`, "[records]", "main.records", `"MAIN"."RECORDS"`, "'records'"} {
		row := &reservationRow{Name: table}
		if err := data.Allocate(ctx, table, row, "ID"); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
		if row.ID != int64(len(rows)) {
			t.Fatalf("alias %s allocated %d, want %d", table, row.ID, len(rows))
		}
	}
	if err := data.Insert("records", rows); err != nil {
		t.Fatal(err)
	}
	if err := data.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS count FROM records"}, []struct{ Count int }{{7}})
}
