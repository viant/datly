package sequencer

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/sqlx/io/insert"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"path/filepath"
	"testing"
	"time"
)

type aliasDefaultRow struct {
	ID   int64  `sqlx:"ID,primaryKey=true,autoincrement=true"`
	Name string `sqlx:"NAME"`
}

func aliasDefaultDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "native-default.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err = db.Exec("CREATE TABLE records(ID INTEGER PRIMARY KEY AUTOINCREMENT,NAME TEXT)"); err != nil {
		t.Fatal(err)
	}
	return db
}
func aliasDefaultCatalog(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query("SELECT name || ':' || coalesce(sql,'') FROM sqlite_master WHERE type='table' ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err = rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
func aliasDefaultCount(t *testing.T, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) int {
	t.Helper()
	var count int
	if err := q.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM records").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
func TestAudience050AliasDefaultNativeOrdinaryControls(t *testing.T) {
	for _, managed := range []bool{false, true} {
		t.Run(fmt.Sprintf("callerTransaction=%v", managed), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			db := aliasDefaultDB(t)
			var tx *sql.Tx
			svc := New(db)
			if managed {
				var err error
				tx, err = db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				svc = New(db, tx)
			}
			rows := []*aliasDefaultRow{{Name: "one"}, {Name: "two"}}
			if err := svc.Allocate(ctx, "records", rows, "ID"); err != nil {
				t.Fatal(err)
			}
			if rows[0].ID != 1 || rows[1].ID != 2 {
				t.Fatalf("default unique IDs=%d/%d", rows[0].ID, rows[1].ID)
			}
			q := interface {
				QueryRowContext(context.Context, string, ...any) *sql.Row
			}(db)
			if managed {
				q = tx
			}
			if aliasDefaultCount(t, q) != 0 {
				t.Fatal("allocation wrote product rows before queue")
			}
			var reserved, engine int64
			if err := q.QueryRowContext(ctx, "SELECT value FROM sqlx_sequence_reservations WHERE table_name='records'").Scan(&reserved); err != nil {
				t.Fatal(err)
			}
			if err := q.QueryRowContext(ctx, "SELECT seq FROM sqlite_sequence WHERE name='records'").Scan(&engine); err != nil {
				t.Fatal(err)
			}
			if reserved != 2 || engine != 2 {
				t.Fatalf("default infra reservation=%d engine=%d want2", reserved, engine)
			}
			if !managed {
				var err error
				tx, err = db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
			}
			inserter, err := insert.New(ctx, db, "records")
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range rows {
				if count, _, err := inserter.Exec(ctx, row, tx); err != nil || count != 1 {
					t.Fatal("physical insert", count, err)
				}
			}
			if aliasDefaultCount(t, tx) != 2 {
				t.Fatal("physical pending prefix")
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if aliasDefaultCount(t, db) != 0 {
				t.Fatal("caller rollback lost product ownership")
			}
			var tables int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE name='sqlx_sequence_reservations'").Scan(&tables); err != nil {
				t.Fatal(err)
			}
			if managed {
				if tables != 0 {
					t.Fatal("caller-owned reservation DDL survived rollback")
				}
				var n int
				if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_sequence").Scan(&n); err != nil || n != 0 {
					t.Fatal("caller-owned engine sequence survived", n, err)
				}
			} else {
				if tables != 1 {
					t.Fatal("standalone infrastructure lost")
				}
				if err := db.QueryRowContext(ctx, "SELECT value FROM sqlx_sequence_reservations WHERE table_name='records'").Scan(&reserved); err != nil || reserved != 2 {
					t.Fatal("standalone reserved range rollback", reserved, err)
				}
			}
			t.Logf("NATIVE_CONTROL managed=%v initialSlots=2 publicIDs=1,2 allAllocationBeforeAnyInsert=true reservedBeforeQueue=2 pendingProductRows=2 rollbackProductRows=0 infrastructureTablesAfterRollback=%d", managed, tables)
		})
	}
	t.Run("repeated-supplied-holder", func(t *testing.T) {
		db := aliasDefaultDB(t)
		public := &aliasDefaultRow{ID: 41, Name: "supplied"}
		rows := []*aliasDefaultRow{public, public}
		svc := New(db)
		if err := svc.Allocate(context.Background(), "records", rows, "ID"); err != nil {
			t.Fatal(err)
		}
		if public.ID != 41 || rows[0] != rows[1] || aliasDefaultCount(t, db) != 0 {
			t.Fatal("supplied alias changed")
		}
		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_sequence").Scan(&n); err != nil || n != 0 {
			t.Fatal("supplied alias allocated", n, err)
		}
		t.Log("NATIVE_CONTROL repeated supplied ID41 preserved; no reserved IDs/product writes")
	})
}

// This desired compatibility assertion must fail on the frozen c06 baseline.
// No per-row allocation, copied ID holders or selected alternate strategy.
func TestAudience050AliasDefaultNativeLegacyContract(t *testing.T) {
	db := aliasDefaultDB(t)
	public := &aliasDefaultRow{Name: "same-public"}
	rows := []*aliasDefaultRow{public, public}
	if err := New(db).Allocate(context.Background(), "records", rows, "ID"); err != nil {
		t.Fatalf("LEGACY_COMPATIBILITY_FAIL same public pointer twice must reserve2 and assign sharedID1 before any queue: %v", err)
	}
	if public.ID != 1 || rows[0] != rows[1] {
		t.Fatalf("wrong alias-public identity %d", public.ID)
	}
	var reserved int64
	if err := db.QueryRow("SELECT value FROM sqlx_sequence_reservations WHERE table_name='records'").Scan(&reserved); err != nil || reserved != 2 {
		t.Fatalf("original occurrence reservation=%d err=%v", reserved, err)
	}
	if aliasDefaultCount(t, db) != 0 {
		t.Fatal("allocation-before-queue boundary lost")
	}
}

func TestAudience050AliasDefaultNativeLegacyMixedContract(t *testing.T) {
	db := aliasDefaultDB(t)
	first := &aliasDefaultRow{Name: "shared"}
	later := &aliasDefaultRow{Name: "later"}
	rows := []*aliasDefaultRow{first, first, later}
	if err := New(db).Allocate(context.Background(), "records", rows, "ID"); err != nil {
		t.Fatalf("LEGACY_COMPATIBILITY_FAIL count3 but assignment cursor advances only once for shared holder: %v", err)
	}
	if first.ID != 1 || later.ID != 2 {
		t.Fatalf("expected first-owner assignment prefix1,2 (not slot-index1,3), got%d,%d", first.ID, later.ID)
	}
	var reserved int64
	if err := db.QueryRow("SELECT value FROM sqlx_sequence_reservations WHERE table_name='records'").Scan(&reserved); err != nil || reserved != 3 {
		t.Fatal("whole occurrence reservation must stay3", reserved, err)
	}
	if aliasDefaultCount(t, db) != 0 {
		t.Fatal("allocation wrote product rows")
	}
}

func TestAudience050AliasDefaultCellLocationPrevalidation(t *testing.T) {
	type nullable struct{ ID *int8 }
	same := &nullable{}
	rows := []*nullable{same, same, {}}
	walker, err := NewWalker(rows, []string{"ID"})
	if err != nil {
		t.Fatal(err)
	}
	cells, err := walker.cells(context.Background(), walker.root, rows)
	if err != nil || len(cells) != 3 {
		t.Fatal("nullable cells", len(cells), err)
	}
	if cells[0].location() != cells[1].location() || cells[0].location() == cells[2].location() {
		t.Fatal("nil location identity must be storage address, not zero pointee")
	}
	if err = cells[0].check(127); err != nil || same.ID != nil {
		t.Fatal("prevalidation initialized nullable pointer", err)
	}
	if err = cells[0].check(128); err == nil || same.ID != nil {
		t.Fatal("overflow prevalidation escaped nullable pointer")
	}
	zero := int8(0)
	a, b := &nullable{ID: &zero}, &nullable{ID: &zero}
	walker, err = NewWalker([]*nullable{a, b}, []string{"ID"})
	if err != nil {
		t.Fatal(err)
	}
	cells, err = walker.cells(context.Background(), walker.root, []*nullable{a, b})
	if err != nil || cells[0].location() != cells[1].location() {
		t.Fatal("shared existing pointee must have identical location", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = New(nil).Allocate(ctx, "records", rows, "ID"); err == nil || same.ID != nil || rows[2].ID != nil {
		t.Fatal("cancelled duplicate preflight changed nullable holders", err)
	}
	t.Log("NATIVE_LOCATION shared nil field aliases; distinct nil fields do not alias; shared zero pointee aliases; check/overflow/cancellation leave pointers uninitialized")
}
