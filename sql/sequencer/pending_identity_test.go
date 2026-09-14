package sequencer

import (
	"context"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestPendingSuppliedAliasesUseNativeIdentity(t *testing.T) {
	for _, managed := range []bool{false, true} {
		t.Run(map[bool]string{false: "standalone", true: "transaction"}[managed], func(t *testing.T) {
			h := sqlite.New(t)
			h.DB.SetMaxOpenConns(1)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT)", "INSERT INTO records VALUES(5)", "CREATE TEMP TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT)", "INSERT INTO temp.records VALUES(50)", "ATTACH ':memory:' AS aux", "CREATE TABLE aux.records(id INTEGER PRIMARY KEY AUTOINCREMENT)", "INSERT INTO aux.records VALUES(5)"); err != nil {
				t.Fatal(err)
			}
			s := New(h.DB)
			if managed {
				tx, err := h.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				s = New(h.DB, tx)
			}
			for _, tc := range []struct {
				reserved, allocated string
				provided, want      int64
			}{
				{`"MAIN"."RECORDS"`, "[main].[records]", 6, 7},
				{"RECORDS", "'records'", 51, 52},
				{"aux.records", `"AUX"."RECORDS"`, 6, 7},
			} {
				provided, row := &pendingRow{ID: tc.provided}, &pendingRow{}
				if err := s.Reserve(ctx, tc.reserved, provided, "ID"); err != nil {
					t.Fatal(err)
				}
				if err := s.Allocate(ctx, tc.allocated, row, "ID"); err != nil || row.ID != tc.want || provided.ID != tc.provided {
					t.Fatalf("%s -> %s: ID=%d want=%d err=%v", tc.reserved, tc.allocated, row.ID, tc.want, err)
				}
			}
			if err := s.Reserve(ctx, "missing.records", &pendingRow{ID: 9}, "ID"); err == nil {
				t.Fatal("unknown native identity invented authority")
			}
		})
	}
}

func TestIndependentServicesUseDurablyReservedRanges(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT)", "INSERT INTO records VALUES(5)"); err != nil {
		t.Fatal(err)
	}
	first, second := New(h.DB), New(h.DB)
	provided := &pendingRow{ID: 6}
	if err := first.Reserve(ctx, "RECORDS", provided, "ID"); err != nil {
		t.Fatal(err)
	}
	left, right := &pendingRow{}, &pendingRow{}
	if err := first.Allocate(ctx, "records", left, "ID"); err != nil {
		t.Fatal(err)
	}
	if err := second.Allocate(ctx, `"main"."records"`, right, "ID"); err != nil {
		t.Fatal(err)
	}
	if left.ID != 7 || right.ID != 8 || provided.ID != 6 {
		t.Fatalf("independent allocations: %d / %d / %d", provided.ID, left.ID, right.ID)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT seq FROM sqlite_sequence WHERE name='records'"}, []struct{ Seq int }{{8}})
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS n FROM records"}, []struct{ N int }{{1}})
}
