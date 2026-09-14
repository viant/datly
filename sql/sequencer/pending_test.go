package sequencer

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/sqlx/metadata/sink"
)

type pendingRow struct {
	ID int64 `sqlx:"id,primaryKey,autoincrement"`
}

func TestPendingAllocationSkipsSuppliedIDs(t *testing.T) {
	for _, managed := range []bool{false, true} {
		name := "standalone"
		if managed {
			name = "transaction"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT)", "INSERT INTO records VALUES(5)", "CREATE TABLE other(id INTEGER PRIMARY KEY AUTOINCREMENT)", "INSERT INTO other VALUES(5)"); err != nil {
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
			rows := []*pendingRow{{ID: 6}, {}}
			if err := s.Allocate(ctx, "records", rows, "ID"); err != nil {
				t.Fatal(err)
			}
			if rows[0].ID != 6 || rows[1].ID != 7 {
				t.Fatalf("pending IDs=%d/%d, want 6/7", rows[0].ID, rows[1].ID)
			}
			// Supplied-only roles, including explicit zero, are registered before
			// later independent batches. Far-away values must not jump the range.
			reserved := []*pendingRow{{ID: 0}, {ID: 8}, {ID: 10}, {ID: math.MaxInt64}}
			if err := s.Reserve(ctx, "records", reserved, "ID"); err != nil {
				t.Fatal(err)
			}
			next := []*pendingRow{{}, {}}
			if err := s.Allocate(ctx, "records", next, "ID"); err != nil {
				t.Fatal(err)
			}
			if next[0].ID != 9 || next[1].ID != 11 || reserved[0].ID != 0 || reserved[3].ID != math.MaxInt64 {
				t.Fatalf("reserved=%+v next=%d/%d", reserved, next[0].ID, next[1].ID)
			}
			other := &pendingRow{}
			if err := s.Allocate(ctx, "other", other, "ID"); err != nil || other.ID != 6 {
				t.Fatalf("unrelated table ID=%d err=%v", other.ID, err)
			}
			// No actual row was inserted by allocation.
			var count int
			if managed {
				if err := s.tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil {
					t.Fatal(err)
				}
			} else if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatalf("allocated rows before Queue: %d", count)
			}
		})
	}
}

func TestPendingAllocationCancelAndOverflowAreNotPublished(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	row := &pendingRow{}
	s := New(nil)
	for _, run := range []func(context.Context, string, any, string) error{s.Reserve, s.Allocate} {
		if err := run(ctx, "records", row, "ID"); !errors.Is(err, context.Canceled) || row.ID != 0 {
			t.Fatalf("cancellation=%v row=%+v", err, row)
		}
	}
	h := sqlite.New(t)
	ctx = context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT)", "INSERT INTO records VALUES(126)"); err != nil {
		t.Fatal(err)
	}
	type narrow struct {
		ID *int8 `sqlx:"id,primaryKey,autoincrement"`
	}
	rows := []*narrow{{}, {}}
	if err := New(h.DB).Allocate(ctx, "records", rows, "ID"); err == nil {
		t.Fatal("accepted narrow integer overflow")
	}
	if rows[0].ID != nil || rows[1].ID != nil {
		t.Fatal("partial allocation escaped overflow preflight")
	}
}

func TestReservationPreservesNativeIncrementAndStart(t *testing.T) {
	s := New(nil)
	for _, want := range []int64{7, 17, 27} {
		native := &sink.Sequence{Name: "records", StartValue: 7, IncrementBy: 5, Value: 17}
		got, err := s.reserve("records", native, 2)
		if err != nil || got.MinValue(2) != want || got.Value != want+10 {
			t.Fatalf("range=%+v err=%v want min=%d", got, err, want)
		}
	}
	for _, invalid := range []*sink.Sequence{
		{StartValue: -1, IncrementBy: 1, Value: math.MaxInt64},
		{StartValue: 7, IncrementBy: 5, Value: 7},
		{StartValue: 7, IncrementBy: 5, Value: 17, MaxValue: 10},
		{StartValue: math.MinInt64, IncrementBy: 2, Value: math.MinInt64 + 1},
	} {
		if err := s.validateRange(invalid, 2); err == nil {
			t.Fatalf("accepted invalid range %+v", invalid)
		}
	}
}

func TestPendingAllocationRejectsAliasedEmptyHolders(t *testing.T) {
	type row struct{ ID *int64 }
	zero := int64(0)
	for _, rows := range [][]*row{{{ID: &zero}, {ID: &zero}}, {nil, {}, nil}} {
		if len(rows) == 3 {
			rows[2] = rows[1]
		}
		if err := New(nil).Allocate(context.Background(), "records", rows, "ID"); err == nil {
			t.Fatal("accepted aliased empty holders")
		}
		if zero != 0 {
			t.Fatal("alias failure changed the original holder")
		}
	}
}
