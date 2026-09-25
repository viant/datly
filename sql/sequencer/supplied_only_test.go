package sequencer

import (
	"context"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestAllocateSuppliedNumericKeyWithoutNativeSequence(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE generations(generation_no INTEGER PRIMARY KEY,revision TEXT)"); err != nil {
		t.Fatal(err)
	}
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	type generation struct {
		GenerationNo int64  `sqlx:"generation_no,primaryKey"`
		Revision     string `sqlx:"revision"`
	}
	rows := []*generation{{GenerationNo: 7, Revision: "published"}}
	if err := New(h.DB, tx).Allocate(ctx, "generations", rows, "GenerationNo"); err != nil {
		t.Fatal(err)
	}
	if rows[0].GenerationNo != 7 {
		t.Fatalf("supplied generation changed to %d", rows[0].GenerationNo)
	}
}

func TestAllocateSuppliedIdentityStillReservesLaterNativeValues(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT)", "INSERT INTO records VALUES(5)"); err != nil {
		t.Fatal(err)
	}
	type row struct {
		ID int64 `sqlx:"id,primaryKey,autoincrement"`
	}
	service := New(h.DB)
	supplied := []*row{{ID: 6}}
	if err := service.Allocate(ctx, "records", supplied, "ID"); err != nil {
		t.Fatal(err)
	}
	next := []*row{{}}
	if err := service.Allocate(ctx, "records", next, "ID"); err != nil {
		t.Fatal(err)
	}
	if supplied[0].ID != 6 || next[0].ID != 7 {
		t.Fatalf("supplied/later IDs=%d/%d", supplied[0].ID, next[0].ID)
	}
}
