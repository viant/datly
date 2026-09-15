package sequencer

import (
	"context"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestSequenceFieldAuthorityPrecedesCompositeFieldOrder(t *testing.T) {
	type row struct {
		Tenant int64 `sqlx:"tenant_id,primaryKey"`
		ID     int64 `sqlx:"local_id,primaryKey"`
	}
	h := sqlite.New(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE records(tenant_id INTEGER,local_id INTEGER,PRIMARY KEY(tenant_id,local_id))", "INSERT INTO records VALUES(100,5)"); err != nil {
		t.Fatal(err)
	}
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	s := New(h.DB, tx)
	supplied := &row{Tenant: 100, ID: 6}
	if err = s.Reserve(ctx, "records", supplied, "ID"); err != nil {
		t.Fatal(err)
	}
	generated := &row{Tenant: 100}
	if err = s.Allocate(ctx, "records", generated, "ID"); err != nil {
		t.Fatal(err)
	}
	if generated.ID != 7 || generated.Tenant != 100 || supplied.ID != 6 {
		t.Fatalf("allocation used another key's maximum: %+v supplied %+v", generated, supplied)
	}
}
