package testharness

import (
	"context"
	"database/sql"
	"testing"
)

func TestNewSQLiteHarness(t *testing.T) {
	h := NewSQLiteHarness(t)
	if h == nil || h.DB == nil {
		t.Fatalf("expected harness with db")
	}
	if err := h.DB.Ping(); err != nil {
		t.Fatalf("failed to ping sqlite db: %v", err)
	}
}

func TestApplyFixtureAndAssertJSON(t *testing.T) {
	h := NewSQLiteHarness(t)
	fixture := &Fixture{
		SchemaSQL: `
CREATE TABLE IF NOT EXISTS t1 (
  id INTEGER PRIMARY KEY,
  name TEXT
);`,
		SeedSQL: `
DELETE FROM t1;
INSERT INTO t1(id, name) VALUES (1, 'john');`,
		Expected: `[{"id":1,"name":"john"}]`,
	}

	if err := h.ApplyFixture(context.Background(), fixture); err != nil {
		t.Fatalf("failed to apply fixture: %v", err)
	}

	rows, err := h.DB.QueryContext(context.Background(), `SELECT id, name FROM t1 ORDER BY id`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	type row struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	var actual []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.ID, &r.Name); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		actual = append(actual, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows failed: %v", err)
	}

	AssertJSONEqual(t, fixture.Expected, actual)
}

func TestDecodeJSON(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
	}
	got, err := DecodeJSON[payload](`{"name":"x"}`)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if got == nil || got.Name != "x" {
		t.Fatalf("unexpected decode result: %#v", got)
	}
}

func TestHarnessUsesConcreteSQLDB(t *testing.T) {
	h := NewSQLiteHarness(t)
	var _ *sql.DB = h.DB
}
