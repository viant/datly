package main

import (
	"database/sql"
	"testing"

	"github.com/viant/sqlx/metadata/registry"
)

// Check the stock executable's linkage without contacting Google or importing
// the driver from the test itself.
func TestGoogleSheetsDriverRegistration(t *testing.T) {
	db, err := sql.Open("gsheets", "gsheets://test-book?tab=products")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if product := registry.MatchProduct(db); product == nil || product.Name != "SQLite" {
		t.Fatalf("gsheets needs SQLite metadata for schema discovery, got %v", product)
	}
}
