package sql

import "testing"

func TestProjectionAvailability(t *testing.T) {
	for _, tc := range []struct {
		SQL, name       string
		found, complete bool
	}{
		{`SELECT * FROM (SELECT i.NAME FROM ITEMS i) items`, "ORDER_ID", false, true},
		{`SELECT * FROM (SELECT i.NAME FROM ITEMS i) items`, "NAME", true, true},
		{`SELECT * FROM (SELECT i.ORDER_ID AS ParentKey FROM ITEMS i) items`, "ORDER_ID", false, true},
		{`SELECT * FROM (SELECT i.ORDER_ID AS ParentKey FROM ITEMS i) items`, "ParentKey", true, true},
		{`SELECT * FROM (SELECT i.* FROM ITEMS i) items`, "ORDER_ID", false, false},
		{`SELECT * FROM (SELECT k.* FROM (KINDS) k) kind`, "ID", false, false},
		{`WITH source AS (SELECT i.NAME FROM ITEMS i) SELECT * FROM source items`, "ORDER_ID", false, true},
		{`SELECT * FROM (SELECT n.* FROM (SELECT i.NAME FROM ITEMS i) n) items`, "ORDER_ID", false, true},
	} {
		t.Run(tc.SQL+"/"+tc.name, func(t *testing.T) {
			found, complete, err := (SelectorProjection{SQL: tc.SQL}).HasOutput(tc.name)
			if err != nil || found != tc.found || complete != tc.complete {
				t.Fatalf("availability=%t/%t err=%v", found, complete, err)
			}
		})
	}
}

func TestProjectionSourceAvailability(t *testing.T) {
	for _, tc := range []struct {
		SQL             string
		found, complete bool
	}{
		{`SELECT items.ORDER_ID FROM (SELECT i.NAME FROM ITEMS i) items`, false, true},
		{`SELECT items.ORDER_ID AS ParentKey FROM (SELECT i.ORDER_ID FROM ITEMS i WHERE i.ACTIVE=1) items`, true, true},
		{`SELECT items.ORDER_ID FROM (SELECT i.* FROM ITEMS i) items`, false, false},
		{`#if($enabled) SELECT items.ORDER_ID FROM (SELECT i.NAME FROM ITEMS i) items WHERE 1=1 #end`, false, true},
	} {
		found, complete, err := (SelectorProjection{SQL: tc.SQL}).HasSourceOutput("items", "ORDER_ID")
		if err != nil || found != tc.found || complete != tc.complete {
			t.Fatalf("%s availability=%t/%t err=%v", tc.SQL, found, complete, err)
		}
	}
}

func TestSourceOutputRejectsDuplicateKeyAndDefersOpaqueCTE(t *testing.T) {
	_, _, err := (SelectorProjection{SQL: `SELECT c.id FROM (SELECT a AS id,b AS id FROM records) c`}).HasSourceOutput("c", "id")
	if err == nil {
		t.Fatal("duplicate inner key accepted")
	}
	found, complete, err := (SelectorProjection{SQL: `SELECT c.id FROM (WITH RECURSIVE q(id) AS (VALUES(1)) SELECT id FROM q) c`}).HasSourceOutput("c", "id")
	if err != nil || found || complete {
		t.Fatalf("opaque schema was guessed: %v %v %v", found, complete, err)
	}
}
