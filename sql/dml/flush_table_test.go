package dml

import "testing"

func TestFlushTableSQLIdentifier(t *testing.T) {
	for _, tc := range []struct {
		queued, requested string
		match             bool
	}{
		{"`records/attributes`", "records/attributes", true},
		{`"records/attributes"`, "records/attributes", true},
		{"`records/attributes`", "RECORDS/ATTRIBUTES", true},
		{"`records/attributes`", "records", false},
		{"`records/attributes`", "records/*", false},
		{"`records/attributes`", " records/attributes", false},
		{"`records/attributes", "records/attributes", false},
	} {
		if got := equalFlushTable(tc.queued, tc.requested); got != tc.match {
			t.Fatalf("%q vs %q: %v", tc.queued, tc.requested, got)
		}
	}
	owner := &Data{open: true}
	other := &Data{open: true, parent: owner}
	first := &dataOperation{id: 1, table: "records", frame: owner}
	selected := &dataOperation{id: 2, table: "`records/attributes`", frame: owner}
	suffix := &dataOperation{id: 3, table: "records", frame: owner}
	owner.queue = []*dataOperation{first, selected, suffix}
	if got := owner.operations("records/attributes", owner); len(got) != 2 || got[0] != first || got[1] != selected {
		t.Fatalf("quoted prefix selection: %v", got)
	}
	if got := owner.operations("records/attributes", other); len(got) != 0 {
		t.Fatal("selected another component's records")
	}
}
