package readmeta_test

import (
	"reflect"
	"testing"

	"github.com/viant/datly/sql/reader/readmeta"
)

type Embedded struct{ ID int }
type row struct {
	Embedded
	Name *string
}

func TestFieldsPreserveNativeIndexesAndKnowledge(t *testing.T) {
	rowType := reflect.TypeOf(row{})
	indexes := [][]int{{0, 0}, {1}}
	fields := readmeta.NewFields(rowType, indexes)
	indexes[0][0] = 1
	indexes[1] = nil
	for _, name := range []string{"ID", "Embedded.ID", "Name"} {
		if !fields.Has(name) {
			t.Fatalf("loaded field %q lost", name)
		}
	}
	for _, name := range []string{"id", "Missing", "Embedded.Missing"} {
		if fields.Has(name) {
			t.Fatalf("unknown/non-Go selector %q accepted", name)
		}
	}
	if !fields.Known() {
		t.Fatal("valid native schema marked unknown")
	}
	empty := readmeta.NewFields(rowType, nil)
	if !empty.Known() || empty.Has("ID") {
		t.Fatal("known empty projection confused with unknown")
	}
	for _, invalid := range []reflect.Type{nil, reflect.TypeOf(1), reflect.TypeOf([]row{})} {
		if candidate := readmeta.NewFields(invalid, nil); candidate.Known() || candidate.Has("ID") {
			t.Fatalf("invalid schema %v marked known", invalid)
		}
	}
}

func TestReadResultMetadataIsDetached(t *testing.T) {
	fields := readmeta.NewFields(reflect.TypeOf(row{}), [][]int{{0, 0}})
	child := readmeta.NewRecord(fields, nil)
	relations := map[string][]*readmeta.Record{"Children": {child}, "Empty": nil}
	parent := readmeta.NewRecord(nil, relations)
	relations["Children"][0] = nil
	delete(relations, "Children")
	children, known := parent.Relation("Children")
	if !known || len(children) != 1 || children[0] != child {
		t.Fatal("constructor retained mutable relation slice/map")
	}
	children[0] = nil
	again, _ := parent.Relation("Children")
	if again[0] != child {
		t.Fatal("returned relation slice mutates snapshot")
	}
	if empty, known := parent.Relation("Empty"); !known || len(empty) != 0 {
		t.Fatal("known empty relation lost")
	}
	if _, known := parent.Relation("Unknown"); known {
		t.Fatal("unknown relation reported known")
	}
	if parent.Fields().Known() || !child.Fields().Known() {
		t.Fatal("field knowledge changed across relation")
	}
	rows := []*readmeta.Record{parent}
	result := readmeta.NewResult("Data", false, rows)
	rows[0] = nil
	view := result.Rows()
	if result.RootHolder() != "Data" || result.DirectOutput() || len(view) != 1 || view[0] != parent {
		t.Fatal("result snapshot changed")
	}
	view[0] = nil
	if result.Rows()[0] != parent {
		t.Fatal("returned root slice mutates snapshot")
	}
}

func TestReadMetadataNilReceivers(t *testing.T) {
	var result *readmeta.Result
	var record *readmeta.Record
	var fields *readmeta.Fields
	if result.RootHolder() != "" || result.DirectOutput() || len(result.Rows()) != 0 || record.Fields() != nil || fields.Known() || fields.Has("ID") {
		t.Fatal("nil metadata reports evidence")
	}
	if _, known := record.Relation("Children"); known {
		t.Fatal("nil record reports relation evidence")
	}
}

func TestReadMetadataIndexedAccess(t *testing.T) {
	child := readmeta.NewRecord(nil, nil)
	parent := readmeta.NewRecord(nil, map[string][]*readmeta.Record{"Children": {child}, "Empty": nil})
	result := readmeta.NewResult("", true, []*readmeta.Record{parent, nil})
	for _, index := range []int{-1, 2} {
		if _, err := result.Row(index); err == nil {
			t.Fatalf("invalid root index %d accepted", index)
		}
	}
	if got, err := result.Row(0); err != nil || got != parent {
		t.Fatalf("root=%v error=%v", got, err)
	}
	if got, err := result.Row(1); err != nil || got != nil {
		t.Fatalf("unknown root=%v error=%v", got, err)
	}
	for _, test := range []struct {
		holder string
		index  int
	}{{"", 0}, {"Unknown", 0}, {"Empty", 0}, {"Children", -1}, {"Children", 1}} {
		if _, err := parent.RelationRow(test.holder, test.index); err == nil {
			t.Fatalf("invalid relation %+v accepted", test)
		}
	}
	if got, err := parent.RelationRow("Children", 0); err != nil || got != child {
		t.Fatalf("child=%v error=%v", got, err)
	}
	var missing *readmeta.Record
	if _, err := missing.RelationRow("Children", 0); err == nil {
		t.Fatal("nil record accepted")
	}
	var absent *readmeta.Result
	if _, err := absent.Row(0); err == nil {
		t.Fatal("nil result accepted")
	}
	if allocations := testing.AllocsPerRun(100, func() {
		root, _ := result.Row(0)
		_, _ = root.RelationRow("Children", 0)
	}); allocations != 0 {
		t.Fatalf("indexed metadata lookup allocates: %v", allocations)
	}
}
