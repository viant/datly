package readmeta_test

import (
	"reflect"
	"testing"

	"github.com/viant/datly/sql/reader/readmeta"
)

func TestOutputSlotsAreDetachedAndIndependentOfEmptyRoot(t *testing.T) {
	fields := readmeta.NewFields(reflect.TypeOf(struct{ Count int }{}), [][]int{{0}})
	row := readmeta.NewRecord(fields, nil)
	base := readmeta.NewResult("Rows", false, nil)
	slots := map[string][]*readmeta.Record{"Total": {row}, "Nested.Bounds": {readmeta.NewRecord(nil, nil)}, "Empty": nil}
	result, err := base.WithOutputs(slots)
	if err != nil {
		t.Fatal(err)
	}
	slots["Total"][0] = nil
	delete(slots, "Empty")
	if _, err := base.Output("Total"); err == nil {
		t.Fatal("base result was mutated")
	}
	if _, err := result.Row(0); err == nil {
		t.Fatal("output rows were appended to main root rows")
	}
	total, err := result.Output("Total")
	if err != nil {
		t.Fatal(err)
	}
	actual, err := total.Row(0)
	if err != nil || actual != row || !actual.Fields().Has("Count") || total.RootHolder() != "Total" || total.DirectOutput() {
		t.Fatalf("slot=%+v row=%+v err=%v", total, actual, err)
	}
	if empty, err := result.Output("Empty"); err != nil || len(empty.Rows()) != 0 {
		t.Fatal("known empty slot lost")
	}
	if nested, err := result.Output("Nested.Bounds"); err != nil || nested.RootHolder() != "Nested.Bounds" {
		t.Fatal("canonical full holder path changed")
	}
	for _, slots := range []map[string][]*readmeta.Record{{"Total": nil}, {"Rows": nil}, {"": nil}, {" Total ": nil}} {
		if _, err := result.WithOutputs(slots); err == nil {
			t.Fatalf("invalid/conflicting slots accepted: %v", slots)
		}
	}
	if _, err := readmeta.NewResult("", true, nil).WithOutputs(map[string][]*readmeta.Record{"Total": {row}}); err == nil {
		t.Fatal("direct data acquired an output envelope")
	}
	if _, err := (*readmeta.Result)(nil).Output("Total"); err == nil {
		t.Fatal("nil result output accepted")
	}
	if _, err := result.Output("Missing"); err == nil {
		t.Fatal("unknown output accepted")
	}
}
