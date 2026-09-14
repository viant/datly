package provider

import (
	"reflect"
	"testing"

	"github.com/viant/datly/sql/reader/readmeta"
	xhandler "github.com/viant/xdatly/handler"
)

func TestReadProjectionOptionalOutputSlots(t *testing.T) {
	fields := readmeta.NewFields(reflect.TypeOf(struct{ Count int }{}), [][]int{{0}})
	result, err := readmeta.NewResult("Rows", false, nil).WithOutputs(map[string][]*readmeta.Record{"Total": {readmeta.NewRecord(fields, nil)}})
	if err != nil {
		t.Fatal(err)
	}
	var root xhandler.ReadProjection = &readProjection{result: result}
	outputs, ok := root.(xhandler.ReadOutputProjection)
	if !ok {
		t.Fatal("optional output projection unavailable")
	}
	total, err := outputs.Output("Total")
	if err != nil {
		t.Fatal(err)
	}
	actual, err := total.Fields(0)
	if err != nil || !actual.Has("Count") || total.RootHolder() != "Total" || total.DirectOutput() {
		t.Fatalf("fields=%v err=%v", actual, err)
	}
	if _, err = root.Fields(0); err == nil {
		t.Fatal("aggregate fabricated a main root row")
	}
	if _, err = outputs.Output("Missing"); err == nil {
		t.Fatal("unknown slot accepted")
	}
	if _, err = (*readProjection)(nil).Output("Total"); err == nil {
		t.Fatal("nil projection accepted")
	}
}
