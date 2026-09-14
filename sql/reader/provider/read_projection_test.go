package provider

import (
	"reflect"
	"testing"

	"github.com/viant/datly/sql/reader/readmeta"
	xhandler "github.com/viant/xdatly/handler"
)

func TestReadProjectionRequiresKnownEvidence(t *testing.T) {
	type row struct {
		ID   int
		Name *string
	}
	known := readmeta.NewFields(reflect.TypeOf(row{}), [][]int{{0}, {1}})
	partial := readmeta.NewFields(reflect.TypeOf(row{}), [][]int{{0}})
	child := readmeta.NewRecord(partial, nil)
	root := readmeta.NewRecord(known, map[string][]*readmeta.Record{"Children": {child}, "Empty": nil, "UnknownRow": {readmeta.NewRecord(nil, nil)}})
	projection := &readProjection{result: readmeta.NewResult("Data", false, []*readmeta.Record{root})}
	if projection.RootHolder() != "Data" || projection.DirectOutput() {
		t.Fatal("output context lost")
	}
	for _, test := range []struct {
		name              string
		root              int
		steps             []xhandler.ReadStep
		valid, nameLoaded bool
	}{
		{"root", 0, nil, true, true},
		{"child", 0, []xhandler.ReadStep{{Holder: "Children", Index: 0}}, true, false},
		{"negative root", -1, nil, false, false},
		{"root bounds", 1, nil, false, false},
		{"missing holder", 0, []xhandler.ReadStep{{Holder: "Missing", Index: 0}}, false, false},
		{"empty holder name", 0, []xhandler.ReadStep{{Index: 0}}, false, false},
		{"empty relation", 0, []xhandler.ReadStep{{Holder: "Empty", Index: 0}}, false, false},
		{"child bounds", 0, []xhandler.ReadStep{{Holder: "Children", Index: 1}}, false, false},
		{"unknown fields", 0, []xhandler.ReadStep{{Holder: "UnknownRow", Index: 0}}, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			fields, err := projection.Fields(test.root, test.steps...)
			if (err == nil) != test.valid {
				t.Fatalf("fields=%v error=%v", fields, err)
			}
			if !test.valid {
				if fields != nil {
					t.Fatal("unknown evidence returned a field set")
				}
				return
			}
			if !fields.Has("ID") || fields.Has("Name") != test.nameLoaded {
				t.Fatal("loaded fields incorrect")
			}
		})
	}
	var missing *readProjection
	if fields, err := missing.Fields(0); err == nil || fields != nil {
		t.Fatal("nil projection implies loaded fields")
	}
}
