package dql

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestQueryListCSVDeclaration(t *testing.T) {
	component, err := parseComponentSource("example.com/lists", "Lists", "#setting($_ = $route('/lists','GET'))\n#define($_ = $IDs<[]int>(query/id).Optional().WithQueryListCSV())\nSELECT id FROM records")
	if err != nil {
		t.Fatal(err)
	}
	params := spec.EffectiveParameters(component.Parameters)
	if len(params) != 1 || !params[0].QueryListCSV {
		t.Fatalf("parameters%+v", params)
	}
}
