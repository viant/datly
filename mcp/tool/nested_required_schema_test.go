package tool

import (
	"reflect"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
)

func TestNestedRequiredSchemaRetainsArrayItemsAndOptionalMembers(t *testing.T) {
	type filters struct {
		Dates *[]string `json:"dates,omitempty" mcp:"name=dates,aliases=legacy_dates"`
		IDs   *[]int    `json:"ids,omitempty"`
		Label *string   `json:"label,omitempty"`
	}
	type input struct {
		Filters filters `json:"filters"`
	}
	required := true
	param := &spec.Parameter{WireSchemas: map[string]*spec.WireSchema{
		"Filters.dates": {Required: true}, "Filters.ids": {Required: true},
	}}
	contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{{
		Path: "Filters", Name: "Filters", Location: bindstate.Location{Kind: "body", In: "filters"}, Required: &required, Extension: param,
	}})
	plan, err := NewCompiler().Compile(Input{Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "cube"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"dates", "legacy_dates"} {
		scope, err := plan.Scope(map[string]interface{}{"filters": map[string]interface{}{key: []string{"2026-01-01"}, "ids": []int{1}}})
		if err != nil {
			t.Fatalf("required metadata broke member alias %s: %v", key, err)
		}
		scope.Close()
	}
	schema := plan.Metadata().InputSchema
	if !reflect.DeepEqual(schema.Required, []string{"filters"}) {
		t.Fatalf("root required: %v", schema.Required)
	}
	object := schema.Properties["filters"]
	if !reflect.DeepEqual(object["required"], []string{"dates", "ids"}) {
		t.Fatalf("nested required: %v", object["required"])
	}
	props := object["properties"].(map[string]interface{})
	for name, itemType := range map[string]string{"dates": "string", "ids": "integer"} {
		property := props[name].(map[string]interface{})
		if property["type"] != "array" || property["items"].(map[string]interface{})["type"] != itemType {
			t.Fatalf("lost array contract: %+v", property)
		}
	}
	if !reflect.DeepEqual(props["label"].(map[string]interface{})["type"], []string{"string", "null"}) {
		t.Fatal("optional pointer contract changed")
	}
}
