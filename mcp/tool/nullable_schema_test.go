package tool

import (
	"reflect"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
)

func TestNullableBusinessFieldSchema(t *testing.T) {
	type body struct {
		Note *string              `json:"note"`
		Has  *struct{ Note bool } `setMarker:"true"`
	}
	type input struct {
		Body *body `json:"body"`
	}
	for _, required := range []bool{false, true} {
		contract := testRouteContract(t, reflect.TypeOf(input{}), []bindly.BindingSpec{{Path: "Body", Location: bindstate.Location{Kind: "body", In: "body"}, Required: &required}})
		plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Name: "Patch"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "patch"}, Contract: contract})
		if err != nil {
			t.Fatal(err)
		}
		property := plan.Metadata().InputSchema.Properties["body"]
		if required {
			if property["type"] != "object" {
				t.Fatal("required body permits null")
			}
		} else if !reflect.DeepEqual(property["type"], []string{"object", "null"}) {
			t.Fatal("optional body lost nullability")
		}
		fields := property["properties"].(map[string]any)
		if _, ok := fields["Has"]; ok {
			t.Fatal("presence leaked")
		}
		kinds := fields["note"].(map[string]any)["type"].([]string)
		if !reflect.DeepEqual(kinds, []string{"string", "null"}) {
			t.Fatal("nullable note lost null")
		}
		kinds[0] = "tampered"
		fresh := plan.Metadata().InputSchema.Properties["body"]["properties"].(map[string]any)["note"].(map[string]any)["type"]
		if !reflect.DeepEqual(fresh, []string{"string", "null"}) {
			t.Fatal("schema clone aliases type union")
		}
	}
}
