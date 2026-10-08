package tool

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
)

func TestCubeNestedMCPExclusionDoesNotChangeJSONBinding(t *testing.T) {
	type filters struct {
		Period *string `json:"period,omitempty" mcp:"-"`
		From   *string `json:"from,omitempty"`
		To     *string `json:"to,omitempty"`
	}
	type input struct {
		Period  string  `json:"period" mcp:"-"`
		Filters filters `json:"filters"`
	}
	contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{{Path: "Period", Location: bindstate.Location{Kind: "form", In: "period"}}, {Path: "Filters", Location: bindstate.Location{Kind: "body", In: "filters"}}})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Name: "Cube"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "cube"}, Contract: contract})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Metadata().InputSchema.Properties["period"] != nil {
		t.Fatal("reader-style top-level exclusion regressed")
	}
	properties := plan.Metadata().InputSchema.Properties["filters"]["properties"].(map[string]any)
	if properties["period"] != nil || properties["from"] == nil || properties["to"] == nil {
		t.Fatalf("nested MCP exclusion ignored: %+v", properties)
	}
	var bound input
	if err := json.Unmarshal([]byte(`{"filters":{"period":"month","from":"2026-10-01","to":"2026-10-08"}}`), &bound); err != nil {
		t.Fatal(err)
	}
	if bound.Filters.Period == nil || *bound.Filters.Period != "month" {
		t.Fatal("discovery exclusion changed JSON binding")
	}
}
