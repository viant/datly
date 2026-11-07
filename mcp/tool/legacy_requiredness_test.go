package tool

import (
	"github.com/viant/bindly"
	"github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestExplicitNamedSchemaRequiredness(t *testing.T) {
	for _, tc := range []struct {
		kind     string
		required bool
		want     bool
	}{
		{"query", false, false}, {"query", true, true}, {"form", true, true},
		{"path", false, true}, {"body", false, false}, {"body", true, true},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			contract := testRouteContract(t, reflect.TypeFor[struct{ Value *int }](), []bindly.BindingSpec{{Path: "Value", Location: state.Location{Kind: tc.kind, In: "value"}, Required: &tc.required}})
			plan, err := NewCompiler().Compile(Input{Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "example"}})
			if err != nil {
				t.Fatal(err)
			}
			if got := len(plan.Metadata().InputSchema.Required) > 0; got != tc.want {
				t.Fatalf("required=%v want %v", got, tc.want)
			}
		})
	}
}
