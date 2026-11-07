package tool

import (
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"github.com/xeipuuv/gojsonschema"
	"reflect"
	"testing"
)

type openScalarToolBody struct {
	Mode *int64 `json:"mode" choice:"1:FIXED_BUDGET" choice:"2:AUTO_BUDGET" enumAccept:"scalar" enumInteger:"passthrough" enumUnknownString:"0" enumCase:"upper"`
}
type openScalarToolInput struct {
	Body *openScalarToolBody `parameter:"Body,kind=body"`
}

func TestOpenScalarToolSchemaAndBinding(t *testing.T) {
	binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}
	contract := testRouteContract(t, reflect.TypeFor[openScalarToolInput](), []bindly.BindingSpec{binding})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "Modes"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "modes"}, Contract: contract})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		value    any
		want     int64
		nilValue bool
	}{{int64(3), 3, false}, {"other", 0, false}, {"fıxed_budget", 1, false}, {nil, 0, true}} {
		args := map[string]any{"Body": map[string]any{"mode": tc.value}}
		r, err := gojsonschema.Validate(gojsonschema.NewGoLoader(plan.Metadata().InputSchema), gojsonschema.NewGoLoader(args))
		if err != nil || !r.Valid() {
			t.Fatalf("value=%v err=%v result=%v", tc.value, err, r)
		}
		scope, err := plan.Scope(args)
		if err != nil {
			t.Fatal(err)
		}
		defer scope.Close()
		body, _, err := scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[*openScalarToolBody](), "")
		if err != nil {
			t.Fatal(err)
		}
		actual := body.(*openScalarToolBody).Mode
		if (actual == nil) != tc.nilValue || actual != nil && *actual != tc.want {
			t.Fatalf("value=%v actual=%v", tc.value, actual)
		}
	}
}
