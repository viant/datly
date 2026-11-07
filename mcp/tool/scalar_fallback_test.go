package tool

import (
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"github.com/xeipuuv/gojsonschema"
	"reflect"
	"testing"
)

type fallbackScalarToolBody struct {
	Mode *string `json:"mode" choice:"0:ASAP" choice:"1:SPEND_EVENLY" enumAccept:"scalar" enumStyle:"literal" enumUnknownString:"0" enumUnknownInteger:"0" enumCase:"fold"`
}
type fallbackScalarToolInput struct {
	Body *fallbackScalarToolBody `parameter:"Body,kind=body"`
}

func TestScalarFallbackToolSchemaAndBinding(t *testing.T) {
	binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}
	contract := testRouteContract(t, reflect.TypeFor[fallbackScalarToolInput](), []bindly.BindingSpec{binding})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "Modes"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "modes"}, Contract: contract})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		value    any
		want     string
		nilValue bool
	}{{int64(1), "SPEND_EVENLY", false}, {int64(3), "ASAP", false}, {"other", "ASAP", false}, {"spend_evenly", "SPEND_EVENLY", false}, {"Even", "ASAP", false}, {"1", "ASAP", false}, {nil, "", true}} {
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
		body, _, err := scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[*fallbackScalarToolBody](), "")
		if err != nil {
			t.Fatal(err)
		}
		actual := body.(*fallbackScalarToolBody).Mode
		if (actual == nil) != tc.nilValue || actual != nil && *actual != tc.want {
			t.Fatalf("value=%v actual=%v", tc.value, actual)
		}
	}
}
