package tool

import (
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

type pointerOpaquePayload struct{ Value string }

func (*pointerOpaquePayload) MarshalJSON() ([]byte, error) {
	panic("MCP publication executed custom marshaler")
}

type pointerOpaqueInput struct {
	Payload pointerOpaquePayload `json:"payload"`
}

func TestCompilerReflectsPointerOpaqueBodyGoShape(t *testing.T) {
	contract := testRouteContract(t, reflect.TypeFor[pointerOpaqueInput](), []bindly.BindingSpec{{Path: "Payload", Name: "Payload", Location: bindstate.Location{Kind: "body", In: "payload"}}})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Name: "Opaque"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "opaque"}, Contract: contract})
	if err != nil {
		t.Fatal(err)
	}
	property := plan.Metadata().InputSchema.Properties["payload"]
	if property["type"] != "object" || !reflect.DeepEqual(property["properties"], map[string]interface{}{"Value": map[string]interface{}{"type": "string"}}) {
		t.Fatalf("Go shape: %#v", property)
	}
}
