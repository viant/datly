package tool

import (
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"reflect"
	"strings"
	"testing"
)

type pointerOpaquePayload struct{ Value string }

func (*pointerOpaquePayload) MarshalJSON() ([]byte, error) {
	panic("MCP publication executed custom marshaler")
}

type pointerOpaqueInput struct {
	Payload pointerOpaquePayload `json:"payload"`
}

func TestCompilerRejectsPointerOpaqueBodySchema(t *testing.T) {
	contract := testRouteContract(t, reflect.TypeFor[pointerOpaqueInput](), []bindly.BindingSpec{{Path: "Payload", Name: "Payload", Location: bindstate.Location{Kind: "body", In: "payload"}}})
	_, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Name: "Opaque"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "opaque"}, Contract: contract})
	if err == nil || !strings.Contains(err.Error(), "authored wire authority") {
		t.Fatal("opaque body schema accepted", err)
	}
}
