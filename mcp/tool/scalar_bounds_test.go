package tool

import (
	"bytes"
	"encoding/json"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/tagly/choice"
	"github.com/xeipuuv/gojsonschema"
	"reflect"
	"testing"
)

func TestScalarInt64MCPPersistedBounds(t *testing.T) {
	enumeration, err := choice.ParseEnum(`choice:"1:A" enumAccept:"scalar" enumInteger:"passthrough"`, reflect.TypeFor[int64]())
	if err != nil {
		t.Fatal(err)
	}
	original := registry.EnumInputSchema(enumeration, false)
	cloned := original.Clone()
	*cloned.AnyOf[0].Maximum = "1"
	if *original.AnyOf[0].Maximum != "9223372036854775807" {
		t.Fatal("clone mutated source")
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var restored spec.WireSchema
	if err = json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(wireSchemaMap(restored.AnyOf[0]))
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]json.RawMessage
	if err = json.Unmarshal(data, &wire); err != nil {
		t.Fatal(err)
	}
	if string(wire["minimum"]) != "-9223372036854775808" || string(wire["maximum"]) != "9223372036854775807" {
		t.Fatalf("rounded persisted MCP bounds: %s", data)
	}
}

func TestScalarInt64CompiledToolBoundaryValidation(t *testing.T) {
	binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}
	contract := testRouteContract(t, reflect.TypeFor[openScalarToolInput](), []bindly.BindingSpec{binding})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "BoundedMode"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "bounded_mode"}, Contract: contract})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(plan.Metadata())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte(`"minimum":-9223372036854775808`)) || !bytes.Contains(data, []byte(`"maximum":9223372036854775807`)) {
		t.Fatalf("compiled metadata rounded bounds: %s", data)
	}
	schema, err := json.Marshal(plan.Metadata().InputSchema)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		value string
		valid bool
	}{{"-9223372036854775808", true}, {"9223372036854775807", true}, {"-9223372036854775809", false}, {"9223372036854775808", false}} {
		t.Run(tc.value, func(t *testing.T) {
			result, err := gojsonschema.Validate(gojsonschema.NewBytesLoader(schema), gojsonschema.NewStringLoader(`{"Body":{"mode":`+tc.value+`}}`))
			if err != nil {
				t.Fatal(err)
			}
			if result.Valid() != tc.valid {
				t.Fatalf("valid=%v expected=%v errors=%v", result.Valid(), tc.valid, result.Errors())
			}
		})
	}
}
