package tool

import (
	"encoding/json"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/tagly/choice"
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
