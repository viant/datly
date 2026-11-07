package registry

import (
	"encoding/json"
	"github.com/viant/tagly/choice"
	"reflect"
	"testing"
)

func TestScalarInt64SchemaRetainsUpperBoundary(t *testing.T) {
	enumeration, err := choice.ParseEnum(`choice:"1:A" enumAccept:"scalar" enumInteger:"passthrough"`, reflect.TypeFor[int64]())
	if err != nil {
		t.Fatal(err)
	}
	schema := EnumInputSchema(enumeration, false)
	encoded, err := json.Marshal(schema.AnyOf[0])
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]json.RawMessage
	if err = json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	if string(wire["maximum"]) != "9223372036854775807" {
		t.Fatalf("int64 upper boundary missing or rounded: %s", encoded)
	}
	if string(wire["minimum"]) != "-9223372036854775808" {
		t.Fatalf("minimum: %s", encoded)
	}
}
