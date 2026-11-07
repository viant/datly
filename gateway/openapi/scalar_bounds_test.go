package openapi

import (
	"encoding/json"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"reflect"
	"testing"
)

func TestScalarInt64OpenAPIBounds(t *testing.T) {
	field := reflect.TypeFor[struct {
		Value int64 `choice:"1:A" enumAccept:"scalar" enumInteger:"passthrough"`
	}]().Field(0)
	schema, err := (&schemaBuilder{}).fieldEnum(&openapi3.Schema{Type: "integer"}, field, false, true)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(schema.AnyOf[0])
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]json.RawMessage
	if err = json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if string(result["minimum"]) != "-9223372036854775808" || string(result["maximum"]) != "9223372036854775807" {
		t.Fatalf("rounded bounds: %s", data)
	}
}
