package openapi

import (
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/xeipuuv/gojsonschema"
	"reflect"
	"testing"
)

func TestOpenScalarRequestAndResponseSchemas(t *testing.T) {
	type row struct {
		Mode int8 `choice:"1:A" enumAccept:"scalar" enumInteger:"passthrough" enumUnknownString:"0"`
	}
	f := reflect.TypeFor[row]().Field(0)
	builder := newSchemaBuilder(openapi3.Schemas{})
	for _, input := range []bool{false, true} {
		schema, err := builder.fieldEnum(&openapi3.Schema{Type: "integer"}, f, false, input)
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			raw   string
			valid bool
		}{{`3`, true}, {`-1`, true}, {`"anything"`, input}, {`true`, false}, {`[]`, false}} {
			r, err := gojsonschema.Validate(gojsonschema.NewGoLoader(schema), gojsonschema.NewStringLoader(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			if r.Valid() != tc.valid {
				t.Fatalf("input=%v raw=%s valid=%v: %v", input, tc.raw, r.Valid(), r.Errors())
			}
		}
	}
}
