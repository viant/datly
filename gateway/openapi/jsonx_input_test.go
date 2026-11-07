package openapi

import (
	"github.com/viant/datly/gateway/openapi/openapi3"
	"reflect"
	"strings"
	"testing"
)

type jsonxSchemaRow struct {
	Flag   *int `json:"flag" jsonx:"boolToInt,null=omit"`
	Plain  *int `json:"plain"`
	Hidden *int `json:"hidden" internal:"true" jsonx:"boolToInt"`
}

func TestJSONXInputSchemaAlternatives(t *testing.T) {
	for _, input := range []bool{true, false} {
		schemas := openapi3.Schemas{}
		b := newSchemaBuilder(schemas)
		ref, err := b.schema(reflect.TypeFor[jsonxSchemaRow](), input)
		if err != nil {
			t.Fatal(err)
		}
		object := schemas[strings.TrimPrefix(ref.Ref, "#/components/schemas/")]
		flag := object.Properties["flag"]
		if input {
			if len(flag.AnyOf) != 2 || flag.AnyOf[0].Type != "integer" || !flag.AnyOf[0].Nullable || flag.AnyOf[1].Type != "boolean" {
				t.Fatalf("missing input alternatives: %+v", flag)
			}
			if object.Properties["hidden"] != nil {
				t.Fatal("internal input exposed")
			}
		} else if flag.Type != "integer" || !flag.Nullable || len(flag.AnyOf) != 0 {
			t.Fatalf("output changed: %+v", flag)
		}
		plain := object.Properties["plain"]
		if plain.Type != "integer" || !plain.Nullable || len(plain.AnyOf) != 0 {
			t.Fatalf("untagged changed: %+v", plain)
		}
	}
}
