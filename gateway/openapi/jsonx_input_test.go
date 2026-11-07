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

func TestJSONXConversionInputSchemas(t *testing.T) {
	type row struct {
		Enabled *bool    `json:"enabled" jsonx:"intToBool"`
		Catalog *string  `json:"catalog" jsonx:"numberToString"`
		Names   []string `json:"names" jsonx:"commaStringToSlice"`
		Billed  *string  `json:"billed" jsonx:"boolIntToYN"`
	}
	schemas := openapi3.Schemas{}
	ref, err := newSchemaBuilder(schemas).schema(reflect.TypeFor[row](), true)
	if err != nil {
		t.Fatal(err)
	}
	object := schemas[strings.TrimPrefix(ref.Ref, "#/components/schemas/")]
	for name, kinds := range map[string][]string{"enabled": {"boolean", "integer"}, "catalog": {"string", "number"}, "names": {"string", "array"}, "billed": {"boolean", "integer", "string"}} {
		property := object.Properties[name]
		for _, kind := range kinds {
			var branch *openapi3.Schema
			for _, candidate := range property.AnyOf {
				if candidate.Type == kind {
					branch = candidate
					break
				}
			}
			if branch == nil {
				t.Errorf("%s missing %s input branch: %+v", name, kind, property)
				continue
			}
			if kind == "array" && (branch.Items == nil || branch.Items.Type != "string") {
				t.Errorf("array lost item schema: %+v", branch)
			}
			if name == "enabled" && kind == "integer" && (len(branch.Enum) != 3 || branch.Enum[2] != nil) {
				t.Errorf("integer boolean must admit only 0/1: %+v", branch)
			}
			if name == "billed" && kind == "integer" && (branch.Min == nil || branch.Max == nil) {
				t.Errorf("integer range missing: %+v", branch)
			}
		}
	}
}

func TestJSONXNullOnlyNonpointerInputSchema(t *testing.T) {
	type row struct {
		Value float64 `json:"value" jsonx:"null=omit"`
	}
	schemas := openapi3.Schemas{}
	ref, err := newSchemaBuilder(schemas).schema(reflect.TypeFor[row](), true)
	if err != nil {
		t.Fatal(err)
	}
	property := schemas[strings.TrimPrefix(ref.Ref, "#/components/schemas/")].Properties["value"]
	if property.Type != "number" || !property.Nullable {
		t.Fatalf("null omission must preserve number and admit null: %+v", property)
	}
}

func TestJSONXNullOnlyObjectReference(t *testing.T) {
	type nested struct {
		Name string `json:"name"`
	}
	type row struct {
		Value nested `json:"value" jsonx:"null=omit"`
	}
	schemas := openapi3.Schemas{}
	ref, err := newSchemaBuilder(schemas).schema(reflect.TypeFor[row](), true)
	if err != nil {
		t.Fatal(err)
	}
	property := schemas[strings.TrimPrefix(ref.Ref, "#/components/schemas/")].Properties["value"]
	if len(property.AnyOf) != 2 || property.AnyOf[0].Ref == "" || !property.AnyOf[1].Nullable || len(property.AnyOf[1].Enum) != 1 || property.AnyOf[1].Enum[0] != nil {
		t.Fatalf("referenced object must admit explicit null without mutating object schema: %+v", property)
	}
}
