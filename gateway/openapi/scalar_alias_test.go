package openapi

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/xeipuuv/gojsonschema"
)

func TestScalarAliasInputSchemaMatchesAcceptedValues(t *testing.T) {
	type input struct {
		Mode string `choice:"0:ASAP" choice:"1:SPEND_EVENLY" enumAccept:"scalar" enumStyle:"literal" enumAlias:"0:0" enumAlias:"1:1" enumAlias:"1:Even" enumCase:"fold"`
	}
	field := reflect.TypeFor[input]().Field(0)
	builder := newSchemaBuilder(openapi3.Schemas{})
	schema, err := builder.fieldEnum(&openapi3.Schema{Type: "string"}, field, false, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		input string
		valid bool
	}{
		{`0`, true}, {`1`, true}, {`"0"`, true}, {`"1"`, true},
		{`"eVeN"`, true}, {`"ſPEND_EVENLY"`, true}, {`"ASAP"`, true},
		{`2`, false}, {`1.5`, false}, {`true`, false}, {`{}`, false},
		{`"unknown"`, false}, {`"Even\n"`, false}, {`null`, false},
	} {
		t.Run(tc.input, func(t *testing.T) {
			result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(schema), gojsonschema.NewStringLoader(tc.input))
			if err != nil {
				t.Fatal(err)
			}
			if result.Valid() != tc.valid {
				t.Fatalf("valid=%v want %v: %v", result.Valid(), tc.valid, result.Errors())
			}
		})
	}
}

func TestGoJayNumberInputAndLiteralOutput(t *testing.T) {
	type request struct {
		Mode string `choice:"0:ASAP" choice:"1:SPEND_EVENLY" enumAccept:"scalar" enumStyle:"literal" enumNumber:"gojayInt"`
	}
	field := reflect.TypeFor[request]().Field(0)
	builder := newSchemaBuilder(openapi3.Schemas{})
	for _, input := range []bool{true, false} {
		schema, err := builder.fieldEnum(&openapi3.Schema{Type: "string"}, field, false, input)
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			raw  string
			want bool
		}{{"1.5", input}, {"1e0", input}, {`"SPEND_EVENLY"`, true}, {"true", false}} {
			result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(schema), gojsonschema.NewStringLoader(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			if result.Valid() != tc.want {
				t.Fatalf("input=%t token=%s valid=%t want=%t", input, tc.raw, result.Valid(), tc.want)
			}
		}
		if input {
			documented := false
			for _, branch := range schema.AnyOf {
				if branch.Type == "number" && strings.Contains(branch.Description, "0") && strings.Contains(branch.Description, "1") {
					documented = true
					break
				}
			}
			if !documented {
				t.Fatal("numeric input must describe the accepted converted IDs")
			}
		}
	}
}
