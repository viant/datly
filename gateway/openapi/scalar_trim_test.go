package openapi

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/xeipuuv/gojsonschema"
	"reflect"
	"testing"
)

func TestScalarTrimSchemaInputAndOutput(t *testing.T) {
	type input struct {
		Channel int `choice:"1:DISPLAY" choice:"2:VIDEO" enumAccept:"scalar" enumStyle:"ordinal" enumInteger:"passthrough" enumCase:"upper" enumAlias:"1:1" enumAlias:"2:OLV" enumTrim:"space"`
	}
	field := reflect.TypeFor[input]().Field(0)
	b := newSchemaBuilder(openapi3.Schemas{})
	schema, err := b.fieldEnum(&openapi3.Schema{Type: "integer"}, field, false, true)
	require.NoError(t, err)
	for _, tc := range []struct {
		raw   string
		valid bool
	}{
		{`" DISPLAY "`, true}, {`"\t1\r\n"`, true}, {`"\u0085\u00a0\u1680\u2000\u200a\u2028\u2029\u202f\u205f\u3000olv\n"`, true},
		{`"\u180eDISPLAY"`, false}, {`"DISPLAY\ufeff"`, false}, {`" 99 "`, false}, {`99`, true}, {`" "`, false}, {`true`, false},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			result, e := gojsonschema.Validate(gojsonschema.NewGoLoader(schema), gojsonschema.NewStringLoader(tc.raw))
			require.NoError(t, e)
			require.Equal(t, tc.valid, result.Valid(), "%v", result.Errors())
		})
	}
	output, err := b.fieldEnum(&openapi3.Schema{Type: "integer"}, field, false, false)
	require.NoError(t, err)
	require.Equal(t, "integer", output.Type)
	require.Empty(t, output.AnyOf)
	field.Tag = reflect.StructTag(`choice:"1:DISPLAY" enumAccept:"scalar" enumCase:"upper"`)
	strict, err := b.fieldEnum(&openapi3.Schema{Type: "integer"}, field, false, true)
	require.NoError(t, err)
	result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(strict), gojsonschema.NewStringLoader(`"DISPLAY\n"`))
	require.NoError(t, err)
	require.False(t, result.Valid())
}
