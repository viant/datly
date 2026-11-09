package openapi

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/output"
	"github.com/viant/datly/spec"
	"github.com/viant/tagly/choice"
	"reflect"
	"strings"
	"testing"
)

func TestScalarUnionFieldSchema(t *testing.T) {
	type row struct {
		Fee choice.ScalarValue `choice:"4237:AD_SERVING" choice:"4241:AD_SERVING" enumAccept:"scalar" enumStyle:"scalar" enumInteger:"passthrough" enumUnknownString:"preserve"`
	}
	field := reflect.TypeFor[row]().Field(0)
	for _, input := range []bool{true, false} {
		original := &openapi3.Schema{Type: "object", Format: "storage", Properties: openapi3.Schemas{"Integer": {Type: "integer"}}, Required: []string{"Integer"}, Description: "Fee type"}
		b := newSchemaBuilder(openapi3.Schemas{})
		got, err := b.fieldEnum(original, field, false, input)
		require.NoError(t, err)
		require.Empty(t, got.Type)
		require.Empty(t, got.Format)
		require.Empty(t, got.Properties)
		require.Empty(t, got.Required)
		require.Equal(t, "Fee type", got.Description)
		require.Len(t, got.AnyOf, 3)
		encoded, err := json.Marshal(got)
		require.NoError(t, err)
		require.Contains(t, string(encoded), `"minimum":-9223372036854775808`)
		require.Contains(t, string(encoded), `"maximum":9223372036854775807`)
		require.Equal(t, "object", original.Type)
		require.Len(t, original.Properties, 1)
	}
}

func TestScalarUnionClosedInputOutputDomains(t *testing.T) {
	type row struct {
		Fee choice.ScalarValue `choice:"4237:AD_SERVING" choice:"4241:AD_SERVING" enumAccept:"scalar" enumStyle:"scalar"`
	}
	field := reflect.TypeFor[row]().Field(0)
	b := newSchemaBuilder(openapi3.Schemas{})
	request, err := b.fieldEnum(&openapi3.Schema{Type: "object"}, field, false, true)
	require.NoError(t, err)
	response, err := b.fieldEnum(&openapi3.Schema{Type: "object"}, field, false, false)
	require.NoError(t, err)
	require.Equal(t, "integer", request.AnyOf[0].Type)
	require.Equal(t, []any{json.Number("4237"), json.Number("4241")}, request.AnyOf[0].Enum)
	require.Len(t, response.AnyOf, 2)
	require.Equal(t, "string", response.AnyOf[0].Type)
	require.Equal(t, []any{"AD_SERVING"}, response.AnyOf[0].Enum)
	require.Equal(t, []any{nil}, response.AnyOf[1].Enum)
	again, err := b.fieldEnum(&openapi3.Schema{Type: "object"}, field, false, true)
	require.NoError(t, err)
	require.Equal(t, request, again)
}

func TestScalarUnionOpenAPIInlineOverride(t *testing.T) {
	type row struct {
		Fee    choice.ScalarValue `json:"fee" choice:"4237:AD_SERVING" enumAccept:"scalar" enumStyle:"scalar"`
		Extras map[string]any     `format:"inline=true"`
	}
	plan, err := (output.Compiler{}).Compile(output.CompileInput{Type: reflect.TypeFor[row](), Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}}})
	require.NoError(t, err)
	wire, err := plan.Wire("json")
	require.NoError(t, err)
	b := newSchemaBuilder(openapi3.Schemas{})
	root, err := b.wire(wire.JSON)
	require.NoError(t, err)
	root = b.schemas[strings.TrimPrefix(root.Ref, "#/components/schemas/")]
	require.NotNil(t, root)
	require.Empty(t, root.Properties["fee"].AnyOf)
	require.Empty(t, root.Properties["fee"].Enum)
	require.Empty(t, root.Properties["fee"].Type)
}
