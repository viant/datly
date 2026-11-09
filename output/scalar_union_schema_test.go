package output

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/tagly/choice"
	"reflect"
	"testing"
)

func TestScalarUnionOutputSchema(t *testing.T) {
	type row struct {
		Fee *choice.ScalarValue `json:"fee" choice:"4237:AD_SERVING" choice:"4241:AD_SERVING" enumAccept:"scalar" enumStyle:"scalar" enumInteger:"passthrough" enumUnknownString:"preserve"`
	}
	field := reflect.TypeFor[row]().Field(0)
	schema := map[string]any{"type": "integer", "format": "int64", "description": "Fee type"}
	require.NoError(t, applyJSONSchemaChoices(schema, jsonSchemaField{field: field, name: "fee", nullable: true}))
	require.NotContains(t, schema, "type")
	require.NotContains(t, schema, "format")
	require.Equal(t, "Fee type", schema["description"])
	branches, ok := schema["anyOf"].([]any)
	require.True(t, ok, "scalar union must describe alternatives")
	require.Len(t, branches, 3)
}

func TestScalarUnionCompiledOutput(t *testing.T) {
	type fee struct {
		Value *choice.ScalarValue `json:"value" choice:"4237:AD_SERVING" choice:"4241:AD_SERVING" enumAccept:"scalar" enumStyle:"scalar" enumInteger:"passthrough" enumUnknownString:"preserve"`
	}
	type envelope struct {
		Rows  []*fee          `json:"rows"`
		Named map[string]*fee `json:"named"`
	}
	for _, format := range []string{"", "lc"} {
		plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[envelope](), Component: &spec.Component{Settings: &spec.Settings{CaseFormat: format}}})
		require.NoError(t, err)
		document, err := plan.JSONSchema()
		require.NoError(t, err)
		for _, value := range []int64{4237, 4241, 9007199254740993, 9223372036854775807, -9223372036854775808} {
			row := &fee{Value: &choice.ScalarValue{Integer: &value}}
			encoded, err := plan.Encode(context.Background(), "json", envelope{Rows: []*fee{row}, Named: map[string]*fee{"a": row}})
			require.NoError(t, err)
			assertJSONSchemaValid(t, document, encoded.Data)
			want := any(value)
			if value == 4237 || value == 4241 {
				want = "AD_SERVING"
			}
			wantJSON, err := json.Marshal(map[string]any{"rows": []any{map[string]any{"value": want}}, "named": map[string]any{"a": map[string]any{"value": want}}})
			require.NoError(t, err)
			require.JSONEq(t, string(wantJSON), string(encoded.Data))
			if value != 4237 && value != 4241 {
				exact, err := json.Marshal(value)
				require.NoError(t, err)
				require.Contains(t, string(encoded.Data), `"value":`+string(exact))
			}
		}
		for _, value := range []string{"AD_SERVING", "unknown", "4241"} {
			row := &fee{Value: &choice.ScalarValue{String: &value}}
			encoded, err := plan.Encode(context.Background(), "json", envelope{Rows: []*fee{row}, Named: map[string]*fee{"a": row}})
			require.NoError(t, err)
			assertJSONSchemaValid(t, document, encoded.Data)
			wantJSON, err := json.Marshal(map[string]any{"rows": []any{map[string]any{"value": value}}, "named": map[string]any{"a": map[string]any{"value": value}}})
			require.NoError(t, err)
			require.JSONEq(t, string(wantJSON), string(encoded.Data))
		}
		encoded, err := plan.Encode(context.Background(), "json", envelope{Rows: []*fee{{}}, Named: map[string]*fee{"a": {}}})
		require.NoError(t, err)
		assertJSONSchemaValid(t, document, encoded.Data)
		require.JSONEq(t, `{"rows":[{"value":null}],"named":{"a":{"value":null}}}`, string(encoded.Data))
		again, err := plan.JSONSchema()
		require.NoError(t, err)
		require.Equal(t, document, again)
	}
}

func TestScalarUnionClosedOutputAndInlineOverride(t *testing.T) {
	type closed struct {
		Value choice.ScalarValue `json:"value" choice:"4237:AD_SERVING" choice:"4241:AD_SERVING" enumAccept:"scalar" enumStyle:"scalar"`
	}
	field := reflect.TypeFor[closed]().Field(0)
	document := map[string]any{"type": "object", "properties": map[string]any{"Integer": map[string]any{"type": "integer"}}, "pattern": "storage-only", "required": []string{"Integer"}}
	require.NoError(t, applyJSONSchemaChoices(document, jsonSchemaField{field: field, name: "value"}))
	require.Equal(t, []any{map[string]any{"type": "string", "enum": []string{"AD_SERVING"}}, map[string]any{"type": "null"}}, document["anyOf"])
	require.NotContains(t, document, "properties")
	require.NotContains(t, document, "pattern")
	type overridden struct {
		Value  choice.ScalarValue `json:"value" choice:"4237:AD_SERVING" enumAccept:"scalar" enumStyle:"scalar"`
		Extras map[string]any     `format:"inline=true"`
	}
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[overridden](), Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}}})
	require.NoError(t, err)
	schema, err := plan.JSONSchema()
	require.NoError(t, err)
	require.Empty(t, schema["properties"].(map[string]any)["value"])
	encoded, err := plan.Encode(context.Background(), "json", overridden{Extras: map[string]any{"value": false}})
	require.NoError(t, err)
	assertJSONSchemaValid(t, schema, encoded.Data)
}
