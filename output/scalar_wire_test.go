package output

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xeipuuv/gojsonschema"
)

type authoredScalar struct{}

func (authoredScalar) MarshalJSON() ([]byte, error) { return []byte(`"2026-10-25T00:00:00Z"`), nil }
func TestOutputAuthoredScalarSchemaMatchesActualJSON(t *testing.T) {
	type row struct {
		When *authoredScalar `json:"when,omitempty" wireSchema:"{\"type\":[\"string\",\"integer\"]}"`
	}
	type result struct {
		Data []row `json:"data"`
	}
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[result]()})
	require.NoError(t, err)
	schema, err := plan.JSONSchema()
	require.NoError(t, err)
	validator, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(schema))
	require.NoError(t, err)
	for _, value := range []result{{Data: []row{{When: &authoredScalar{}}}}, {Data: []row{{}}}} {
		encoded, err := json.Marshal(value)
		require.NoError(t, err)
		checked, err := validator.Validate(gojsonschema.NewBytesLoader(encoded))
		require.NoError(t, err)
		require.True(t, checked.Valid(), checked.Errors())
	}
	checked, err := validator.Validate(gojsonschema.NewGoLoader(map[string]any{"data": []any{map[string]any{"when": map[string]any{}}}}))
	require.NoError(t, err)
	require.False(t, checked.Valid())
}
func TestNullableScalarUnionPreservesEveryDeclaredType(t *testing.T) {
	source := map[string]any{"type": []string{"string", "integer"}}
	require.Equal(t, []string{"string", "integer", "null"}, nullableJSONSchema(source)["type"])
	require.Equal(t, []string{"string", "integer", "null"}, nullableJSONSchema(source)["type"])
}
