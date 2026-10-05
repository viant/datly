package tool

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/runtime/output"
	"github.com/viant/datly/spec"
)

func TestOutputSchemaUsesCompiledPlan(t *testing.T) {
	type summary struct {
		PageCount   *int
		RecordCount int
	}
	plan, err := (output.Compiler{}).Compile(output.CompileInput{
		Type: reflect.TypeFor[summary](), Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}},
	})
	require.NoError(t, err)
	result, err := outputContractSchema(reflect.TypeFor[struct{ WrongField string }](), plan)
	require.NoError(t, err)
	require.NotNil(t, result)
	data, err := json.Marshal(result)
	require.NoError(t, err)
	require.Contains(t, string(data), `"recordCount"`)
	require.NotContains(t, string(data), `"RecordCount"`)
	require.NotContains(t, string(data), `"WrongField"`)
}

func TestOutputSchemaDoesNotInventCollisionMapping(t *testing.T) {
	type row struct {
		SampleSeenDay  int
		SampleSeen_Day int
	}
	plan, err := (output.Compiler{}).Compile(output.CompileInput{
		Type: reflect.TypeFor[row](), Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}},
	})
	require.NoError(t, err)
	result, err := outputContractSchema(reflect.TypeFor[row](), plan)
	require.NoError(t, err, "optional discovery schema must not block existing execution")
	require.Nil(t, result, "neither raw Go names nor an arbitrary colliding field describe the wire contract")
}

func TestOutputSchemaPreservesNumericNames(t *testing.T) {
	type row struct {
		SampleSeen_1Day int
		SampleSeen_7Day int
	}
	plan, err := (output.Compiler{}).Compile(output.CompileInput{
		Type: reflect.TypeFor[row](), Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}},
	})
	require.NoError(t, err)
	result, err := outputContractSchema(reflect.TypeFor[row](), plan)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Properties, 2)
	require.Contains(t, result.Properties, "sampleSeen1Day")
	require.Contains(t, result.Properties, "sampleSeen7Day")
	require.ElementsMatch(t, []string{"sampleSeen1Day", "sampleSeen7Day"}, result.Required)
}
