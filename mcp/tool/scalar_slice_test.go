package tool

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"github.com/xeipuuv/gojsonschema"
)

func TestScalarQuerySliceDiscoveryAndAdmission(t *testing.T) {
	type input struct {
		IDs []int `mcp:"name=ids,aliases=legacy_ids,scalar=true"`
	}
	contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{{Path: "IDs", Name: "IDs", Location: bindstate.Location{Kind: "query", In: "ids"}}})
	plan, err := NewCompiler(WithStrictArguments(true)).Compile(Input{Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "ids"}, Contract: contract})
	require.NoError(t, err)
	property := plan.Metadata().InputSchema.Properties["ids"]
	alternatives, ok := property["anyOf"].([]any)
	require.True(t, ok, "%+v", property)
	require.Equal(t, "integer", alternatives[0].(map[string]any)["type"])
	for _, tc := range []struct {
		args  map[string]any
		valid bool
	}{
		{map[string]any{"ids": 7}, true}, {map[string]any{"ids": 0}, true},
		{map[string]any{"ids": []int{7, 9}}, true}, {map[string]any{"ids": []int{}}, true}, {map[string]any{}, true}, {map[string]any{"ids": nil}, true},
		{map[string]any{"ids": "7"}, false}, {map[string]any{"ids": true}, false}, {map[string]any{"ids": 1.5}, false},
		{map[string]any{"ids": []any{7, "9"}}, false}, {map[string]any{"undeclared": 7}, false},
	} {
		result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(plan.Metadata().InputSchema), gojsonschema.NewGoLoader(tc.args))
		require.NoError(t, err)
		require.Equal(t, tc.valid, result.Valid(), "%+v: %v", tc.args, result.Errors())
		scope, err := plan.Scope(tc.args)
		if tc.valid {
			require.NoError(t, err)
			scope.Close()
		} else {
			require.Error(t, err)
		}
	}
	for _, args := range []map[string]any{{"legacy_ids": 7}, {"ids": nil}} {
		scope, err := plan.Scope(args)
		require.NoError(t, err)
		scope.Close()
	}
	_, err = plan.Scope(map[string]any{"ids": 7, "legacy_ids": []int{7}})
	require.ErrorContains(t, err, "conflicting")
}

func TestScalarQuerySliceMetadataValidation(t *testing.T) {
	for _, tag := range []reflect.StructTag{`mcp:"scalar=yes"`, `mcp:"scalar"`, `mcp:"scalar=true,scalar=false"`} {
		_, err := scalarAdmission(reflect.StructField{Name: "IDs", Tag: tag})
		if tag == `mcp:"scalar"` {
			// Positional MCP names retain naming authority.
			require.NoError(t, err)
			continue
		}
		require.Error(t, err)
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[int](), reflect.TypeFor[[]string](), reflect.TypeFor[[]float64]()} {
		input := reflect.StructOf([]reflect.StructField{{Name: "IDs", Type: typ, Tag: `mcp:"scalar=true"`}})
		contract := testRouteContract(t, input, []bindly.BindingSpec{{Path: "IDs", Name: "IDs", Location: bindstate.Location{Kind: "query", In: "ids"}}})
		_, err := NewCompiler().Compile(Input{Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "ids"}, Contract: contract})
		require.Error(t, err)
	}
}

func TestScalarQuerySliceFalsePreservesArrayContract(t *testing.T) {
	type input struct {
		IDs []int `mcp:"name=ids,scalar=false"`
	}
	contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{{Path: "IDs", Name: "IDs", Location: bindstate.Location{Kind: "query", In: "ids"}}})
	plan, err := NewCompiler().Compile(Input{Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "ids"}, Contract: contract})
	require.NoError(t, err)
	require.Equal(t, "array", plan.Metadata().InputSchema.Properties["ids"]["type"])
	require.NotContains(t, plan.Metadata().InputSchema.Properties["ids"], "anyOf")
	_, err = plan.Scope(map[string]any{"ids": 7})
	require.Error(t, err)
	scope, err := plan.Scope(map[string]any{"ids": []int{7}})
	require.NoError(t, err)
	scope.Close()
}

func TestScalarQuerySliceRejectsExplicitWireRepresentation(t *testing.T) {
	type input struct {
		IDs []int `mcp:"name=ids,scalar=true"`
	}
	for _, param := range []*spec.Parameter{
		{WireSchema: &spec.WireSchema{Type: "string"}},
		{WireSchemas: map[string]*spec.WireSchema{"IDs": {Type: "integer"}}},
	} {
		contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{{Path: "IDs", Name: "IDs", Location: bindstate.Location{Kind: "query", In: "ids"}, Extension: param}})
		_, err := NewCompiler().Compile(Input{Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "ids"}, Contract: contract})
		require.ErrorContains(t, err, "conflicts with explicit wire representation")
	}
}

func TestScalarQuerySliceRequiredDiscoveryRejectsNull(t *testing.T) {
	type input struct {
		IDs []int `mcp:"name=ids,scalar=true"`
	}
	required := true
	contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{{Path: "IDs", Name: "IDs", Location: bindstate.Location{Kind: "query", In: "ids"}, Required: &required}})
	plan, err := NewCompiler().Compile(Input{Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "ids"}, Contract: contract})
	require.NoError(t, err)
	for _, args := range []map[string]any{{}, {"ids": nil}, {"ids": ""}} {
		result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(plan.Metadata().InputSchema), gojsonschema.NewGoLoader(args))
		require.NoError(t, err)
		require.False(t, result.Valid(), "%+v", args)
	}
	_, err = plan.Scope(map[string]any{})
	require.ErrorContains(t, err, "missing required MCP argument")
	// Supplied query null retains native omission semantics. The actual
	// required binding owns its rejection; see input's native binder regression.
	scope, err := plan.Scope(map[string]any{"ids": nil})
	require.NoError(t, err)
	scope.Close()
}
