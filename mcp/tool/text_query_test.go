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

func TestTextQueryDiscoveryAndAdmission(t *testing.T) {
	for _, scalar := range []bool{false, true} {
		tag := `mcp:"name=ids,aliases=legacy_ids,text=true"`
		if scalar {
			tag = `mcp:"name=ids,text=true,scalar=true"`
		}
		input := reflect.StructOf([]reflect.StructField{{Name: "IDs", Type: reflect.TypeFor[[]int](), Tag: reflect.StructTag(tag)}})
		contract := testRouteContract(t, input, []bindly.BindingSpec{{Path: "IDs", Name: "IDs", Location: bindstate.Location{Kind: "query", In: "id"}}})
		plan, err := NewCompiler(WithStrictArguments(true)).Compile(Input{Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "ids"}, Contract: contract})
		require.NoError(t, err)
		require.Equal(t, reflect.TypeFor[[]int](), plan.Arguments()[0].SourceType())
		require.Equal(t, "id", plan.Arguments()[0].SourceName())
		for _, tc := range []struct {
			value any
			valid bool
		}{{[]any{7, "050", "+50", "1e2"}, true}, {[]int{}, true}, {nil, true}, {7, scalar}, {"50", scalar}, {[]any{true}, false}, {[]any{1.5}, false}, {[]any{nil}, false}, {map[string]any{}, false}} {
			args := map[string]any{"ids": tc.value}
			result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(plan.Metadata().InputSchema), gojsonschema.NewGoLoader(args))
			require.NoError(t, err)
			require.Equal(t, tc.valid, result.Valid(), "%#v %v", args, result.Errors())
			scope, err := plan.Scope(args)
			if tc.valid {
				require.NoError(t, err)
				scope.Close()
			} else {
				require.Error(t, err)
			}
		}
	}
}

func TestTextQueryMetadataValidation(t *testing.T) {
	for _, tag := range []reflect.StructTag{`mcp:"text=yes"`, `mcp:"text=true,text=false"`} {
		_, err := textAdmission(reflect.StructField{Name: "IDs", Tag: tag})
		require.Error(t, err)
	}
	for _, tag := range []reflect.StructTag{`mcp:"text"`, `mcp:"name=ids,text=false"`} {
		enabled, err := textAdmission(reflect.StructField{Name: "IDs", Tag: tag})
		require.NoError(t, err)
		require.False(t, enabled)
	}
	type input struct {
		IDs []int `mcp:"text=true"`
	}
	for _, kind := range []string{"path", "body", "form"} {
		contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{{Path: "IDs", Name: "IDs", Location: bindstate.Location{Kind: kind, In: "id"}}})
		_, err := NewCompiler().Compile(Input{Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "ids"}, Contract: contract})
		require.Error(t, err)
	}
	contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{{Path: "IDs", Name: "IDs", Location: bindstate.Location{Kind: "query", In: "id"}, Extension: &spec.Parameter{WireSchema: &spec.WireSchema{Type: "string"}}}})
	_, err := NewCompiler().Compile(Input{Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "ids"}, Contract: contract})
	require.ErrorContains(t, err, "conflicts with explicit wire representation")
}

func TestTextQueryAdmissionPreservesDefault(t *testing.T) {
	for _, tag := range []reflect.StructTag{`mcp:"name=ids"`, `mcp:"name=ids,text=false"`} {
		typ := reflect.StructOf([]reflect.StructField{{Name: "IDs", Type: reflect.TypeFor[[]int](), Tag: tag}})
		contract := testRouteContract(t, typ, []bindly.BindingSpec{{Path: "IDs", Name: "IDs", Location: bindstate.Location{Kind: "query", In: "id"}}})
		plan, err := NewCompiler().Compile(Input{Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "ids"}, Contract: contract})
		require.NoError(t, err)
		require.Equal(t, "array", plan.Metadata().InputSchema.Properties["ids"]["type"])
		require.Equal(t, "integer", plan.Metadata().InputSchema.Properties["ids"]["items"].(map[string]any)["type"])
		_, err = plan.Scope(map[string]any{"ids": []any{7, "9"}})
		require.Error(t, err)
	}
}

func TestTextQueryAdmissionPreservesNullablePolicy(t *testing.T) {
	for _, nullable := range []string{"true", "false"} {
		tag := reflect.StructTag(`mcp:"name=ids,text=true" format:"nullable=` + nullable + `"`)
		typ := reflect.StructOf([]reflect.StructField{{Name: "IDs", Type: reflect.TypeFor[[]int](), Tag: tag}})
		contract := testRouteContract(t, typ, []bindly.BindingSpec{{Path: "IDs", Name: "IDs", Location: bindstate.Location{Kind: "query", In: "id"}}})
		plan, err := NewCompiler().Compile(Input{Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "ids"}, Contract: contract})
		require.NoError(t, err)
		result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(plan.Metadata().InputSchema), gojsonschema.NewGoLoader(map[string]any{"ids": nil}))
		require.NoError(t, err)
		require.Equal(t, nullable == "true", result.Valid())
		scope, err := plan.Scope(map[string]any{"ids": nil})
		if nullable == "false" {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			scope.Close()
		}
	}
}
