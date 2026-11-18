package tool

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"github.com/xeipuuv/gojsonschema"
)

func TestWrapperWholeBodyDiscoveryAndBinding(t *testing.T) {
	type input struct {
		Body []int `jsonx:"wrapper=ids"`
	}
	for _, required := range []bool{false, true} {
		t.Run(map[bool]string{false: "optional", true: "required"}[required], func(t *testing.T) {
			contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}, Required: &required}})
			plan, err := NewCompiler().Compile(Input{Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "ids"}})
			require.NoError(t, err)
			validator, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(plan.Metadata().InputSchema))
			require.NoError(t, err)
			for _, tc := range []struct {
				raw   string
				want  []int
				valid bool
			}{
				{`{}`, nil, !required}, {`{"body":null}`, nil, !required}, {`{"body":[]}`, []int{}, !required},
				{`{"body":[1,2]}`, []int{1, 2}, true}, {`{"body":{"ids":[1,2]}}`, []int{1, 2}, true},
				{`{"body":{"ids":[]}}`, []int{}, !required},
				{`{"body":{}}`, nil, false}, {`{"body":{"ids":null}}`, nil, false},
				{`{"body":{"ids":[1],"other":2}}`, nil, false}, {`{"body":{"other":[1]}}`, nil, false},
				{`{"body":{"ids":[1.5]}}`, nil, false}, {`{"body":{"ids":["1"]}}`, nil, false},
			} {
				t.Run(tc.raw, func(t *testing.T) {
					result, err := validator.Validate(gojsonschema.NewStringLoader(tc.raw))
					require.NoError(t, err)
					require.Equal(t, tc.valid, result.Valid(), "%v", result.Errors())
					var args map[string]any
					require.NoError(t, json.Unmarshal([]byte(tc.raw), &args))
					scope, err := plan.Scope(args)
					if err != nil {
						require.False(t, tc.valid)
						return
					}
					defer scope.Close()
					injector, err := bindly.NewInjector()
					require.NoError(t, err)
					bound, err := injector.ForScope(scope.Providers()...)
					require.NoError(t, err)
					var actual input
					err = bound.Bind(t.Context(), &actual, bindly.WithPlan(contract.Plan()))
					if !tc.valid {
						require.Error(t, err)
						return
					}
					require.NoError(t, err)
					require.Equal(t, tc.want, actual.Body)
				})
			}
		})
	}
}

func TestWrapperNestedSchemaPreservesBoundsAndOutput(t *testing.T) {
	type body struct {
		IDs []int `json:"ids" jsonx:"wrapper=values"`
	}
	projected, err := (&schemaProjector{nativeBody: true, schemas: map[string]*spec.WireSchema{"Body.ids": {MinItems: 1, MaxItems: 2}}}).argument(reflect.TypeFor[body](), "Body", "Body", nil)
	require.NoError(t, err)
	validator, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(projected))
	require.NoError(t, err)
	for _, tc := range []struct {
		raw   string
		valid bool
	}{
		{`{"ids":[1]}`, true}, {`{"ids":{"values":[1,2]}}`, true},
		{`{"ids":[]}`, false}, {`{"ids":{"values":[]}}`, false},
		{`{"ids":[1,2,3]}`, false}, {`{"ids":{"values":[1,2,3]}}`, false},
		{`{"ids":{"values":null}}`, false}, {`{"ids":{"values":[1],"other":[]}}`, false},
	} {
		result, err := validator.Validate(gojsonschema.NewStringLoader(tc.raw))
		require.NoError(t, err)
		require.Equal(t, tc.valid, result.Valid(), "%s: %v", tc.raw, result.Errors())
	}
	output, err := (&schemaProjector{}).argument(reflect.TypeFor[body](), "Body", "Body", nil)
	require.NoError(t, err)
	encoded, err := json.Marshal(output)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), `"values"`, "input wrapper must not alter output")
}
