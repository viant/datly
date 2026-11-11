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

func TestObjectToSliceWholeBodyDiscovery(t *testing.T) {
	type record struct {
		CreativeID *int `json:"creativeId"`
	}
	type input struct {
		Body []record `parameter:"Body,kind=body" jsonx:"objectToSlice"`
	}
	contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "RecordCollection"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "recordCollection"}, Contract: contract})
	require.NoError(t, err)
	for _, tc := range []struct {
		raw   string
		valid bool
	}{
		{`{"body":{"creativeId":1}}`, true},
		{`{"body":[{"creativeId":1}]}`, true},
		{`{"body":[]}`, true},
		{`{"body":{"creativeId":"bad"}}`, false},
		{`{"body":[{"creativeId":"bad"}]}`, false},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(plan.Metadata().InputSchema), gojsonschema.NewStringLoader(tc.raw))
			require.NoError(t, err)
			require.Equal(t, tc.valid, result.Valid(), "%v", result.Errors())
			var args map[string]any
			require.NoError(t, json.Unmarshal([]byte(tc.raw), &args))
			scope, err := plan.Scope(args)
			require.NoError(t, err)
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
			if tc.raw == `{"body":[]}` {
				require.NotNil(t, actual.Body)
				require.Empty(t, actual.Body)
			} else {
				require.Len(t, actual.Body, 1)
				require.NotNil(t, actual.Body[0].CreativeID)
				require.Equal(t, 1, *actual.Body[0].CreativeID)
			}
		})
	}
}

func TestObjectToSliceScopeAndRequiredness(t *testing.T) {
	type row struct {
		ID *int `json:"id"`
	}
	type input struct {
		Body []row `jsonx:"objectToSlice"`
	}
	for _, selected := range []bool{false, true} {
		required := true
		location := bindstate.Location{Kind: "body"}
		if selected {
			location.In = "pairs"
		}
		contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{{Path: "Body", Name: "Body", Location: location, Required: &required}})
		plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Name: "scope"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "scope"}, Contract: contract})
		require.NoError(t, err)
		for _, tc := range []struct {
			raw   string
			valid bool
		}{
			{`{"body":null}`, false},
			{`{"body":{"id":1}}`, !selected},
			{`{"body":[{"id":1}]}`, true},
		} {
			result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(plan.Metadata().InputSchema), gojsonschema.NewStringLoader(tc.raw))
			require.NoError(t, err)
			require.Equal(t, tc.valid, result.Valid(), "selected=%v raw=%s %v", selected, tc.raw, result.Errors())
		}
		if !selected {
			result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(plan.Metadata().InputSchema), gojsonschema.NewStringLoader(`{"body":[]}`))
			require.NoError(t, err)
			require.False(t, result.Valid(), "required body rejects an empty collection at runtime")
		}
	}
}

func TestObjectToSliceSchemaPreservesRecordGraph(t *testing.T) {
	type detail struct {
		DisplayName string
	}
	type record struct {
		CreativeID *int `json:"creativeId"`
		Details    detail
		Hidden     string `internal:"true"`
	}
	type input struct {
		Pairs []record `json:"pairs" jsonx:"objectToSlice"`
	}
	projected, err := (&schemaProjector{nativeBody: true, caseFormat: "lc"}).argument(reflect.TypeFor[input](), "Body", "Body", nil)
	require.NoError(t, err)
	validator, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(projected))
	require.NoError(t, err)
	for _, tc := range []struct {
		payload string
		valid   bool
	}{
		{`{"pairs":{"creativeId":1,"details":{"displayName":"one"}}}`, true},
		{`{"pairs":[{"creativeId":1,"details":{"displayName":"one"}}]}`, true},
		{`{"pairs":[]}`, true},
		{`{"pairs":null}`, true},
		{`{"pairs":1}`, false},
		{`{"pairs":{"creativeId":"bad"}}`, false},
		{`{"pairs":[{"creativeId":"bad"}]}`, false},
		{`{"pairs":{"details":{"displayName":42}}}`, false},
		{`{"pairs":[{"details":{"displayName":42}}]}`, false},
	} {
		t.Run(tc.payload, func(t *testing.T) {
			result, err := validator.Validate(gojsonschema.NewStringLoader(tc.payload))
			require.NoError(t, err)
			require.Equal(t, tc.valid, result.Valid(), "%v", result.Errors())
		})
	}
}
