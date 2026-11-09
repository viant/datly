package tool

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"github.com/xeipuuv/gojsonschema"
	"reflect"
	"testing"
)

type groupedInputSchemaBody struct {
	Low  *int `json:"low" format:"inputGroup=source,inputPriority=0,inputCase=exact" jsonx:"boolToInt" required:"true"`
	High *int `json:"high" format:"inputGroup=source,inputPriority=1,inputCase=exact"`
}

func TestNativeInputGroupSchemaDoesNotValidateLosingSources(t *testing.T) {
	projected, err := (&schemaProjector{nativeBody: true, schemas: map[string]*spec.WireSchema{"Body.low": {Type: "integer", Required: true}}}).argument(reflect.TypeFor[groupedInputSchemaBody](), "Body", "Body", nil)
	require.NoError(t, err)
	validator, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(projected))
	require.NoError(t, err)
	for _, raw := range []string{`{"high":2}`, `{"low":"ignored","high":2}`, `{"low":null,"high":2}`} {
		result, err := validator.Validate(gojsonschema.NewStringLoader(raw))
		require.NoError(t, err)
		require.True(t, result.Valid(), "%s %v", raw, result.Errors())
	}
	_, low, _ := schemaMember(projected, []string{"low"})
	require.Contains(t, low["description"], "runtime")
	generic, err := (&schemaProjector{}).argument(reflect.TypeFor[groupedInputSchemaBody](), "Body", "Body", nil)
	require.NoError(t, err)
	ordinary, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(generic))
	require.NoError(t, err)
	result, err := ordinary.Validate(gojsonschema.NewStringLoader(`{"low":"invalid","high":2}`))
	require.NoError(t, err)
	require.False(t, result.Valid())
}

type groupedNativeBody struct {
	Low  *int                      `json:"low" format:"inputGroup=source,inputPriority=0,inputCase=exact" jsonx:"boolToInt"`
	High *int                      `json:"high" format:"inputGroup=source,inputPriority=1,inputCase=exact"`
	Has  *struct{ Low, High bool } `json:"-" setMarker:"true"`
}

func TestNativeInputGroupToolBinding(t *testing.T) {
	type input struct {
		Body *groupedNativeBody `parameter:"Body,kind=body"`
	}
	binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}
	contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{binding})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "InputGroup"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "inputGroup"}, Contract: contract})
	require.NoError(t, err)
	for _, tc := range []struct {
		data      map[string]any
		low, high bool
		value     int
		null      bool
	}{
		{map[string]any{"low": "ignored", "high": 2}, false, true, 2, false},
		{map[string]any{"low": "ignored", "high": nil}, false, true, 0, true},
		{map[string]any{"low": true, "HIGH": 2}, true, false, 1, false},
	} {
		scope, err := plan.Scope(map[string]any{"Body": tc.data})
		require.NoError(t, err)
		defer scope.Close()
		value, found, err := scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[*groupedNativeBody](), "")
		require.NoError(t, err)
		require.True(t, found)
		actual := value.(*groupedNativeBody)
		require.NotNil(t, actual.Has)
		require.Equal(t, tc.low, actual.Has.Low)
		require.Equal(t, tc.high, actual.Has.High)
		if tc.null {
			require.Nil(t, actual.High)
		} else if tc.high {
			require.Equal(t, tc.value, *actual.High)
		} else {
			require.Equal(t, tc.value, *actual.Low)
		}
	}
}

func TestNativeInputGroupNestedToolBinding(t *testing.T) {
	type body struct {
		Rows   []*groupedNativeBody          `json:"rows"`
		ByName map[string]*groupedNativeBody `json:"byName"`
	}
	type input struct {
		Body *body `parameter:"Body,kind=body"`
	}
	binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}
	contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{binding})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "NestedInputGroup"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "nestedInputGroup"}, Contract: contract})
	require.NoError(t, err)
	winner := map[string]any{"low": "ignored", "high": 2}
	scope, err := plan.Scope(map[string]any{"Body": map[string]any{"rows": []any{winner}, "byName": map[string]any{"x": winner}}})
	require.NoError(t, err)
	defer scope.Close()
	value, found, err := scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[*body](), "")
	require.NoError(t, err)
	require.True(t, found)
	actual := value.(*body)
	require.Len(t, actual.Rows, 1)
	require.Len(t, actual.ByName, 1)
	for _, row := range []*groupedNativeBody{actual.Rows[0], actual.ByName["x"]} {
		require.NotNil(t, row.Has)
		require.False(t, row.Has.Low)
		require.True(t, row.Has.High)
		require.Nil(t, row.Low)
		require.Equal(t, 2, *row.High)
	}
}
