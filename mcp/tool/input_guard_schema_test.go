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

type guardedToolChild struct {
	Value *int                  `json:"value" format:"inputUnless=../Canonical" jsonx:"boolToInt"`
	Has   *struct{ Value bool } `json:"-" setMarker:"true"`
}
type guardedToolBody struct {
	Canonical *int                            `json:"canonical"`
	Rows      []*guardedToolChild             `json:"rows"`
	Has       *struct{ Canonical, Rows bool } `json:"-" setMarker:"true"`
}

func TestNativeInputGuardToolBindingAndSchema(t *testing.T) {
	type input struct {
		Body *guardedToolBody `parameter:"Body,kind=body"`
	}
	binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}
	contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{binding})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "Guard"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "guard"}, Contract: contract})
	require.NoError(t, err)
	for _, canonical := range []any{nil, 2} {
		scope, err := plan.Scope(map[string]any{"body": map[string]any{"canonical": canonical, "rows": []any{map[string]any{"value": "ignored", "Has": map[string]any{"Value": true}}}}})
		require.NoError(t, err)
		value, found, err := scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[*guardedToolBody](), "")
		require.NoError(t, err)
		require.True(t, found)
		actual := value.(*guardedToolBody)
		require.True(t, actual.Has.Canonical)
		require.Nil(t, actual.Rows[0].Value)
		require.False(t, actual.Rows[0].Has.Value)
		scope.Close()
	}
	projected, err := (&schemaProjector{nativeBody: true, schemas: map[string]*spec.WireSchema{"Body.rows.value": {Type: "integer", Required: true}}}).argument(reflect.TypeFor[guardedToolBody](), "Body", "Body", nil)
	require.NoError(t, err)
	validator, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(projected))
	require.NoError(t, err)
	result, err := validator.Validate(gojsonschema.NewStringLoader(`{"canonical":null,"rows":[{"value":"ignored"}]}`))
	require.NoError(t, err)
	require.True(t, result.Valid(), "%v", result.Errors())
	generic, err := (&schemaProjector{}).argument(reflect.TypeFor[guardedToolBody](), "Body", "Body", nil)
	require.NoError(t, err)
	validator, err = gojsonschema.NewSchema(gojsonschema.NewGoLoader(generic))
	require.NoError(t, err)
	result, err = validator.Validate(gojsonschema.NewStringLoader(`{"canonical":2,"rows":[{"value":"ignored"}]}`))
	require.NoError(t, err)
	require.False(t, result.Valid())
}
