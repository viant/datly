package compiler

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestScalarDiscoverySurvivesRouteActivationAndOptionalProjection(t *testing.T) {
	type input struct {
		Shared string
		ItemID int
	}
	for _, describe := range []bool{false, true} {
		shared := &spec.Parameter{Name: "Shared", TypeExpr: "*string", OutputTypeExpr: "string", Source: spec.BindSource{Kind: "query", Name: "q"}}
		id := &spec.Parameter{Name: "ItemID", TypeExpr: "*int", OutputTypeExpr: "int", Source: spec.BindSource{Kind: "path", Name: "id"}, Activation: &spec.RouteActivation{URI: "/items/{id}"}}
		c := &spec.Component{Parameters: []*spec.Parameter{shared, id}, Routes: []*spec.Route{{Method: "GET", Path: "/items"}, {Method: "GET", Path: "/items/{id}"}}}
		compiler := New(Input{Component: c, InputType: reflect.TypeFor[input]()})
		var result *Result
		var err error
		if describe {
			result, err = compiler.Describe()
		} else {
			result, err = compiler.Compile()
		}
		require.NoError(t, err)
		require.Same(t, shared, result.Bindings[0].Extension)
		require.Same(t, id, result.Bindings[1].Extension)
		require.Nil(t, shared.WireSchema)
		require.Nil(t, id.WireSchema)
		require.Nil(t, id.Required)
		collection, ok := result.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/items"})
		require.True(t, ok)
		require.Len(t, collection.Fields(), 1)
		require.Equal(t, &spec.WireSchema{Type: "string", Nullable: true}, collection.Fields()[0].WireSchema())
		detail, ok := result.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/items/{id}"})
		require.True(t, ok)
		require.Len(t, detail.Fields(), 2)
		require.Equal(t, &spec.WireSchema{Type: "integer", Nullable: true}, detail.Fields()[1].WireSchema())
		projected, err := detail.WithOptionalBindings("ItemID")
		require.NoError(t, err)
		require.Equal(t, detail.Fields()[1].WireSchema(), projected.Fields()[1].WireSchema())
		require.False(t, *projected.Fields()[1].Binding().Required)
		require.Nil(t, id.Required)
		schema := projected.Fields()[1].WireSchema()
		schema.Nullable = false
		require.True(t, projected.Fields()[1].WireSchema().Nullable)
		require.True(t, detail.Fields()[1].WireSchema().Nullable)
	}
}
