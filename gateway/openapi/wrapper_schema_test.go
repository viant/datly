package openapi

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/state"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
)

func TestWrapperExplicitBindingPolicy(t *testing.T) {
	type input struct{ Body []int }
	injector, err := bindly.NewInjector()
	require.NoError(t, err)
	binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: state.Location{Kind: "body"}, BodyInputTag: `jsonx:"wrapper=keys"`}
	typ := reflect.TypeFor[input]()
	plan, err := injector.CompilePlan(typ, binding)
	require.NoError(t, err)
	projection, err := plan.Projection()
	require.NoError(t, err)
	ref := spec.RouteRef{Method: "POST", Path: "/records"}
	contract, err := registry.NewInputContract(typ, projection, registry.RouteInput{Route: ref, Plan: plan, Bindings: []bindly.BindingSpec{binding}})
	require.NoError(t, err)
	effective, _ := contract.ForRoute(ref)
	template, err := route.CompilePathTemplate(ref.Path)
	require.NoError(t, err)
	builder := inputBuilder{schemas: newSchemaBuilder(openapi3.Schemas{}), template: template, endpoint: &spec.Route{Method: ref.Method, Path: ref.Path}}
	operation := &openapi3.Operation{}
	require.NoError(t, builder.build(effective.Fields(), operation))
	var object *openapi3.Schema
	for _, branch := range operation.RequestBody.Content["application/json"].Schema.AnyOf {
		if branch.Type == "object" {
			object = branch
		}
	}
	require.NotNil(t, object)
	require.NotNil(t, object.Properties["keys"])
	require.Nil(t, object.Properties["ids"], "member must come from declared binding, not application convention")
}

func TestWrapperHTTPSchemaBoundsRemainIndependent(t *testing.T) {
	max := uint64(2)
	original := &openapi3.Schema{Type: "array", Nullable: true, MinItems: 1, MaxItems: &max, Items: &openapi3.Schema{Type: "integer"}}
	field := reflect.StructField{Type: reflect.TypeFor[[]int](), Tag: `jsonx:"wrapper=ids"`}
	projected, err := newSchemaBuilder(openapi3.Schemas{}).jsonxInputSchema(field, original)
	require.NoError(t, err)
	strict := strictInputValue(projected, true, "ids")
	for _, schema := range []*openapi3.Schema{projected, strict} {
		for _, branch := range schema.AnyOf {
			if branch.Type == "object" {
				branch = branch.Properties["ids"]
				require.False(t, branch.Nullable)
			}
			require.EqualValues(t, 1, branch.MinItems)
			require.EqualValues(t, 2, *branch.MaxItems)
			require.NotNil(t, branch.Items)
		}
	}
	require.True(t, original.Nullable, "projecting required union must not mutate source schema")
	require.False(t, original.Items.Nullable, "item null admission must not mutate source items")
}
