package openapi

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/state"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestObjectToSliceExplicitBindingPolicy(t *testing.T) {
	type row struct {
		CreativeID *int `json:"creativeId"`
	}
	type input struct{ Body []row }
	injector, err := bindly.NewInjector()
	require.NoError(t, err)
	binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: state.Location{Kind: "body"}, BodyInputTag: `jsonx:"objectToSlice"`}
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
	schemas := openapi3.Schemas{}
	builder := inputBuilder{schemas: newSchemaBuilder(schemas), template: template, endpoint: &spec.Route{Method: ref.Method, Path: ref.Path}}
	operation := &openapi3.Operation{}
	require.NoError(t, builder.build(effective.Fields(), operation))
	schema := operation.RequestBody.Content["application/json"].Schema
	require.GreaterOrEqual(t, len(schema.AnyOf), 2, "explicit binding policy must advertise object and array")
}
