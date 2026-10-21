package selectorcompanion_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	"github.com/viant/datly/internal/selectorcompanion"
	"github.com/viant/datly/internal/selectorcompanion/model"
	compiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
	"github.com/viant/x"
	"github.com/viant/xdatly"
)

type scalarSelectorName string
type scalarSelectorInput struct {
	ID     int                `parameter:"ID,kind=query,in=id,required=false"`
	Active *bool              `parameter:",kind=query,in=active,dataType=bool"`
	Name   scalarSelectorName `parameter:",kind=query,in=name"`
}
type scalarSelectorSide struct {
	Fields []string `parameter:"Fields,kind=query,in=fields,required=false" querySelector:"view=rows,property=fields"`
	Page   int      `parameter:"Page,kind=query,in=page,required=false" querySelector:"view=rows,property=page"`
}
type scalarSelectorHolder struct {
	Contract xdatly.Component[scalarSelectorInput, fixture.Output] `component:"ReadRecords,path=/selector-records,method=GET"`
}

func (scalarSelectorHolder) DatlyQuerySelectors() scalarSelectorSide {
	panic("declaration must not execute")
}

func TestSelectorCompanionExplicitScalarWireKeepsCanonicalIdentity(t *testing.T) {
	for _, describe := range []bool{false, true} {
		t.Run(fmt.Sprint(describe), func(t *testing.T) {
			c := runtimeComponent(t)
			association, err := model.New(x.NewType(reflect.TypeFor[scalarSelectorHolder]()), "Contract", x.NewType(reflect.TypeFor[scalarSelectorSide]()))
			require.NoError(t, err)
			c.SelectorCompanion = association
			c.Key.Scope = reflect.TypeFor[scalarSelectorHolder]().PkgPath()
			c.Parameters[0].TypeExpr = "*int"
			c.Parameters[0].OutputTypeExpr = "int"
			active := &spec.Parameter{Name: "Active", TypeExpr: "bool", OutputTypeExpr: "*bool", Source: spec.BindSource{Kind: "query", Name: "active"}}
			name := &spec.Parameter{Name: "Name", TypeExpr: "*alias.Name", OutputTypeExpr: "alias.Name", Source: spec.BindSource{Kind: "query", Name: "name"}}
			c.Parameters = append(c.Parameters[:1], append([]*spec.Parameter{active, name}, c.Parameters[1:]...)...)
			pointers := append([]*spec.Parameter(nil), c.Parameters...)
			before, err := json.Marshal(c.Parameters)
			require.NoError(t, err)
			lookup := func(name string) (reflect.Type, error) {
				if name == "alias.Name" {
					return reflect.TypeFor[scalarSelectorName](), nil
				}
				return nil, fmt.Errorf("unknown %s", name)
			}
			engine := compiler.New(compiler.Input{Component: c, InputType: reflect.TypeFor[scalarSelectorInput](), TypeLookup: lookup})
			var result *compiler.Result
			if describe {
				result, err = engine.Describe()
			} else {
				result, err = engine.Compile()
			}
			require.NoError(t, err)
			require.NotNil(t, result.SelectorPlan)
			for i, p := range c.Parameters {
				require.Same(t, pointers[i], p)
			}
			after, err := json.Marshal(c.Parameters)
			require.NoError(t, err)
			require.Equal(t, string(before), string(after))
			for i, binding := range result.Bindings {
				require.Same(t, pointers[i], binding.Extension)
			}
			canonicalName := "*" + reflect.TypeFor[scalarSelectorName]().PkgPath() + "." + reflect.TypeFor[scalarSelectorName]().Name()
			require.Equal(t, canonicalName, result.Bindings[2].DataType)
			require.Equal(t, "*alias.Name", name.TypeExpr)
			route, ok := result.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/selector-records"})
			require.True(t, ok)
			want := map[string]*spec.WireSchema{"ID": {Type: "integer", Nullable: true}, "Active": {Type: "boolean", Nullable: false}, "Name": {Type: "string", Nullable: true}}
			for _, field := range route.Fields() {
				require.Equal(t, want[field.Path()], field.WireSchema())
				require.Equal(t, field.DestinationType(), field.SourceType())
				require.Nil(t, field.Binding().Transformer)
			}
			// Registry reconstruction preserves immutable schemas and selector ownership.
			projected, err := route.WithOptionalBindings("Active")
			require.NoError(t, err)
			for _, field := range projected.Fields() {
				require.Equal(t, want[field.Path()], field.WireSchema())
			}
			selected, err := selectorcompanion.Select(c, reflect.TypeFor[scalarSelectorInput]())
			require.NoError(t, err)
			replaced := append([]*spec.Parameter(nil), c.Parameters...)
			copy := *replaced[0]
			replaced[0] = &copy
			changed := *c
			changed.Parameters = replaced
			injector, err := bindly.NewInjector()
			require.NoError(t, err)
			sidePlan, err := injector.CompilePlan(selected.Type, result.SelectorPlan.Bindings()...)
			require.NoError(t, err)
			_, err = selectorcompanion.NewBindingPlan(&changed, selected, reflect.TypeFor[scalarSelectorInput](), result.SelectorPlan.PublicPlan(), sidePlan, result.Bindings, result.SelectorPlan.Bindings())
			require.ErrorContains(t, err, "canonical binding order changed")
		})
	}
}
