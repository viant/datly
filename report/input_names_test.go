package report

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/state"
	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
)

func TestCubeWireNameCollisionsBeforeDerivation(t *testing.T) {
	for _, section := range []string{"Dimensions", "Measures", "Filters"} {
		t.Run(section, func(t *testing.T) {
			source := reportSource(t, &spec.ReportSettings{Enabled: true})
			if section == "Filters" {
				source.Component.Parameters = []*spec.Parameter{
					{Name: "AccountIDs", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "first"}},
					{Name: "Account_IDs", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "second"}},
				}
				input, err := handlercompiler.New(handlercompiler.Input{Component: source.Component, InputType: reflect.TypeFor[struct{ AccountIDs, Account_IDs int }]()}).Compile()
				require.NoError(t, err)
				source.Input = input.Input
			} else {
				source.Component.RootView.Columns = []*spec.Column{
					{Name: "AccountIDs", Source: "first", Groupable: boolPointer(section == "Dimensions")},
					{Name: "Account_IDs", Source: "second", Groupable: boolPointer(section == "Dimensions")},
				}
			}
			contract, ok := source.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/spend"})
			require.True(t, ok)
			before := source.Component.Clone()
			_, err := compileMetadata(source.Component, contract)
			require.ErrorContains(t, err, section)
			require.ErrorContains(t, err, "AccountIDs")
			require.ErrorContains(t, err, "Account_IDs")
			require.ErrorContains(t, err, `wire name "accountIds"`)
			require.Equal(t, before, source.Component.Clone())
		})
	}
}

func TestCubeWireNamesAreScopedBySection(t *testing.T) {
	m := &metadata{
		inputLayout: spec.ReportInputLayout{Dimensions: "Dimensions", Measures: "Measures", Filters: "Filters"},
		dimensions:  []field{{fieldName: "AccountID"}},
		measures:    []field{{fieldName: "AccountId"}},
		filters:     []filterField{{field: field{fieldName: "AccountID"}}},
	}
	require.NoError(t, m.validateWireNames())
}

func TestLinkedCubeRejectsEffectiveWireNameCollisions(t *testing.T) {
	type selections struct {
		First  bool `json:"same"`
		Second bool `json:"same"`
	}
	type filters struct {
		First  *int `json:"same"`
		Second *int `json:"same"`
	}
	type input struct {
		Dimensions    selections
		Measures      selections
		Filters       filters
		OrderBy       []string
		Limit, Offset *int
	}
	for _, section := range []string{"Dimensions", "Measures", "Filters"} {
		t.Run(section, func(t *testing.T) {
			definition := Definition{View: "rows", OrderBy: "OrderBy", Limit: "Limit", Offset: "Offset"}
			if section == "Filters" {
				definition.Filters = []FilterDefinition{
					{Field: "Filters.First", Name: "first", Location: state.Location{Kind: "query", In: "first"}},
					{Field: "Filters.Second", Name: "second", Location: state.Location{Kind: "query", In: "second"}},
				}
			} else {
				fields := []SelectionDefinition{{Field: section + ".First", Name: "first"}, {Field: section + ".Second", Name: "second"}}
				if section == "Dimensions" {
					definition.Dimensions = fields
				} else {
					definition.Measures = fields
				}
			}
			_, err := definition.Compile(reflect.TypeFor[input](), reflect.TypeFor[struct{}]())
			require.ErrorContains(t, err, section+".First")
			require.ErrorContains(t, err, section+".Second")
			require.ErrorContains(t, err, `wire name "same"`)
		})
	}
	definition := Definition{
		View: "rows", OrderBy: "OrderBy", Limit: "Limit", Offset: "Offset",
		Dimensions: []SelectionDefinition{{Field: "Dimensions.First", Name: "dimension"}},
		Measures:   []SelectionDefinition{{Field: "Measures.First", Name: "measure"}},
		Filters:    []FilterDefinition{{Field: "Filters.First", Name: "first", Location: state.Location{Kind: "query", In: "first"}}},
	}
	_, err := definition.Compile(reflect.TypeFor[input](), reflect.TypeFor[struct{}]())
	require.NoError(t, err)
}
