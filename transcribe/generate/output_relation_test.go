package generate

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestDerivedOutputUsesPlannedViewType(t *testing.T) {
	for _, tc := range []struct {
		name, authored, want string
		cardinality          spec.Cardinality
	}{
		{"inferred singleton", "", "*DiscoveredTotals", spec.CardinalityOne},
		{"inferred collection", "", "[]*DiscoveredTotals", spec.CardinalityMany},
		{"explicit value", "DiscoveredTotals", "DiscoveredTotals", spec.CardinalityOne},
		{"explicit pointer", "*DiscoveredTotals", "*DiscoveredTotals", spec.CardinalityOne},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Orders"},
				Parameters: []*spec.Parameter{{Name: "Meta", TypeExpr: tc.authored, Source: spec.BindSource{Kind: "output", Name: "summary"}}},
				RootView:   &spec.View{Name: "Orders", Source: &spec.ViewSource{SQL: "SELECT id FROM orders"}, Columns: []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: "int"}}}},
			}
			summary := &spec.View{Name: "Meta", TypeName: "DiscoveredTotals", Source: &spec.ViewSource{SQL: "SELECT COUNT(*) AS count FROM orders"}, Columns: []*spec.Column{{Name: "count", Type: spec.TypeRef{Name: "int"}}}}
			component.RootView.Relations = []*spec.Relation{{Name: "Meta", Holder: "Meta", Kind: spec.RelationKindDerived, Cardinality: tc.cardinality, View: summary}}
			generator := New(Input{Component: component})
			plan, err := generator.Plan()
			require.NoError(t, err)
			require.Equal(t, tc.want, plan.Output.Fields[0].Type)
			for _, view := range plan.Views {
				if view.Type == plan.RootViewType {
					for _, field := range view.Fields {
						require.NotEqual(t, "Meta", field.Name)
					}
				}
			}
			typ, err := generator.RuntimeOutputType()
			require.NoError(t, err)
			field, ok := typ.FieldByName("Meta")
			require.True(t, ok)
			require.NotEqual(t, reflect.Interface, field.Type.Kind())
		})
	}
}
