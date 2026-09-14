package compiler

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestSingletonOutputAuthority(t *testing.T) {
	type row struct {
		ID int `sqlx:"id"`
	}
	type envelope struct{ Data *row }
	type named struct{ Record *row }
	for _, tc := range []struct {
		name        string
		output      reflect.Type
		cardinality spec.Cardinality
		holder      string
		direct      bool
	}{
		{"direct pointer", reflect.TypeFor[*row](), spec.CardinalityOne, "", true},
		{"direct value", reflect.TypeFor[row](), spec.CardinalityOne, "", true},
		{"conventional holder", reflect.TypeFor[envelope](), spec.CardinalityOne, "Data", false},
		{"explicit holder", reflect.TypeFor[named](), spec.CardinalityOne, "Record", false},
		{"body holder", reflect.TypeFor[named](), spec.CardinalityOne, "Record", false},
		{"collection", reflect.TypeFor[[]row](), spec.CardinalityMany, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := &spec.Component{RootView: &spec.View{Name: "records", Cardinality: tc.cardinality, Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}}
			if tc.name == "explicit holder" || tc.name == "body holder" {
				slot := "view"
				if tc.name == "body holder" {
					slot = "body"
				}
				component.Parameters = []*spec.Parameter{{Name: "Record", Source: spec.BindSource{Kind: "output", Name: slot}}}
			}
			plan, err := Compile(Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: tc.output})
			require.NoError(t, err)
			require.Equal(t, tc.direct, plan.DirectOutput)
			require.Equal(t, tc.holder, plan.OutputViewField)
			require.NotNil(t, plan.Root.Collector)
		})
	}
}
