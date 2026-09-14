package reader

import (
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

type accessorCount struct{ Count int }
type accessorBox struct{ Total *accessorCount }
type accessorPromoted struct{ Alias *accessorCount }
type accessorOutput struct {
	Data        *accessorBox
	Left, Right accessorBox
	*accessorPromoted
}

func TestOutputAccessorsRejectAliasesAncestorsAndRootOverlap(t *testing.T) {
	for _, tc := range []struct {
		name, root string
		holders    []string
		invalid    bool
	}{
		{"distinct nested", "", []string{"Left.Total", "Right.Total"}, false},
		{"duplicate", "", []string{"Left.Total", "Left.Total"}, true},
		{"ancestor", "", []string{"Left", "Left.Total"}, true},
		{"reverse ancestor", "", []string{"Left.Total", "Left"}, true},
		{"root overlap", "Data", []string{"Data.Total"}, true},
		{"promoted alias", "", []string{"Alias", "accessorPromoted.Alias"}, true},
		{"missing", "", []string{"Left.Missing"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := &Plan{OutputViewField: tc.root, Root: &ViewPlan{}}
			for _, holder := range tc.holders {
				plan.Root.Relations = append(plan.Root.Relations, &RelationPlan{Relation: &data.Relation{Kind: spec.RelationKindDerived, Holder: holder, Of: &data.RelationRef{View: &data.View{Spec: spec.View{Source: &spec.ViewSource{SQL: "SELECT 1"}}}}}})
			}
			_, err := plan.compileOutputAccessors(reflect.TypeOf(accessorOutput{}))
			if (err != nil) != tc.invalid {
				t.Fatalf("accessors=%v", err)
			}
		})
	}
}
