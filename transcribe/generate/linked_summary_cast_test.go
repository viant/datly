package generate

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
)

type linkedSummaryRecord struct {
	ID int `sqlx:"ID"`
}

type linkedSummaryTotals struct {
	Count int `sqlx:"CNT"`
}

func TestLinkedRowCastsKeepDerivedOutputSeparate(t *testing.T) {
	totals := &spec.View{Name: "Meta", Columns: []*spec.Column{{Name: "CNT", Source: "CNT", ExplicitType: true, Type: spec.TypeRef{Name: "int"}}}}
	row := &spec.View{Name: "Records", Relations: []*spec.Relation{{Name: "Meta", Holder: "Meta", Kind: spec.RelationKindDerived, View: totals}}}
	validate := func(view *spec.View, typ reflect.Type) error {
		v := &linkedViewCastValidator{plan: &Plan{Package: typ.PkgPath()}, shape: xshape.Linked(typ), visiting: map[*spec.View]bool{}}
		return v.validate(view, "")
	}
	// Summary belongs to the output envelope, not to every linked record.
	require.NoError(t, validate(row, reflect.TypeFor[linkedSummaryRecord]()))
	require.NoError(t, validate(totals, reflect.TypeFor[linkedSummaryTotals]()))
	totals.Columns[0].Type.Name = "string"
	require.ErrorContains(t, validate(totals, reflect.TypeFor[linkedSummaryTotals]()), "CAST cannot change")
	// Ordinary child holders must still exist on their linked parent row.
	row.Relations[0].Kind = ""
	require.ErrorContains(t, validate(row, reflect.TypeFor[linkedSummaryRecord]()), "Meta")
}
