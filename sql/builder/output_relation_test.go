package builder

import (
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func mustSummaryRelation(t *testing.T, parent *data.View, source *spec.ViewSource, rType reflect.Type) *data.Relation {
	t.Helper()
	if rType == nil {
		rType = reflect.TypeOf(struct{}{})
	}
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	if rType.Kind() != reflect.Struct {
		t.Fatalf("summary type must be a struct, got %s", rType)
	}
	child := &data.View{Spec: spec.View{Name: "Summary", Source: source.Clone()}, Relations: []*data.Relation{}}
	return &data.Relation{
		Name: "Summary", Kind: spec.RelationKindDerived, Holder: "Summary", Cardinality: spec.CardinalityOne,
		Of: &data.RelationRef{View: child, MatchStrategy: data.MatchSequential},
	}
}
