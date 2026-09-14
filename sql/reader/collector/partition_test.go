package collector

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
)

type reducedChild struct{ ID, ParentID int }
type reducedValueParent struct {
	ID       int
	Children []reducedChild
}
type reducedPointerParent struct {
	ID       int
	Children []*reducedChild
}

func TestAppendReducedRowsPreservesPointerCardinality(t *testing.T) {
	for _, test := range []struct {
		name          string
		parent, child reflect.Type
		rows          any
	}{
		{"values", reflect.TypeFor[*reducedValueParent](), reflect.TypeFor[reducedChild](), []reducedChild{{ID: 1, ParentID: 10}, {ID: 2, ParentID: 10}}},
		{"pointers", reflect.TypeFor[*reducedPointerParent](), reflect.TypeFor[*reducedChild](), []*reducedChild{{ID: 1, ParentID: 10}, {ID: 2, ParentID: 10}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			child := &data.View{Spec: spec.View{Name: "Children"}}
			parent := &data.View{Spec: spec.View{Name: "Parent"}, Relations: []*data.Relation{{Name: "Children", Holder: "Children", Cardinality: spec.CardinalityMany, On: data.Links{{Column: "id", Field: "ID"}}, Of: &data.RelationRef{View: child, On: data.Links{{Column: "parent_id", Field: "ParentID"}}}}}}
			graph, err := Compile(parent, map[*data.View]reflect.Type{parent: test.parent, child: test.child})
			if err != nil {
				t.Fatal(err)
			}
			dest := reflect.New(reflect.SliceOf(test.parent))
			root := NewCollector(graph.Root, dest.Interface(), false)
			row := root.NewItem()()
			id, err := xshape.Linked(test.parent).Accessor("ID")
			if err != nil {
				t.Fatal(err)
			}
			if err := id.Set(row, 10); err != nil {
				t.Fatal(err)
			}
			if err := root.Visitor(ctx)(row); err != nil {
				t.Fatal(err)
			}
			root.Fetched()
			if err := root.Relations(nil)[0].AppendSlice(ctx, test.rows); err != nil {
				t.Fatal(err)
			}
			holder, err := xshape.Linked(test.parent).Accessor("Children")
			if err != nil {
				t.Fatal(err)
			}
			actual, err := holder.Get(row)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual.Interface(), test.rows) {
				t.Fatalf("attached=%+v expected=%+v", actual.Interface(), test.rows)
			}
		})
	}
}
