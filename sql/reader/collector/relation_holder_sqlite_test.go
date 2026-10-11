package collector

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
)

// SQLX and the collector expose *T rows for both []T and []*T destinations.
// Preserve value copying and pointer identity in both relation assembly paths.
func TestRelationHolderRowShapesSQLite(t *testing.T) {
	for _, pointerHolder := range []bool{false, true} {
		for _, pointerRow := range []bool{false, true} {
			for _, strategy := range []data.MatchStrategy{data.MatchSequential, data.MatchReadAll} {
				name := map[data.MatchStrategy]string{data.MatchSequential: "streaming", data.MatchReadAll: "read-all"}[strategy]
				if pointerHolder {
					name += "/pointer-holder"
				} else {
					name += "/value-holder"
				}
				if pointerRow {
					name += "/pointer-row"
				} else {
					name += "/value-row"
				}
				t.Run(name, func(t *testing.T) {
					ctx := context.Background()
					h := sqlite.New(t)
					if err := h.ExecStatements(ctx, "CREATE TABLE parents(ID INTEGER)", "INSERT INTO parents VALUES(10)", "CREATE TABLE children(ID INTEGER,ParentID INTEGER)", "INSERT INTO children VALUES(1,10),(2,10),(3,99)"); err != nil {
						t.Fatal(err)
					}
					child := &data.View{Spec: spec.View{Name: "children"}}
					parent := &data.View{Spec: spec.View{Name: "parents"}, Relations: []*data.Relation{{Name: "Children", Holder: "Children", Cardinality: spec.CardinalityMany, On: data.Links{data.NewLink("", "ID", "ID")}, Of: &data.RelationRef{View: child, On: data.Links{data.NewLink("", "ParentID", "ParentID")}, MatchStrategy: strategy}}}}
					parentType := reflect.TypeFor[*reducedValueParent]()
					if pointerHolder {
						parentType = reflect.TypeFor[*reducedPointerParent]()
					}
					childType := reflect.TypeFor[reducedChild]()
					if pointerRow {
						childType = reflect.PointerTo(childType)
					}
					graph, err := Compile(parent, map[*data.View]reflect.Type{parent: parentType, child: childType})
					if err != nil {
						t.Fatal(err)
					}
					dest := reflect.New(reflect.SliceOf(parentType))
					root := NewCollector(graph.Root, dest.Interface(), false)
					fixture := evidenceSQL{t, h}
					fixture.scan(root, "SELECT ID FROM parents")
					root.Fetched()
					relation := root.Relations(nil)[0]
					fixture.scan(relation, "SELECT ID,ParentID FROM children ORDER BY ID")
					relation.Fetched()
					if err = relation.RebindToParent(); err != nil {
						t.Fatal(err)
					}
					if err = root.MergeData(); err != nil {
						t.Fatal(err)
					}
					holder := dest.Elem().Index(0).Elem().FieldByName("Children")
					if holder.Len() != 2 {
						t.Fatalf("children=%d, expected 2", holder.Len())
					}
					for i := 0; i < holder.Len(); i++ {
						row := holder.Index(i)
						if pointerHolder {
							if pointerRow && row.Pointer() != reflect.ValueOf(relation.Dest()).Index(i).Pointer() {
								t.Fatal("pointer holder lost source row identity")
							}
							row = row.Elem()
						}
						if row.FieldByName("ID").Int() != int64(i+1) || row.FieldByName("ParentID").Int() != 10 {
							t.Fatalf("unexpected child %+v", row.Interface())
						}
					}
				})
			}
		}
	}
}

func BenchmarkRelationHolderMillionRows(b *testing.B) {
	const count = 1_000_000
	for _, pointerHolder := range []bool{false, true} {
		for _, pointerRow := range []bool{false, true} {
			for _, strategy := range []data.MatchStrategy{data.MatchSequential, data.MatchReadAll} {
				name := map[data.MatchStrategy]string{data.MatchSequential: "streaming", data.MatchReadAll: "read-all"}[strategy]
				if pointerHolder {
					name += "/pointer-holder"
				} else {
					name += "/value-holder"
				}
				if pointerRow {
					name += "/pointer-row"
				} else {
					name += "/value-row"
				}
				b.Run(name, func(b *testing.B) {
					child := &data.View{Spec: spec.View{Name: "children"}}
					parent := &data.View{Spec: spec.View{Name: "parents"}, Relations: []*data.Relation{{Name: "Children", Holder: "Children", Cardinality: spec.CardinalityMany, On: data.Links{data.NewLink("", "ID", "ID")}, Of: &data.RelationRef{View: child, On: data.Links{data.NewLink("", "ParentID", "ParentID")}, MatchStrategy: strategy}}}}
					parentType := reflect.TypeFor[*reducedValueParent]()
					if pointerHolder {
						parentType = reflect.TypeFor[*reducedPointerParent]()
					}
					childType := reflect.TypeFor[reducedChild]()
					values := make([]reducedChild, count)
					for i := range values {
						values[i] = reducedChild{ID: i + 1, ParentID: 10}
					}
					var rows any = values
					if pointerRow {
						childType = reflect.PointerTo(childType)
						pointers := make([]*reducedChild, count)
						for i := range pointers {
							pointers[i] = &values[i]
						}
						rows = pointers
					}
					graph, err := Compile(parent, map[*data.View]reflect.Type{parent: parentType, child: childType})
					if err != nil {
						b.Fatal(err)
					}
					ctx := context.Background()
					b.ReportAllocs()
					b.ResetTimer()
					for iteration := 0; iteration < b.N; iteration++ {
						dest := reflect.New(reflect.SliceOf(parentType))
						root := NewCollector(graph.Root, dest.Interface(), false)
						row := root.NewItem()()
						reflect.ValueOf(row).Elem().FieldByName("ID").SetInt(10)
						if err = root.Visitor(ctx)(row); err != nil {
							b.Fatal(err)
						}
						root.Fetched()
						relation := root.Relations(nil)[0]
						if err = relation.AppendSlice(ctx, rows); err != nil {
							b.Fatal(err)
						}
						relation.Fetched()
					if err = relation.RebindToParent(); err != nil {
						b.Fatal(err)
					}
						if err = root.MergeData(); err != nil {
							b.Fatal(err)
						}
						holder := dest.Elem().Index(0).Elem().FieldByName("Children")
						if holder.Len() != count {
							b.Fatalf("children=%d expected %d", holder.Len(), count)
						}
					}
				})
			}
		}
	}
}
