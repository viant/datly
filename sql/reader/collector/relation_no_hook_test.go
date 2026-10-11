package collector

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
)

type ordinaryRelationParent struct {
	ID       int
	Children []*ordinaryParentHookChild
}

type ordinaryParentHookChild struct {
	ID, ParentID int
	Calls        int  `sqlx:"-"`
	ContextSeen  bool `sqlx:"-"`
}

type relationHookContextKey struct{}

func (r *ordinaryParentHookChild) OnRelation(ctx context.Context) {
	r.Calls++
	r.ContextSeen = ctx.Value(relationHookContextKey{}) == "request"
}

func TestOrdinaryParentStillRunsChildRelationHooksSQLite(t *testing.T) {
	for _, pointerParent := range []bool{false, true} {
		name := "value"
		if pointerParent {
			name = "pointer"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), relationHookContextKey{}, "request")
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE parents(ID INTEGER)", "INSERT INTO parents VALUES(10)", "CREATE TABLE children(ID INTEGER,ParentID INTEGER)", "INSERT INTO children VALUES(1,10),(2,10)"); err != nil {
				t.Fatal(err)
			}
			child := &data.View{Spec: spec.View{Name: "children", RelationalConcurrency: 2}}
			parent := &data.View{Spec: spec.View{Name: "parents", RelationalConcurrency: 8}, Relations: []*data.Relation{{Name: "Children", Holder: "Children", Cardinality: spec.CardinalityMany, On: data.Links{data.NewLink("", "ID", "ID")}, Of: &data.RelationRef{View: child, On: data.Links{data.NewLink("", "ParentID", "ParentID")}, MatchStrategy: data.MatchReadAll}}}}
			parentType := reflect.TypeFor[ordinaryRelationParent]()
			if pointerParent {
				parentType = reflect.PointerTo(parentType)
			}
			graph, err := Compile(parent, map[*data.View]reflect.Type{parent: parentType, child: reflect.TypeFor[*ordinaryParentHookChild]()})
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
			if err := root.MergeData(); err != nil {
				t.Fatal(err)
			}
			if err := root.RunOnRelation(ctx); err != nil {
				t.Fatal(err)
			}
			row := dest.Elem().Index(0)
			if pointerParent {
				row = row.Elem()
			}
			holder := row.FieldByName("Children")
			if holder.Len() != 2 {
				t.Fatalf("child count = %d", holder.Len())
			}
			for i := 0; i < holder.Len(); i++ {
				value := holder.Index(i).Interface().(*ordinaryParentHookChild)
				if value.ID != i+1 || value.Calls != 1 || !value.ContextSeen {
					t.Fatalf("child hook or inherited request context lost: %+v", value)
				}
			}
		})
	}
}

type namedRelationHookRows []int

func (r *namedRelationHookRows) OnRelation(context.Context) { *r = append(*r, 42) }

func TestNamedSliceRelationHookRetainsDynamicDispatch(t *testing.T) {
	rows := []namedRelationHookRows{{1}}
	view := newTestView(&data.View{Spec: spec.View{Name: "slice_hook"}}, reflect.TypeFor[namedRelationHookRows]())
	collector := NewCollector(view, &rows, false)
	if err := collector.RunOnRelation(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(rows[0]) != 2 || rows[0][1] != 42 {
		t.Fatalf("named slice hook skipped: %v", rows)
	}
}
