package collector

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func TestCompileRejectsMissingRelationChildRowType(t *testing.T) {
	type childRow struct{ ParentID int }
	type parentRow struct {
		ID       int
		Children []childRow
	}
	child := &data.View{Spec: spec.View{Name: "children"}}
	root := &data.View{Spec: spec.View{Name: "parents"}, Relations: []*data.Relation{{
		Name: "Children", Holder: "Children", Cardinality: spec.CardinalityMany,
		On: data.Links{data.NewLink("", "id", "ID")},
		Of: &data.RelationRef{View: child, On: data.Links{
			data.NewLink("", "parent_id", "ParentID"),
		}},
	}}}

	_, err := Compile(root, map[*data.View]reflect.Type{root: reflect.TypeOf(parentRow{})})
	if err == nil || !strings.Contains(err.Error(), "row type is required for relation Children child view children") {
		t.Fatalf("expected missing child row type error, got %v", err)
	}
}

func TestCompileAllowsSchemaLessRootForTypedOutputRelation(t *testing.T) {
	type totals struct{ Count int }
	child := &data.View{Spec: spec.View{Name: "totals", Source: &spec.ViewSource{SQL: "SELECT COUNT(*) AS count FROM users"}}}
	root := &data.View{Spec: spec.View{Name: "users"}, Relations: []*data.Relation{{
		Name: "Totals", Kind: spec.RelationKindDerived, Holder: "Totals", Cardinality: spec.CardinalityOne,
		Of: &data.RelationRef{View: child, MatchStrategy: data.MatchSequential},
	}}}

	graph, err := Compile(root, map[*data.View]reflect.Type{child: reflect.TypeOf(totals{})})
	if err != nil {
		t.Fatalf("compile output-only graph: %v", err)
	}
	if graph.Root.Schema.RowType() != nil || graph.View(child).Schema.RowType() != reflect.TypeOf(totals{}) {
		t.Fatalf("unexpected output-only schemas: root=%v child=%v", graph.Root.Schema.RowType(), graph.View(child).Schema.RowType())
	}
}

func TestCompilePreservesSharedViewIdentity(t *testing.T) {
	type childRow struct{ ID int }
	type parentRow struct {
		Left  []childRow
		Right []childRow
	}
	child := &data.View{Spec: spec.View{Name: "shared"}}
	root := &data.View{Spec: spec.View{Name: "root"}, Relations: []*data.Relation{
		{Name: "Left", Holder: "Left", Cardinality: spec.CardinalityMany, Of: &data.RelationRef{View: child}},
		{Name: "Right", Holder: "Right", Cardinality: spec.CardinalityMany, Of: &data.RelationRef{View: child}},
	}}

	graph, err := Compile(root, map[*data.View]reflect.Type{
		root:  reflect.TypeOf(parentRow{}),
		child: reflect.TypeOf(childRow{}),
	})
	if err != nil {
		t.Fatalf("compile shared graph: %v", err)
	}
	left := graph.Root.Relations[0].Of.View
	right := graph.Root.Relations[1].Of.View
	if left != right || left != graph.View(child) {
		t.Fatalf("expected one compiled view for shared metadata: left=%p right=%p lookup=%p", left, right, graph.View(child))
	}
}

func TestCompilePreservesCyclicMetadataGraph(t *testing.T) {
	type aRow struct{ Bs []any }
	type bRow struct{ As []any }
	a := &data.View{Spec: spec.View{Name: "a"}}
	b := &data.View{Spec: spec.View{Name: "b"}}
	a.Relations = []*data.Relation{{Name: "Bs", Holder: "Bs", Cardinality: spec.CardinalityMany, Of: &data.RelationRef{View: b}}}
	b.Relations = []*data.Relation{{Name: "As", Holder: "As", Cardinality: spec.CardinalityMany, Of: &data.RelationRef{View: a}}}

	graph, err := Compile(a, map[*data.View]reflect.Type{
		a: reflect.TypeOf(aRow{}),
		b: reflect.TypeOf(bRow{}),
	})
	if err != nil {
		t.Fatalf("compile cyclic graph: %v", err)
	}
	backToRoot := graph.Root.Relations[0].Of.View.Relations[0].Of.View
	if backToRoot != graph.Root {
		t.Fatalf("expected cycle to reuse compiled root: got=%p root=%p", backToRoot, graph.Root)
	}
}
