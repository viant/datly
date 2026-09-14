package collector

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

type valueTreeNode struct {
	ID       int
	ParentID *int
	Children []valueTreeNode
}

type pointerTreeNode struct {
	ID       int
	ParentID *int
	Name     string
	Children []*pointerTreeNode
}

type interfaceTreeNode struct {
	ID       int
	ParentID *int
	Children []interface{}
}

type nullableTreeNode struct {
	ID       *int
	ParentID *int
	Children []*nullableTreeNode
}

func TestTreePlan_BuildsUnlimitedDepthForValueHolder(t *testing.T) {
	two, one := 2, 1
	nodes := []valueTreeNode{{ID: 3, ParentID: &two}, {ID: 1}, {ID: 2, ParentID: &one}}
	plan := newTestTreePlan(t, reflect.TypeOf(valueTreeNode{}))
	roots := plan.Build(&nodes).(*[]valueTreeNode)
	if len(*roots) != 1 || (*roots)[0].ID != 1 || len((*roots)[0].Children) != 1 || len((*roots)[0].Children[0].Children) != 1 || (*roots)[0].Children[0].Children[0].ID != 3 {
		t.Fatalf("unexpected value-holder tree: %#v", *roots)
	}
}

func TestTreePlan_BuildsPointerAndInterfaceHolders(t *testing.T) {
	one := 1
	pointerNodes := []pointerTreeNode{{ID: 1}, {ID: 2, ParentID: &one}}
	pointerRoots := newTestTreePlan(t, reflect.TypeOf(pointerTreeNode{})).Build(&pointerNodes).(*[]pointerTreeNode)
	if len(*pointerRoots) != 1 || len((*pointerRoots)[0].Children) != 1 || (*pointerRoots)[0].Children[0].ID != 2 {
		t.Fatalf("unexpected pointer-holder tree: %#v", *pointerRoots)
	}

	interfaceNodes := []interfaceTreeNode{{ID: 1}, {ID: 2, ParentID: &one}}
	interfaceRoots := newTestTreePlan(t, reflect.TypeOf(interfaceTreeNode{})).Build(&interfaceNodes).(*[]interfaceTreeNode)
	if len(*interfaceRoots) != 1 || len((*interfaceRoots)[0].Children) != 1 {
		t.Fatalf("unexpected interface-holder tree: %#v", *interfaceRoots)
	}
	child, ok := (*interfaceRoots)[0].Children[0].(*interfaceTreeNode)
	if !ok || child.ID != 2 {
		t.Fatalf("unexpected interface child: %#v", (*interfaceRoots)[0].Children[0])
	}
}

func TestTreePlan_DeduplicatesParentChildEdgesUsingLastIDRow(t *testing.T) {
	one := 1
	nodes := []pointerTreeNode{
		{ID: 1},
		{ID: 2, ParentID: &one, Name: "first"},
		{ID: 2, ParentID: &one, Name: "last"},
	}
	roots := newTestTreePlan(t, reflect.TypeOf(pointerTreeNode{})).Build(&nodes).(*[]pointerTreeNode)
	if len(*roots) != 1 || len((*roots)[0].Children) != 1 || (*roots)[0].Children[0].Name != "last" {
		t.Fatalf("unexpected duplicate-ID tree: %#v", *roots)
	}
}

func BenchmarkTreePlan_BuildWide(b *testing.B) {
	rootID := 1
	nodes := make([]pointerTreeNode, 10_001)
	nodes[0].ID = rootID
	for i := 1; i < len(nodes); i++ {
		nodes[i] = pointerTreeNode{ID: i + 1, ParentID: &rootID}
	}
	plan := newTestTreePlan(b, reflect.TypeOf(pointerTreeNode{}))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		input := append([]pointerTreeNode(nil), nodes...)
		_ = plan.Build(&input)
	}
}

func TestTreePlan_TreatsNilIDAndMissingParentAsRoots(t *testing.T) {
	one, missing := 1, 99
	nodes := []nullableTreeNode{
		{ID: &one},
		{ParentID: &one},
		{ID: func() *int { value := 2; return &value }(), ParentID: &missing},
	}
	roots := newTestTreePlan(t, reflect.TypeOf(nullableTreeNode{})).Build(&nodes).(*[]nullableTreeNode)
	if len(*roots) != 3 {
		t.Fatalf("unexpected nullable roots: %#v", *roots)
	}
}

func TestTreePlan_CycleDoesNotRecurseIndefinitely(t *testing.T) {
	one, two := 1, 2
	nodes := []pointerTreeNode{{ID: 1, ParentID: &two}, {ID: 2, ParentID: &one}}
	roots := newTestTreePlan(t, reflect.TypeOf(pointerTreeNode{})).Build(&nodes).(*[]pointerTreeNode)
	if len(*roots) != 0 {
		t.Fatalf("cyclic rows must not be promoted to roots: %#v", *roots)
	}
}

func TestCollector_AssemblesTreeAfterDeterministicPartitionMerge(t *testing.T) {
	one := 1
	view := newTestView(&data.View{Spec: spec.View{Name: "partitionedTree",
		SelfReference: &spec.SelfReference{Holder: "Children", Child: "ID", Parent: "ParentID"}},
	}, reflect.TypeOf(pointerTreeNode{}))
	plan, err := NewTreePlan(view)
	if err != nil {
		t.Fatalf("compile tree plan: %v", err)
	}
	view.Tree = plan
	dest := &[]pointerTreeNode{}
	canonical := NewCollector(view, dest, false)
	childPartition := canonical.PartitionCopy()
	rootPartition := canonical.PartitionCopy()
	if err := childPartition.AppendSlice(context.Background(), []pointerTreeNode{{ID: 2, ParentID: &one}}); err != nil {
		t.Fatalf("append child partition: %v", err)
	}
	if err := rootPartition.AppendSlice(context.Background(), []pointerTreeNode{{ID: 1}}); err != nil {
		t.Fatalf("append root partition: %v", err)
	}
	if err := canonical.AppendPartition(context.Background(), childPartition); err != nil {
		t.Fatalf("merge child partition: %v", err)
	}
	if err := canonical.AppendPartition(context.Background(), rootPartition); err != nil {
		t.Fatalf("merge root partition: %v", err)
	}
	if err := canonical.AssembleTrees(); err != nil {
		t.Fatalf("assemble partitioned tree: %v", err)
	}
	roots := canonical.Dest().([]pointerTreeNode)
	if len(roots) != 1 || roots[0].ID != 1 || len(roots[0].Children) != 1 || roots[0].Children[0].ID != 2 {
		t.Fatalf("unexpected partitioned tree: %#v", roots)
	}
}

func newTestTreePlan(t testing.TB, rType reflect.Type) *TreePlan {
	t.Helper()
	view := newTestView(&data.View{Spec: spec.View{Name: "tree",
		SelfReference: &spec.SelfReference{Holder: "Children", Child: "ID", Parent: "ParentID"}},
	}, rType)
	plan, err := NewTreePlan(view)
	if err != nil {
		t.Fatalf("compile tree plan: %v", err)
	}
	return plan
}
