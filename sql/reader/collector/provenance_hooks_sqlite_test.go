package collector

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
)

type topologyModeKey struct{}
type topologyPointerNode struct {
	ID       int
	ParentID *int
	Name     *string
	Children []*topologyPointerNode `sqlx:"-"`
}

func (r *topologyPointerNode) OnRelation(ctx context.Context) {
	if r.ID != 1 {
		return
	}
	mode, _ := ctx.Value(topologyModeKey{}).(string)
	switch mode {
	case "reorder":
		r.Children[0], r.Children[1] = r.Children[1], r.Children[0]
	case "subset":
		r.Children = r.Children[1:]
	case "equal replacement":
		clone := *r.Children[0]
		r.Children[0] = &clone
	case "duplicate":
		r.Children = append(r.Children, r.Children[0])
	case "append":
		r.Children = append(r.Children, &topologyPointerNode{ID: 999})
	case "nested reorder":
		children := r.Children[0].Children
		children[0], children[1] = children[1], children[0]
	case "scalar mutation":
		name := "changed by hook"
		r.Children[0].Name = &name
	}
}

func TestHookProvenancePreservesOnlyUniquePointersAcrossPartitionedSelfTreesSQLite(t *testing.T) {
	for _, tc := range []struct {
		mode  string
		ids   []int
		known []bool
		name  []bool
	}{
		{"read only", []int{2, 3}, []bool{true, true}, []bool{true, false}},
		{"reorder", []int{3, 2}, []bool{true, true}, []bool{false, true}},
		{"subset", []int{3}, []bool{true}, []bool{false}},
		{"equal replacement", []int{2, 3}, []bool{false, true}, []bool{false, false}},
		{"duplicate", []int{2, 3, 2}, []bool{false, true, false}, []bool{false, false, false}},
		{"append", []int{2, 3, 999}, []bool{true, true, false}, []bool{true, false, false}},
		{"nested reorder", []int{2, 3}, []bool{true, true}, []bool{true, false}},
		{"scalar mutation", []int{2, 3}, []bool{true, true}, []bool{true, false}},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			h := sqlite.New(t)
			if err := h.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER,parent_id INTEGER,name TEXT)", "INSERT INTO records VALUES(1,NULL,'root'),(2,1,NULL),(3,1,'omitted'),(4,2,'leaf'),(5,2,'omitted leaf')"); err != nil {
				t.Fatal(err)
			}
			view := &data.View{Spec: spec.View{Name: "nodes", SelfReference: &spec.SelfReference{Holder: "Children", Child: "ID", Parent: "ParentID"}, RelationalConcurrency: 3}}
			graph, err := Compile(view, map[*data.View]reflect.Type{view: reflect.TypeOf(topologyPointerNode{})})
			if err != nil {
				t.Fatal(err)
			}
			dest := &[]topologyPointerNode{}
			root := NewCollector(graph.Root, dest, false)
			if err = root.EnableProvenance(); err != nil {
				t.Fatal(err)
			}
			fixture := evidenceSQL{t, h}
			loaded, omitted := root.PartitionCopy(), root.PartitionCopy()
			fixture.scan(loaded, "SELECT id,parent_id,name FROM records WHERE id IN(1,2,4) ORDER BY id")
			fixture.scan(omitted, "SELECT id,parent_id FROM records WHERE id IN(3,5) ORDER BY id")
			for _, partition := range []*Collector{loaded, omitted} {
				if err = root.AppendPartition(context.Background(), partition); err != nil {
					t.Fatal(err)
				}
			}
			if err = root.AssembleTrees(); err != nil {
				t.Fatal(err)
			}
			ctx := context.WithValue(context.Background(), topologyModeKey{}, tc.mode)
			if err = root.RunOnRelation(ctx); err != nil {
				t.Fatal(err)
			}
			projection, err := root.Provenance("", true)
			if err != nil {
				t.Fatal(err)
			}
			rows, ok := projection.Rows()[0].Relation("Children")
			if !ok || len(rows) != len(tc.ids) || len((*dest)[0].Children) != len(tc.ids) {
				t.Fatalf("children count data=%d metadata=%d", len((*dest)[0].Children), len(rows))
			}
			for i, row := range rows {
				if (*dest)[0].Children[i].ID != tc.ids[i] || row.Fields().Known() != tc.known[i] || row.Fields().Has("Name") != tc.name[i] {
					t.Fatalf("child%d ID=%d known=%v name=%v", i, (*dest)[0].Children[i].ID, row.Fields().Known(), row.Fields().Has("Name"))
				}
			}
			if tc.mode == "nested reorder" {
				leaves, known := rows[0].Relation("Children")
				if !known || len(leaves) != 2 || (*dest)[0].Children[0].Children[0].ID != 5 || leaves[0].Fields().Has("Name") || !leaves[1].Fields().Has("Name") {
					t.Fatal("nested pointer reorder lost source-specific schema")
				}
			}
			if tc.mode == "scalar mutation" && ((*dest)[0].Children[0].Name == nil || *(*dest)[0].Children[0].Name != "changed by hook" || !rows[0].Fields().Has("Name")) {
				t.Fatal("loaded-field evidence was confused with immutable original values")
			}
		})
	}
}

type topologyParent struct {
	ID       int
	Children []*topologyPointerNode `sqlx:"-"`
}

func (r *topologyParent) OnRelation(context.Context) {
	if len(r.Children) > 1 {
		r.Children[0], r.Children[1] = r.Children[1], r.Children[0]
	}
}

func TestHookProvenanceUsesCompletedRelationAttachmentsSQLite(t *testing.T) {
	for _, strategy := range []data.MatchStrategy{data.MatchSequential, data.MatchReadAll} {
		t.Run(map[data.MatchStrategy]string{data.MatchSequential: "streaming", data.MatchReadAll: "read_all"}[strategy], func(t *testing.T) {
			h := sqlite.New(t)
			if err := h.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER,parent_id INTEGER,name TEXT)", "INSERT INTO records VALUES(2,1,'two'),(3,1,NULL),(11,10,'eleven'),(12,10,NULL)"); err != nil {
				t.Fatal(err)
			}
			child := &data.View{Spec: spec.View{Name: "children"}}
			parent := &data.View{Spec: spec.View{Name: "parents", RelationalConcurrency: 4}, Relations: []*data.Relation{{Name: "Children", Holder: "Children", Cardinality: spec.CardinalityMany, On: data.Links{data.NewLink("", "id", "ID")}, Of: &data.RelationRef{View: child, On: data.Links{data.NewLink("", "parent_id", "ParentID")}, MatchStrategy: strategy}}}}
			graph, err := Compile(parent, map[*data.View]reflect.Type{parent: reflect.TypeOf(topologyParent{}), child: reflect.TypeOf(topologyPointerNode{})})
			if err != nil {
				t.Fatal(err)
			}
			dest := &[]topologyParent{}
			root := NewCollector(graph.Root, dest, false)
			if err = root.EnableProvenance(); err != nil {
				t.Fatal(err)
			}
			fixture := evidenceSQL{t, h}
			fixture.scan(root, "SELECT 1 AS id UNION ALL SELECT 10 AS id")
			root.Fetched()
			relation := root.Relations(nil)[0]
			first, second := relation.PartitionCopy(), relation.PartitionCopy()
			fixture.scan(first, "SELECT id,parent_id,name FROM records WHERE id IN(2,11) ORDER BY id")
			fixture.scan(second, "SELECT id,parent_id FROM records WHERE id IN(3,12) ORDER BY id")
			for _, batch := range []*Collector{first, second} {
				if err = relation.AppendPartition(context.Background(), batch); err != nil {
					t.Fatal(err)
				}
			}
			if err = root.MergeData(); err != nil {
				t.Fatal(err)
			}
			if err = relation.RebindToParent(); err != nil {
				t.Fatal(err)
			}
			if err = root.RunOnRelation(context.Background()); err != nil {
				t.Fatal(err)
			}
			projection, err := root.Provenance("Data", false)
			if err != nil {
				t.Fatal(err)
			}
			for i, row := range projection.Rows() {
				children, ok := row.Relation("Children")
				if !ok || len(children) != 2 || len((*dest)[i].Children) != 2 || children[0].Fields().Has("Name") || !children[1].Fields().Has("Name") || !children[0].Fields().Has("ID") {
					t.Fatalf("parent%d completed batch association was not preserved", i)
				}
			}
		})
	}
}
