package collector

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
)

type rootAliasKey struct{}
type rootAliasState struct {
	mode string
	rows *[]*rootAliasRow
}

type childRootAliasKey struct{}
type childRootAliasState struct {
	rows *[]*childRootAliasParent
	once sync.Once
	mode string
}
type childRootAliasParent struct {
	ID       int
	Children []*childRootAliasChild `sqlx:"-"`
	Calls    int                    `sqlx:"-"`
}

func (p *childRootAliasParent) OnRelation(context.Context) { p.Calls++ }

type childRootAliasChild struct {
	ID, ParentID int
	Children     []*childRootAliasChild `sqlx:"-"`
}

func (*childRootAliasChild) OnRelation(ctx context.Context) {
	state := ctx.Value(childRootAliasKey{}).(*childRootAliasState)
	state.once.Do(func() {
		switch state.mode {
		case "reorder":
			rows := *state.rows
			rows[0], rows[1] = rows[1], rows[0]
		case "nil slot":
			(*state.rows)[1] = nil
		default:
			*state.rows = nil
		}
	})
}

func TestChildHookRootAliasMutationDoesNotChangeFetchedParentTargetsSQLite(t *testing.T) {
	for _, mode := range []string{"nil roots", "reorder", "nil slot"} {
		t.Run(mode, func(t *testing.T) {
			h := sqlite.New(t)
			if err := h.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER,parent_id INTEGER)", "INSERT INTO records VALUES(11,1),(21,2)"); err != nil {
				t.Fatal(err)
			}
			child := &data.View{Spec: spec.View{Name: "children", RelationalConcurrency: 2, SelfReference: &spec.SelfReference{Holder: "Children", Child: "ID", Parent: "ParentID"}}}
			parent := &data.View{Spec: spec.View{Name: "parents"}, Relations: []*data.Relation{{Name: "Children", Holder: "Children", Cardinality: spec.CardinalityMany, On: data.Links{data.NewLink("", "id", "ID")}, Of: &data.RelationRef{View: child, On: data.Links{data.NewLink("", "parent_id", "ParentID")}, MatchStrategy: data.MatchReadAll}}}}
			graph, err := Compile(parent, map[*data.View]reflect.Type{parent: reflect.TypeOf((*childRootAliasParent)(nil)), child: reflect.TypeOf(childRootAliasChild{})})
			if err != nil {
				t.Fatal(err)
			}
			dest := &[]*childRootAliasParent{}
			root := NewCollector(graph.Root, dest, false)
			if err = root.EnableProvenance(); err != nil {
				t.Fatal(err)
			}
			fixture := evidenceSQL{t, h}
			fixture.scan(root, "SELECT 1 AS id UNION ALL SELECT 2 AS id")
			root.Fetched()
			fixture.scan(root.Relations(nil)[0], "SELECT id,parent_id FROM records")
			if err = root.MergeData(); err != nil {
				t.Fatal(err)
			}
			if err = root.AssembleTrees(); err != nil {
				t.Fatal(err)
			}
			original := append([]*childRootAliasParent(nil), (*dest)...)
			ctx := context.WithValue(context.Background(), childRootAliasKey{}, &childRootAliasState{rows: dest, mode: mode})
			if err = root.RunOnRelation(ctx); err != nil {
				t.Fatal(err)
			}
			if mode == "nil roots" && len(*dest) != 0 {
				t.Fatal("child hook did not clear aliased roots")
			}
			if mode == "reorder" && ((*dest)[0].ID != 2 || (*dest)[1].ID != 1) {
				t.Fatal("child hook did not reorder roots")
			}
			if mode == "nil slot" && (*dest)[1] != nil {
				t.Fatal("child hook did not nil a root slot")
			}
			for _, parent := range original {
				if parent.Calls != 1 {
					t.Fatalf("fetched parent%d hooks=%d", parent.ID, parent.Calls)
				}
				if len(parent.Children) != 1 || parent.Children[0].ParentID != parent.ID {
					t.Fatalf("self rebind attached to wrong frozen parent: %+v", parent)
				}
			}
			projection, err := root.Provenance("", true)
			if err != nil || len(projection.Rows()) != len(*dest) {
				t.Fatalf("projection=%+v err=%v", projection, err)
			}
			for i, row := range projection.Rows() {
				if (*dest)[i] == nil {
					if row.Fields().Known() {
						t.Fatal("nil root inherited fields")
					}
					continue
				}
				children, known := row.Relation("Children")
				if !row.Fields().Has("ID") || !known || len(children) != 1 || !children[0].Fields().Has("ParentID") {
					t.Fatalf("retained parent%d lost proven child fields", i)
				}
			}
		})
	}
}

type rootAliasRow struct {
	ID    int
	Name  *string
	Calls int `sqlx:"-"`
}

func (r *rootAliasRow) OnRelation(ctx context.Context) {
	r.Calls++
	if r.ID != 1 {
		return
	}
	state := ctx.Value(rootAliasKey{}).(*rootAliasState)
	rows := *state.rows
	switch state.mode {
	case "reorder":
		rows[0], rows[1] = rows[1], rows[0]
	case "truncate":
		rows = rows[:1]
	case "nil slot":
		rows[1] = nil
	case "nil roots":
		rows = nil
	case "equal replacement":
		copy := *rows[0]
		rows[0] = &copy
	case "duplicate":
		rows = append(rows, rows[0])
	case "append":
		rows = append(rows, &rootAliasRow{ID: 3})
	}
	*state.rows = rows
}

func TestHookRootPointerIdentityAndFrozenVisitationSQLite(t *testing.T) {
	for _, tc := range []struct {
		mode         string
		ids          []int
		known, names []bool
	}{
		{"read only", []int{1, 2}, []bool{true, true}, []bool{true, false}},
		{"reorder", []int{2, 1}, []bool{true, true}, []bool{false, true}},
		{"truncate", []int{1}, []bool{true}, []bool{true}},
		{"nil slot", []int{1, 0}, []bool{true, false}, []bool{true, false}},
		{"preexisting nil", []int{1, 0}, []bool{true, false}, []bool{true, false}},
		{"nil roots", nil, nil, nil},
		{"equal replacement", []int{1, 2}, []bool{false, true}, []bool{false, false}},
		{"duplicate", []int{1, 2, 1}, []bool{false, true, false}, []bool{false, false, false}},
		{"append", []int{1, 2, 3}, []bool{true, true, false}, []bool{true, false, false}},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			h := sqlite.New(t)
			if err := h.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER,name TEXT)", "INSERT INTO records VALUES(1,'one'),(2,'omitted')"); err != nil {
				t.Fatal(err)
			}
			view := newTestView(&data.View{Spec: spec.View{Name: "roots"}}, reflect.TypeOf((*rootAliasRow)(nil)))
			dest := &[]*rootAliasRow{}
			root := NewCollector(view, dest, false)
			if err := root.EnableProvenance(); err != nil {
				t.Fatal(err)
			}
			fixture := evidenceSQL{t, h}
			one, two := root.PartitionCopy(), root.PartitionCopy()
			fixture.scan(one, "SELECT id,name FROM records WHERE id=1")
			fixture.scan(two, "SELECT id FROM records WHERE id=2")
			for _, part := range []*Collector{one, two} {
				if err := root.AppendPartition(context.Background(), part); err != nil {
					t.Fatal(err)
				}
			}
			original := append([]*rootAliasRow(nil), (*dest)...)
			if tc.mode == "preexisting nil" {
				(*dest)[1] = nil
			}
			ctx := context.WithValue(context.Background(), rootAliasKey{}, &rootAliasState{mode: tc.mode, rows: dest})
			if err := root.RunOnRelation(ctx); err != nil {
				t.Fatal(err)
			}
			for _, row := range original {
				wantCalls := 1
				if tc.mode == "preexisting nil" && row.ID == 2 {
					wantCalls = 0
				}
				if row.Calls != wantCalls {
					t.Fatalf("fetched root%d called%d times", row.ID, row.Calls)
				}
			}
			projection, err := root.Provenance("", true)
			if err != nil {
				t.Fatal(err)
			}
			rows := projection.Rows()
			if len(rows) != len(tc.ids) || len(*dest) != len(tc.ids) {
				t.Fatalf("length data=%d evidence=%d", len(*dest), len(rows))
			}
			for i, row := range rows {
				id := 0
				if (*dest)[i] != nil {
					id = (*dest)[i].ID
				}
				if id != tc.ids[i] || row.Fields().Known() != tc.known[i] || row.Fields().Has("Name") != tc.names[i] {
					t.Fatalf("slot%d ID=%d known=%v Name=%v", i, id, row.Fields().Known(), row.Fields().Has("Name"))
				}
			}
			if tc.mode == "append" && (*dest)[2].Calls != 0 {
				t.Fatal("new root was visited as a fetched target")
			}
		})
	}
}

type valueRootAliasKey struct{}
type valueRootAliasState struct {
	rows    *[]valueRootAliasParent
	changed bool
}
type valueRootAliasParent struct {
	ID       int
	Children []*topologyPointerNode `sqlx:"-"`
}

func (*valueRootAliasParent) OnRelation(ctx context.Context) {
	state := ctx.Value(valueRootAliasKey{}).(*valueRootAliasState)
	if !state.changed {
		(*state.rows)[0], (*state.rows)[1] = (*state.rows)[1], (*state.rows)[0]
		state.changed = true
	}
}

func TestHookValueRootReorderDoesNotMoveChildEvidenceAcrossScopesSQLite(t *testing.T) {
	h := sqlite.New(t)
	if err := h.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER,parent_id INTEGER,name TEXT)", "INSERT INTO records VALUES(2,1,'two'),(11,10,'eleven')"); err != nil {
		t.Fatal(err)
	}
	child := &data.View{Spec: spec.View{Name: "children"}}
	parent := &data.View{Spec: spec.View{Name: "parents"}, Relations: []*data.Relation{{Name: "Children", Holder: "Children", Cardinality: spec.CardinalityMany, On: data.Links{data.NewLink("", "id", "ID")}, Of: &data.RelationRef{View: child, On: data.Links{data.NewLink("", "parent_id", "ParentID")}, MatchStrategy: data.MatchReadAll}}}}
	graph, err := Compile(parent, map[*data.View]reflect.Type{parent: reflect.TypeOf(valueRootAliasParent{}), child: reflect.TypeOf(topologyPointerNode{})})
	if err != nil {
		t.Fatal(err)
	}
	dest := &[]valueRootAliasParent{}
	root := NewCollector(graph.Root, dest, false)
	if err = root.EnableProvenance(); err != nil {
		t.Fatal(err)
	}
	fixture := evidenceSQL{t, h}
	fixture.scan(root, "SELECT 1 AS id UNION ALL SELECT 10 AS id")
	root.Fetched()
	relation := root.Relations(nil)[0]
	fixture.scan(relation, "SELECT id,parent_id,name FROM records ORDER BY id")
	if err = root.MergeData(); err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), valueRootAliasKey{}, &valueRootAliasState{rows: dest})
	if err = root.RunOnRelation(ctx); err != nil {
		t.Fatal(err)
	}
	if (*dest)[0].ID != 10 || (*dest)[1].ID != 1 {
		t.Fatal("fixture did not reorder value roots")
	}
	projection, err := root.Provenance("", true)
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range projection.Rows() {
		children, known := row.Relation("Children")
		if row.Fields().Known() || !known || len(children) != 1 || children[0].Fields().Known() {
			t.Fatalf("value root%d moved evidence across an unverifiable scope", i)
		}
	}
}

func TestHookValueParentsWithSharedChildPointerAreAmbiguousSQLite(t *testing.T) {
	h := sqlite.New(t)
	if err := h.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER,parent_id INTEGER,name TEXT)", "INSERT INTO records VALUES(2,1,'shared')"); err != nil {
		t.Fatal(err)
	}
	child := &data.View{Spec: spec.View{Name: "children"}}
	parent := &data.View{Spec: spec.View{Name: "parents"}, Relations: []*data.Relation{{Name: "Children", Holder: "Children", Cardinality: spec.CardinalityMany, On: data.Links{data.NewLink("", "id", "ID")}, Of: &data.RelationRef{View: child, On: data.Links{data.NewLink("", "parent_id", "ParentID")}, MatchStrategy: data.MatchReadAll}}}}
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
	fixture.scan(root, "SELECT 1 AS id UNION ALL SELECT 1 AS id")
	root.Fetched()
	relation := root.Relations(nil)[0]
	fixture.scan(relation, "SELECT id,parent_id,name FROM records")
	if err = root.MergeData(); err != nil {
		t.Fatal(err)
	}
	if (*dest)[0].Children[0] != (*dest)[1].Children[0] {
		t.Fatal("fixture does not contain an actual shared child pointer")
	}
	if err = root.RunOnRelation(context.Background()); err != nil {
		t.Fatal(err)
	}
	projection, err := root.Provenance("", true)
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range projection.Rows() {
		children, known := row.Relation("Children")
		if row.Fields().Known() || !known || len(children) != 1 || children[0].Fields().Known() {
			t.Fatalf("value parent%d claimed ambiguous root/child origin", i)
		}
	}
}
