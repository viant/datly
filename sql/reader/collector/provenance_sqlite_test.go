package collector

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/reader/readmeta"
	sqlxio "github.com/viant/sqlx/io"
	sqlxread "github.com/viant/sqlx/io/read"
)

type evidenceNode struct {
	ID       int            `sqlx:"id|row_id"`
	ParentID *int           `sqlx:"parent_id"`
	Name     *string        `sqlx:"name"`
	Children []evidenceNode `sqlx:"-"`
}
type evidenceParent struct {
	ID       int            `sqlx:"id"`
	Children []evidenceNode `sqlx:"-"`
}

type evidenceHookNode struct {
	ID       int
	ParentID *int
	Children []evidenceHookNode `sqlx:"-"`
}

func (r *evidenceHookNode) OnRelation(context.Context) {
	if r.ID == 1 {
		r.Children = append(r.Children, evidenceHookNode{ID: 999})
	}
}

type evidenceSQL struct {
	t  *testing.T
	db *sqlite.Harness
}

func (f evidenceSQL) scan(c *Collector, query string) {
	f.t.Helper()
	var fields *readmeta.Fields
	reader, err := sqlxread.New(context.Background(), f.db.DB, query, c.NewItem(), sqlxread.WithColumnsObserver(func(columns []sqlxio.Column) error {
		matched, err := sqlxio.NewMatcher(nil).Match(c.view.Schema.RowType(), columns)
		if err != nil {
			return err
		}
		indexes := make([][]int, len(matched))
		for i, field := range matched {
			indexes[i] = field.FieldIndex()
		}
		fields = readmeta.NewFields(c.view.Schema.RowType(), indexes)
		return nil
	}))
	if err != nil {
		f.t.Fatal(err)
	}
	visitor := c.Visitor(context.Background())
	if err = reader.QueryAll(context.Background(), func(row any) error { c.RecordFields(fields); return visitor(row) }); err != nil {
		f.t.Fatal(err)
	}
}

func TestCollectorReadEvidenceSurvivesValueGrowthAndPartitionProjectionSQLite(t *testing.T) {
	for _, pointer := range []bool{false, true} {
		t.Run(map[bool]string{false: "values", true: "pointers"}[pointer], func(t *testing.T) {
			h := sqlite.New(t)
			if err := h.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER,name TEXT)", "WITH RECURSIVE seq(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM seq WHERE i<299) INSERT INTO records SELECT i,NULL FROM seq"); err != nil {
				t.Fatal(err)
			}
			typ := reflect.TypeOf(evidenceNode{})
			if pointer {
				typ = reflect.PointerTo(typ)
			}
			view := newTestView(&data.View{Spec: spec.View{Name: "records"}}, typ)
			dest := reflect.New(reflect.SliceOf(typ))
			collector := NewCollector(view, dest.Interface(), false)
			if err := collector.EnableProvenance(); err != nil {
				t.Fatal(err)
			}
			first, second := collector.PartitionCopy(), collector.PartitionCopy()
			fixture := evidenceSQL{t, h}
			fixture.scan(first, "SELECT id AS row_id,name FROM records WHERE id<200 ORDER BY id")
			fixture.scan(second, "SELECT id FROM records WHERE id>=200 ORDER BY id")
			for _, part := range []*Collector{first, second} {
				if err := collector.AppendPartition(context.Background(), part); err != nil {
					t.Fatal(err)
				}
			}
			projection, err := collector.Provenance("", true)
			if err != nil {
				t.Fatal(err)
			}
			rows := projection.Rows()
			if len(rows) != 300 || dest.Elem().Len() != 300 {
				t.Fatalf("lengths evidence=%d data=%d", len(rows), dest.Elem().Len())
			}
			for i, row := range rows {
				if !row.Fields().Has("ID") || row.Fields().Has("Name") != (i < 200) || row.Fields().Has("ParentID") {
					t.Fatalf("row%d fields incorrect", i)
				}
				value := dest.Elem().Index(i)
				if pointer {
					value = value.Elem()
				}
				if value.FieldByName("ID").Int() != int64(i) || !value.FieldByName("Name").IsNil() {
					t.Fatalf("row%d data incorrect", i)
				}
			}
			rows[0] = nil
			if projection.Rows()[0] == nil {
				t.Fatal("mutable rows escaped")
			}
			var reduced any = []evidenceNode{{ID: 999}}
			if pointer {
				reduced = []*evidenceNode{{ID: 999}}
			}
			if err := collector.AppendSlice(context.Background(), reduced); err != nil {
				t.Fatal(err)
			}
			withReduced, err := collector.Provenance("", true)
			if err != nil {
				t.Fatal(err)
			}
			if len(withReduced.Rows()) != 301 || withReduced.Rows()[300].Fields().Known() || !withReduced.Rows()[0].Fields().Known() {
				t.Fatal("reducer inherited query schema or destroyed retained evidence")
			}
			collector.SetDest(collector.DestPtr())
			replaced, err := collector.Provenance("", true)
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range replaced.Rows() {
				if row.Fields().Known() {
					t.Fatal("replacement inherited loaded fields")
				}
			}
		})
	}
}

func TestCollectorReadEvidenceFollowsRelationRebindAndSelfCopiesSQLite(t *testing.T) {
	for _, readAll := range []bool{false, true} {
		t.Run(map[bool]string{false: "streaming", true: "read_all"}[readAll], func(t *testing.T) {
			h := sqlite.New(t)
			if err := h.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER,parent_id INTEGER,name TEXT)", "INSERT INTO records VALUES(1,NULL,'root'),(2,1,NULL),(3,2,'leaf')"); err != nil {
				t.Fatal(err)
			}
			child := &data.View{Spec: spec.View{Name: "nodes", SelfReference: &spec.SelfReference{Holder: "Children", Child: "ID", Parent: "ParentID"}}}
			strategy := data.MatchSequential
			if readAll {
				strategy = data.MatchReadAll
			}
			parent := &data.View{Spec: spec.View{Name: "parent"}, Relations: []*data.Relation{{Name: "Children", Holder: "Children", Cardinality: spec.CardinalityMany, On: data.Links{data.NewLink("", "id", "ID")}, Of: &data.RelationRef{View: child, On: data.Links{data.NewLink("", "parent_id", "ParentID")}, MatchStrategy: strategy}}}}
			graph, err := Compile(parent, map[*data.View]reflect.Type{parent: reflect.TypeOf(evidenceParent{}), child: reflect.TypeOf(evidenceNode{})})
			if err != nil {
				t.Fatal(err)
			}
			dest := &[]evidenceParent{}
			root := NewCollector(graph.Root, dest, false)
			var relation *Collector
			if readAll {
				relation = root.Relations(nil)[0]
			}
			if err = root.EnableProvenance(); err != nil {
				t.Fatal(err)
			}
			fixture := evidenceSQL{t, h}
			fixture.scan(root, "SELECT 0 AS id")
			root.Fetched()
			if relation == nil {
				relation = root.Relations(nil)[0]
			}
			fixture.scan(relation, "SELECT id,COALESCE(parent_id,0) AS parent_id,name FROM records ORDER BY id DESC")
			if err = root.MergeData(); err != nil {
				t.Fatal(err)
			}
			if err = root.AssembleTrees(); err != nil {
				t.Fatal(err)
			}
			if err = relation.RebindToParent(); err != nil {
				t.Fatal(err)
			}
			projection, err := root.Provenance("Data", false)
			if err != nil {
				t.Fatal(err)
			}
			rows, ok := projection.Rows()[0].Relation("Children")
			if !ok || len(rows) != 1 || len((*dest)[0].Children) != 1 {
				t.Fatalf("root relation: %+v %v data=%+v", rows, ok, dest)
			}
			for depth := 0; depth < 3; depth++ {
				if len(rows) != 1 || !rows[0].Fields().Has("Name") || !rows[0].Fields().Has("ParentID") {
					t.Fatalf("depth%d missing read fields", depth)
				}
				rows, ok = rows[0].Relation("Children")
				if !ok {
					t.Fatalf("depth%d missing tree attachment evidence", depth)
				}
			}
		})
	}
}

func TestCollectorOpaqueHookTopologyDoesNotInheritReadEvidenceSQLite(t *testing.T) {
	h := sqlite.New(t)
	if err := h.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER,parent_id INTEGER)", "INSERT INTO records VALUES(1,NULL),(2,1)"); err != nil {
		t.Fatal(err)
	}
	metadata := &data.View{Spec: spec.View{Name: "nodes", SelfReference: &spec.SelfReference{Holder: "Children", Child: "ID", Parent: "ParentID"}}}
	graph, err := Compile(metadata, map[*data.View]reflect.Type{metadata: reflect.TypeOf(evidenceHookNode{})})
	if err != nil {
		t.Fatal(err)
	}
	dest := &[]evidenceHookNode{}
	root := NewCollector(graph.Root, dest, false)
	if err = root.EnableProvenance(); err != nil {
		t.Fatal(err)
	}
	(evidenceSQL{t, h}).scan(root, "SELECT id,parent_id FROM records ORDER BY id")
	if err = root.AssembleTrees(); err != nil {
		t.Fatal(err)
	}
	if err = root.RunOnRelation(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len((*dest)[0].Children) != 2 || (*dest)[0].Children[1].ID != 999 {
		t.Fatalf("hook output=%+v", dest)
	}
	projection, err := root.Provenance("", true)
	if err != nil {
		t.Fatal(err)
	}
	row := projection.Rows()[0]
	if row.Fields().Known() {
		t.Fatal("opaque value root was treated as a stable row identity")
	}
	children, known := row.Relation("Children")
	if !known || len(children) != 2 {
		t.Fatal("final opaque holder ordinals were lost")
	}
	for _, child := range children {
		if child.Fields().Known() {
			t.Fatal("opaque value-holder row inherited loaded evidence")
		}
	}
}
