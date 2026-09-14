package reader_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
	xreader "github.com/viant/xdatly/reader"
)

type graphNode struct {
	ID          int          `sqlx:"id"`
	ParentID    *int         `sqlx:"parent_id"`
	BucketID    int          `sqlx:"bucket_id"`
	Family      string       `sqlx:"family"`
	Children    []*graphNode `sqlx:"-" self:"child=id,parent=parent_id"`
	Related     bool         `sqlx:"-"`
	OrderBroken bool         `sqlx:"-"`
}

func (n *graphNode) OnFetch(ctx context.Context) error {
	parent, _ := ctx.Value(reflect.TypeOf(&graphGroup{})).(*graphGroup)
	if parent == nil || !parent.Fetched {
		return fmt.Errorf("parent was not published after its fetch hook")
	}
	if n.Family == "b" {
		sync, _ := ctx.Value(xhandler.DataSyncKey).(*xhandler.DataSync)
		if sync == nil {
			return fmt.Errorf("sibling DataSync was not published")
		}
		sync.Wait("First")
		if len(parent.First) == 0 {
			return fmt.Errorf("first relation unavailable after synchronization")
		}
	}
	return nil
}
func (n *graphNode) OnRelation(context.Context) {
	if n.Related {
		n.OrderBroken = true
	}
	for _, child := range n.Children {
		if !child.Related {
			n.OrderBroken = true
		}
	}
	n.Related = true
}

type graphGroup struct {
	ID          int          `sqlx:"id"`
	First       []*graphNode `sqlx:"-" view:"First,publishParent=true,limit=5000,batch=1,batchConcurrency=2" on:"ID:id=BucketID:bucket_id" sql:"SELECT id,parent_id,bucket_id,family FROM nodes WHERE family='a' AND $COLUMN_IN ORDER BY id"`
	Second      []*graphNode `sqlx:"-" view:"Second,publishParent=true,limit=5000,batch=1,batchConcurrency=2" on:"ID:id=BucketID:bucket_id" sql:"SELECT id,parent_id,bucket_id,family FROM nodes WHERE family='b' AND $COLUMN_IN ORDER BY id"`
	Fetched     bool         `sqlx:"-"`
	Related     bool         `sqlx:"-"`
	OrderBroken bool         `sqlx:"-"`
}

func (g *graphGroup) OnFetch(context.Context) error { g.Fetched = true; return nil }
func (g *graphGroup) OnRelation(context.Context) {
	if g.Related {
		g.OrderBroken = true
	}
	for _, roots := range [][]*graphNode{g.First, g.Second} {
		for _, node := range roots {
			if !node.Related {
				g.OrderBroken = true
			}
		}
	}
	g.Related = true
}

type graphCount struct {
	Total int `sqlx:"total"`
}
type graphAggregate struct {
	SumID int `sqlx:"sum_id"`
	MaxID int `sqlx:"max_id"`
}
type selfGraphOutput struct {
	Rows      []*graphGroup   `parameter:"Rows,kind=output,in=view" view:"Groups" sql:"SELECT id FROM groups ORDER BY id"`
	Count     *graphCount     `parameter:"Count,kind=output,in=summary" view:"Count" sql:"SELECT COUNT(*) AS total FROM ($View.Groups.NonWindowSQL) source"`
	Aggregate *graphAggregate `parameter:"Aggregate,kind=output,in=summary" view:"Aggregate" sql:"SELECT SUM(id) AS sum_id,MAX(id) AS max_id FROM ($View.Groups.NonWindowSQL) source"`
}

type selfGraphPartitions struct{}

func (selfGraphPartitions) Partitions(context.Context, xreader.PartitionRequest) ([]xreader.Partition, error) {
	return []xreader.Partition{{Expression: "id <= ?", Args: []any{20032}}, {Expression: "id > ?", Args: []any{20032}}}, nil
}

func TestMultipleSelfTreesSummariesAndHooksThroughRuntimeSQLite(t *testing.T) {
	for _, test := range []struct {
		name      string
		depth     int
		paged     bool
		partition bool
	}{{"paged_64", 64, true, false}, {"all_16", 16, false, false}, {"deep_self_1024", 1024, true, false}, {"partitioned_paged", 64, true, true}, {"partitioned_batches", 16, false, true}} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE groups(id INTEGER)", "INSERT INTO groups VALUES(1),(2),(3)", "CREATE TABLE nodes(id INTEGER,parent_id INTEGER,bucket_id INTEGER,family TEXT)", fmt.Sprintf("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<%d),families(f,k) AS(SELECT 'a',0 UNION ALL SELECT 'b',2000) INSERT INTO nodes SELECT g.id*10000+f.k+n.i, CASE WHEN n.i=1 THEN NULL ELSE g.id*10000+f.k+n.i-1 END,g.id,f.f FROM groups g CROSS JOIN n CROSS JOIN families f", test.depth)); err != nil {
				t.Fatal(err)
			}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Forest"}, Name: "Forest", Routes: []*spec.Route{{Method: "GET", Path: "/graph"}}, RootView: &spec.View{Name: "Groups", Source: &spec.ViewSource{SQL: "SELECT id FROM groups ORDER BY id"}}}
			if test.paged {
				limit, offset := 1, 1
				component.RootView.Source.Controls = &spec.ViewControls{Limit: &limit, Offset: &offset}
			}
			var catalog *typecatalog.Catalog
			if test.partition {
				catalog = typecatalog.NewCatalog()
				descriptor := xshape.Linked(reflect.TypeOf(selfGraphPartitions{})).Descriptor()
				if err := catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
					t.Fatal(err)
				}
				for i, holder := range []string{"First", "Second"} {
					family := "a"
					if i == 1 {
						family = "b"
					}
					component.RootView.Relations = append(component.RootView.Relations, &spec.Relation{Name: holder, Holder: holder, Cardinality: spec.CardinalityMany, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "bucket_id"}}, View: &spec.View{Name: holder, PublishParent: true, BatchSize: 1, BatchConcurrency: 2, Partitioning: &spec.Partitioning{Type: descriptor.Key(), Concurrency: 2}, Source: &spec.ViewSource{SQL: fmt.Sprintf("SELECT id,parent_id,bucket_id,family FROM nodes WHERE family='%s' AND $COLUMN_IN ORDER BY id", family)}}})
				}
			}
			actual := (typedGraphFixture{db: db, component: component, output: reflect.TypeOf(selfGraphOutput{}), types: catalog}).invoke(t, ctx).(*selfGraphOutput)
			wantRows := 3
			if test.paged {
				wantRows = 1
			}
			if len(actual.Rows) != wantRows || actual.Count == nil || actual.Count.Total != 3 || actual.Aggregate == nil || actual.Aggregate.SumID != 6 || actual.Aggregate.MaxID != 3 {
				t.Fatalf("pagination/summary output %+v", actual)
			}
			for index, group := range actual.Rows {
				wantID := index + 1
				if test.paged {
					wantID = 2
				}
				if group.ID != wantID || !group.Fetched || !group.Related || group.OrderBroken {
					t.Fatalf("group lifecycle %+v", group)
				}
				for family, roots := range map[string][]*graphNode{"a": group.First, "b": group.Second} {
					for level := 1; level <= test.depth; level++ {
						if len(roots) != 1 {
							t.Fatalf("group%d family%s depth%d roots%d", group.ID, family, level, len(roots))
						}
						node := roots[0]
						base := group.ID * 10000
						if family == "b" {
							base += 2000
						}
						if node.ID != base+level || node.Family != family || !node.Related || node.OrderBroken {
							t.Fatalf("self node lifecycle %+v", node)
						}
						roots = node.Children
					}
					if len(roots) != 0 {
						t.Fatal("self tree has unexpected tail")
					}
				}
			}
		})
	}
}
