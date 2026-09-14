package reader_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap/cacheconfig"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/sqlx/io/read/cache"
	xreader "github.com/viant/xdatly/reader"
)

type codecChild struct {
	ParentID int      `sqlx:"parent_id"`
	Tags     []string `sqlx:"tags,type=string" codec:"split"`
}
type codecParent struct {
	ID       int           `sqlx:"id"`
	Tags     []string      `sqlx:"tags,type=string" codec:"split"`
	Children []*codecChild `sqlx:"-"`
}
type codecSummary struct {
	Tags []string `sqlx:"tags,type=string" codec:"split"`
}
type codecGraphOutput struct {
	Rows    []*codecParent
	Summary *codecSummary
}
type codecPartitions struct{}

func (codecPartitions) Partitions(context.Context, xreader.PartitionRequest) ([]xreader.Partition, error) {
	return []xreader.Partition{{Expression: "id <= ?", Args: []any{1}}, {Expression: "id > ?", Args: []any{1}}}, nil
}

func TestColumnCodecGraphSQLite(t *testing.T) {
	for _, tc := range []struct {
		name            string
		partition, fail bool
	}{{"relations_and_summary", false, false}, {"partitioned", true, false}, {"relation_error", false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER,tags TEXT)", "CREATE TABLE children(parent_id INTEGER,tags TEXT)", "INSERT INTO parents VALUES(1,'p1'),(2,'p2')", "INSERT INTO children VALUES(1,'c1'),(2,'c2')"); err != nil {
				t.Fatal(err)
			}
			if tc.fail {
				if err := db.ExecStatements(ctx, "UPDATE children SET tags='bad' WHERE parent_id=1"); err != nil {
					t.Fatal(err)
				}
			}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "CodecGraph"}, RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT id,tags FROM parents ORDER BY id"}, Relations: []*spec.Relation{
				{Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}, View: &spec.View{Name: "children", Source: &spec.ViewSource{SQL: "SELECT parent_id,tags FROM children WHERE $COLUMN_IN ORDER BY parent_id"}}},
				{Name: "summary", Holder: "Summary", Kind: spec.RelationKindDerived, Cardinality: spec.CardinalityOne, View: &spec.View{Name: "summary", Source: &spec.ViewSource{SQL: "SELECT group_concat(tags, ',') AS tags FROM ($View.NonWindowSQL) parent"}}},
			}}}
			if tc.partition {
				component.RootView.Partitioning = &spec.Partitioning{Type: "codecPartitions", Concurrency: 2}
			}
			factory := &splitCodecFactory{}
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(codecGraphOutput{}), DirectViewField: "Rows", CodecFactory: factory, TypeLookup: func(string) (reflect.Type, error) { return reflect.TypeOf(codecPartitions{}), nil }})
			if err != nil {
				t.Fatal(err)
			}
			service, err := (cacheconfig.Config{Identity: "graph/raw", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
			if err != nil {
				t.Fatal(err)
			}
			services := map[*data.View]cache.Cache{plan.Root.View: service}
			for _, relation := range plan.Root.Relations {
				services[relation.Target.View] = service
			}
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(codecGraphOutput{}), Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}, ReadCaches: services})
			if err != nil {
				t.Fatal(err)
			}
			for pass := 1; pass <= 2; pass++ {
				actual, err := execution.Read(ctx, &struct{}{}, nil, nil)
				if tc.fail {
					if err == nil || !strings.Contains(err.Error(), "decode column Tags") {
						t.Fatalf("error=%v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				want := &codecGraphOutput{Rows: []*codecParent{{ID: 1, Tags: []string{"p1"}, Children: []*codecChild{{ParentID: 1, Tags: []string{"c1"}}}}, {ID: 2, Tags: []string{"p2"}, Children: []*codecChild{{ParentID: 2, Tags: []string{"c2"}}}}}, Summary: &codecSummary{Tags: []string{"p1", "p2"}}}
				if !reflect.DeepEqual(actual, want) {
					t.Fatalf("pass=%d actual=%#v", pass, actual)
				}
				if factory.calls.Load() != int32(pass*5) {
					t.Fatalf("codec calls=%d", factory.calls.Load())
				}
				if factory.builds.Load() != 3 {
					t.Fatalf("codec plans recompiled: %d", factory.builds.Load())
				}
				if pass == 1 {
					if err := db.ExecStatements(ctx, "DROP TABLE children", "DROP TABLE parents"); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}
