package reader

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap/cacheconfig"
	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader/collector"
	"github.com/viant/sqlx/io/read/cache"
	xreader "github.com/viant/xdatly/reader"
)

type warmupPartitions struct{}

func (warmupPartitions) Partitions(context.Context, xreader.PartitionRequest) ([]xreader.Partition, error) {
	return []xreader.Partition{{Expression: "id <= ?", Args: []any{2}}, {Expression: "id > ?", Args: []any{2}}}, nil
}

func TestGraphWarmupSQLite(t *testing.T) {
	type input struct{}
	type row struct {
		ID int `sqlx:"id"`
	}
	type summary struct {
		Total int `sqlx:"total"`
	}
	type output struct {
		Rows    []row
		Summary *summary
	}
	for _, tc := range []struct {
		name               string
		partition, summary bool
	}{{"partitioned", true, false}, {"summary", false, true}, {"partitioned_summary", true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(1),(2),(3)"); err != nil {
				t.Fatal(err)
			}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Records"}, RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records ORDER BY id"}}}
			view := data.FromComponent(component)
			view.Cache = &data.Cache{Warmup: &spec.CacheWarmupSettings{IndexMeta: tc.summary}}
			rowTypes := map[*data.View]reflect.Type{view: reflect.TypeOf(row{})}
			if tc.summary {
				meta := &data.View{Spec: spec.View{Name: "summary", Source: &spec.ViewSource{SQL: "SELECT COUNT(*) AS total FROM ($View.NonWindowSQL) parent"}}}
				view.Relations = []*data.Relation{{Name: "summary", Holder: "Summary", Kind: spec.RelationKindDerived, Cardinality: spec.CardinalityOne, Of: &data.RelationRef{View: meta}}}
				rowTypes[meta] = reflect.TypeOf(summary{})
			}
			partitioners := map[*data.View]xreader.Partitioner{}
			if tc.partition {
				view.Spec.Partitioning = &spec.Partitioning{Concurrency: 2}
				partitioners[view] = warmupPartitions{}
			}
			graph, err := collector.Compile(view, rowTypes)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := NewPlan(PlanConfig{RootView: view, ViewIndex: NewViewIndex(component, view), Collection: graph, OutputViewField: "Rows", Partitioners: partitioners})
			if err != nil {
				t.Fatal(err)
			}
			services := map[*data.View]cache.Cache{}
			for candidate := range rowTypes {
				service, err := (cacheconfig.Config{Identity: "Records/" + candidate.Spec.Name, Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
				if err != nil {
					t.Fatal(err)
				}
				services[candidate] = service
			}
			execution, err := NewExecution(Config{Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}, ReadCaches: services})
			if err != nil {
				t.Fatal(err)
			}
			wantCount := 1
			if tc.partition {
				wantCount++
			}
			if tc.summary {
				wantCount++
			}
			if count, err := execution.Warmup(ctx, dexec.ReaderWarmupInvocation{Input: &input{}}); err != nil || count != wantCount {
				t.Fatalf("Warmup=%d,%v", count, err)
			}
			if err := db.ExecStatements(ctx, "DROP TABLE records"); err != nil {
				t.Fatal(err)
			}
			actual, err := execution.Read(ctx, &input{}, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			result := actual.(*output)
			if !reflect.DeepEqual(result.Rows, []row{{1}, {2}, {3}}) {
				t.Fatalf("rows=%v", result.Rows)
			}
			if tc.summary && (result.Summary == nil || result.Summary.Total != 3) {
				t.Fatalf("summary=%+v", result.Summary)
			}
		})
	}
}
