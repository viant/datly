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
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/io/read/cache"
)

func TestExecutionWarmupSQLite(t *testing.T) {
	type input struct{ Tenant string }
	type row struct {
		ID int `sqlx:"id"`
	}
	type output struct{ Rows []row }
	for _, tc := range []struct {
		name, tenant string
		want         []row
	}{{"matching", "a", []row{{1}, {2}}}, {"other", "b", []row{{3}}}, {"empty", "none", []row{}}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,tenant TEXT)", "INSERT INTO records VALUES(1,'a'),(2,'a'),(3,'b')"); err != nil {
				t.Fatal(err)
			}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Records"}, RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records WHERE tenant=:tenant ORDER BY id"}}}
			view := data.FromComponent(component)
			view.Cache = &data.Cache{Warmup: &spec.CacheWarmupSettings{}}
			graph, err := collector.Compile(view, map[*data.View]reflect.Type{view: reflect.TypeOf(row{})})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := NewPlan(PlanConfig{RootView: view, ViewIndex: NewViewIndex(component, view), Collection: graph, OutputViewField: "Rows"})
			if err != nil {
				t.Fatal(err)
			}
			service, err := (cacheconfig.Config{Identity: "Records/root", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
			if err != nil {
				t.Fatal(err)
			}
			execution, err := NewExecution(Config{Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}, ReadCaches: map[*data.View]cache.Cache{view: service}})
			if err != nil {
				t.Fatal(err)
			}
			bound := &input{Tenant: tc.tenant}
			resolver := sqlx.ParameterResolver(func(name string) (any, bool, error) { return bound.Tenant, name == "tenant", nil })
			if count, err := execution.Warmup(ctx, dexec.ReaderWarmupInvocation{Input: bound, Parameters: resolver}); err != nil || count != 1 {
				t.Fatalf("Warmup=%d,%v", count, err)
			}
			if err := db.ExecStatements(ctx, "DROP TABLE records"); err != nil {
				t.Fatal(err)
			}
			actual, err := execution.Read(ctx, bound, nil, resolver)
			if err != nil {
				t.Fatal(err)
			}
			rows := actual.(*output).Rows
			if len(rows) != 0 || len(tc.want) != 0 {
				if !reflect.DeepEqual(rows, tc.want) {
					t.Fatalf("rows=%v,want %v", rows, tc.want)
				}
			}
		})
	}
}
