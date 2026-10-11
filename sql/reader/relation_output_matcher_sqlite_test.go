package reader_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap/cacheconfig"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/sqlx/io/read/cache"
)

type feeMatcherChild struct {
	ID       int  `sqlx:"ID"`
	ParentID *int `sqlx:"PARENT_ID" source:"ID"`
	TenantID int  `sqlx:"TENANT_ID" source:"TENANT"`
}

type feeMatcherParent struct {
	ID   int                `sqlx:"ID"`
	Fees []*feeMatcherChild `view:"fees" on:"ID:p.ID=ParentID:a.ID" sql:"SELECT f.id AS ID,a.id AS PARENT_ID FROM fees f JOIN parents a ON a.id=f.parent_id"`
}

type compositeFeeMatcherParent struct {
	ID     int                `sqlx:"ID"`
	Tenant int                `sqlx:"TENANT"`
	Fees   []*feeMatcherChild `view:"fees" on:"ID:p.ID=ParentID:a.ID,Tenant:p.TENANT=TenantID:a.TENANT" sql:"SELECT f.id AS ID,a.id AS PARENT_ID,a.tenant AS TENANT_ID FROM fees f JOIN parents a ON a.id=f.parent_id AND a.tenant=f.tenant"`
}

func TestCompositeRelationMatcherUsesLinkedOutputsSQLite(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(map[bool]string{false: "uncached", true: "cached"}[cached], func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(ctx,
				"CREATE TABLE parents(id INTEGER,tenant INTEGER)", "INSERT INTO parents VALUES(5,7),(6,8),(5,8)",
				"CREATE TABLE fees(id INTEGER,parent_id INTEGER,tenant INTEGER)", "INSERT INTO fees VALUES(1,5,7),(2,6,8),(99,5,8)"))
			component := &spec.Component{RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT p.id AS ID,p.tenant AS TENANT FROM parents p WHERE (p.id=5 AND p.tenant=7) OR p.id=6 ORDER BY p.id"}}}
			typ := reflect.TypeFor[[]*compositeFeeMatcherParent]()
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
			require.NoError(t, err)
			caches := map[*data.View]cache.Cache{}
			if cached {
				for _, view := range []*data.View{plan.Root.View, plan.Root.View.Relations[0].Of.View} {
					service, err := (cacheconfig.Config{Identity: "composite/" + view.Spec.Name, Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
					require.NoError(t, err)
					caches[view] = service
				}
			}
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}, ReadCaches: caches})
			require.NoError(t, err)
			for pass := 0; pass < 2; pass++ {
				result, err := execution.Read(ctx, &struct{}{}, nil, nil)
				require.NoError(t, err)
				rows := result.([]*compositeFeeMatcherParent)
				require.Len(t, rows, 2)
				for i, row := range rows {
					require.Len(t, row.Fees, 1, "pass=%d parent=%d", pass, row.ID)
					require.Equal(t, i+1, row.Fees[0].ID)
					require.Equal(t, row.ID, *row.Fees[0].ParentID)
					require.Equal(t, row.Tenant, row.Fees[0].TenantID)
				}
				if cached && pass == 0 {
					require.NoError(t, db.ExecStatements(ctx, "DROP TABLE parents", "DROP TABLE fees"))
				}
			}
		})
	}
}

func TestRelationMatcherUsesLinkedOutputSQLite(t *testing.T) {
	for _, rootSQL := range []string{
		"SELECT p.id AS ID FROM parents p ORDER BY p.id",
		"SELECT p.id AS ID FROM parents p WHERE p.id IN (5,6) ORDER BY p.id",
		"SELECT p.id AS ID FROM parents p WHERE p.id=5",
	} {
		for _, cached := range []bool{false, true} {
			t.Run(rootSQL+map[bool]string{false: "/uncached", true: "/cached"}[cached], func(t *testing.T) {
				ctx := context.Background()
				db := sqlite.New(t)
				require.NoError(t, db.ExecStatements(ctx,
					"CREATE TABLE parents(id INTEGER)", "INSERT INTO parents VALUES(1),(5),(6)",
					"CREATE TABLE fees(id INTEGER,parent_id INTEGER)", "INSERT INTO fees VALUES(1,5),(99,7)"))
				component := &spec.Component{RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: rootSQL}}}
				typ := reflect.TypeFor[[]*feeMatcherParent]()
				plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
				require.NoError(t, err)
				link := plan.Root.View.Relations[0].Of.On[0]
				require.Equal(t, "ID", link.Column)
				require.Equal(t, "PARENT_ID", link.OutputColumn())
				caches := map[*data.View]cache.Cache{}
				if cached {
					for _, view := range []*data.View{plan.Root.View, plan.Root.View.Relations[0].Of.View} {
						service, err := (cacheconfig.Config{Identity: "fees/" + view.Spec.Name, Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
						require.NoError(t, err)
						caches[view] = service
					}
				}
				execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}, ReadCaches: caches})
				require.NoError(t, err)
				for pass := 0; pass < 2; pass++ {
					result, err := execution.Read(ctx, &struct{}{}, nil, nil)
					require.NoError(t, err)
					found := false
					for _, row := range result.([]*feeMatcherParent) {
						if row.ID != 5 {
							require.Empty(t, row.Fees)
							continue
						}
						found = true
						require.Len(t, row.Fees, 1, "pass=%d", pass)
						require.Equal(t, 1, row.Fees[0].ID)
						require.Equal(t, 5, *row.Fees[0].ParentID)
					}
					require.True(t, found)
					if cached && pass == 0 {
						require.NoError(t, db.ExecStatements(ctx, "DROP TABLE parents", "DROP TABLE fees"))
					}
				}
				require.Equal(t, rootSQL, component.RootView.Source.SQL)
			})
		}
	}
}
