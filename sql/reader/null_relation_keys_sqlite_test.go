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
	xexec "github.com/viant/xdatly/exec"
	xstate "github.com/viant/xdatly/state"
)

type nullKeyChild struct {
	ID     int    `sqlx:"id"`
	Tenant string `sqlx:"tenant"`
}

type memoryNullParent struct {
	ID       int                `sqlx:"id"`
	Key      *int               `sqlx:"lookup_id"`
	Rows     []*memoryNullChild `view:"memory" on:"Key:lookup_id=Owner:owner_id"`
	Finished bool               `sqlx:"-"`
}
type memoryNullChild struct {
	Owner    *int          `sqlx:"owner_id"`
	Key      *int          `sqlx:"key_id"`
	Lookup   *nullKeyChild `view:"nested" on:"Key:key_id=ID:id" sql:"SELECT id,tenant FROM children WHERE $COLUMN_IN"`
	Finished bool          `sqlx:"-"`
}

func (p *memoryNullParent) OnFetch(context.Context) error {
	zero, seven := 0, 7
	p.Rows = []*memoryNullChild{{Key: &zero}, {Key: &seven}, {}}
	return nil
}
func (p *memoryNullParent) OnRelation(context.Context) { p.Finished = true }
func (p *memoryNullChild) OnRelation(context.Context)  { p.Finished = true }

func TestNullParentKeyPreservesInMemoryNestedEnrichment(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER,lookup_id INTEGER)", "INSERT INTO parents VALUES(1,NULL)", "CREATE TABLE children(id INTEGER,tenant TEXT)", "INSERT INTO children VALUES(0,''),(7,'acme')"))
	typ := reflect.TypeFor[[]*memoryNullParent]()
	component := &spec.Component{RootView: &spec.View{Name: "parents", Selector: &spec.Selector{AllowFields: true}, Source: &spec.ViewSource{SQL: "SELECT id,lookup_id FROM parents"}}}
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
	require.NoError(t, err)
	execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	result, err := execution.Read(ctx, &struct{}{}, nil, nil)
	require.NoError(t, err)
	rows := result.([]*memoryNullParent)
	require.Len(t, rows, 1)
	require.Nil(t, rows[0].Key)
	require.True(t, rows[0].Finished)
	require.Len(t, rows[0].Rows, 3)
	for i, child := range rows[0].Rows {
		require.True(t, child.Finished)
		if i == 2 {
			require.Nil(t, child.Lookup)
		} else {
			require.NotNil(t, child.Lookup)
			require.Equal(t, *child.Key, child.Lookup.ID)
		}
	}
	require.NoError(t, db.ExecStatements(ctx, "DROP TABLE children"))
	selectors := xstate.Selectors{&xstate.NamedSelector{Name: "parents", Selector: xstate.Selector{Fields: []string{"id"}}}}
	_, err = execution.Read(ctx, &struct{}{}, sourceGuardBinder{selectors}, nil)
	require.NoError(t, err, "excluded in-memory relation must not execute its nested lookup")
}

type nullKeyParent struct {
	ID        int             `sqlx:"id"`
	Key       *int            `sqlx:"lookup_id"`
	Tenant    *string         `sqlx:"tenant"`
	Hidden    *int            `sqlx:"-"`
	Hook      *int            `sqlx:"-" relationKey:"hook"`
	Typed     []*nullKeyChild `view:"typed" on:"Key:lookup_id=ID:id" sql:"SELECT id,tenant FROM children WHERE $COLUMN_IN"`
	Captured  []*nullKeyChild `view:"captured" on:"Hidden:lookup_id=ID:id" sql:"SELECT id,tenant FROM children WHERE $COLUMN_IN"`
	Hooked    []*nullKeyChild `view:"hooked" on:"Hook:hook_id=ID:id" sql:"SELECT id,tenant FROM children WHERE $COLUMN_IN"`
	Composite []*nullKeyChild `view:"composite" on:"Key:lookup_id=ID:id,Tenant:tenant=Tenant:tenant" sql:"SELECT id,tenant FROM children WHERE $COLUMN_IN"`
}

func (p *nullKeyParent) OnFetch(context.Context) error {
	// Explicit hook authority must retain nil, even though SQL supplies 999.
	p.Hook = p.Key
	return nil
}

func TestNullRelationKeysAndCacheReplay(t *testing.T) {
	for _, allNull := range []bool{false, true} {
		name := "mixed_cache_replay"
		if allNull {
			name = "all_null_no_child_table"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER,lookup_id INTEGER,tenant TEXT)"))
			if allNull {
				require.NoError(t, db.ExecStatements(ctx, "INSERT INTO parents VALUES(1,NULL,''),(2,NULL,NULL)"))
			} else {
				require.NoError(t, db.ExecStatements(ctx,
					"INSERT INTO parents VALUES(1,0,''),(2,NULL,''),(3,7,'acme'),(4,99,NULL)",
					"CREATE TABLE children(id INTEGER,tenant TEXT)", "INSERT INTO children VALUES(0,''),(7,'acme'),(99,'other')"))
			}
			typ := reflect.TypeFor[[]*nullKeyParent]()
			component := &spec.Component{RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT id,lookup_id,tenant,999 AS hook_id FROM parents ORDER BY id"}}}
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
			require.NoError(t, err)
			caches := map[*data.View]cache.Cache{}
			var addCache func(*data.View)
			addCache = func(view *data.View) {
				service, err := (cacheconfig.Config{Identity: "null-keys/" + view.Spec.Name, Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
				require.NoError(t, err)
				caches[view] = service
				for _, relation := range view.Relations {
					addCache(relation.Of.View)
				}
			}
			addCache(plan.Root.View)
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}, ReadCaches: caches})
			require.NoError(t, err)
			for pass := 0; pass < 2; pass++ {
				ec := xexec.New()
				result, err := execution.Read(xexec.WithContext(ctx, ec), &struct{}{}, nil, nil)
				require.NoError(t, err)
				rows := result.([]*nullKeyParent)
				wantCount := 4
				if allNull {
					wantCount = 2
				}
				require.Len(t, rows, wantCount)
				for _, row := range rows {
					require.Nil(t, row.Hidden)
					for _, attached := range [][]*nullKeyChild{row.Typed, row.Captured, row.Hooked} {
						if row.Key == nil {
							require.Empty(t, attached)
							continue
						}
						require.Len(t, attached, 1)
						require.Equal(t, *row.Key, attached[0].ID)
					}
					if row.Key == nil || row.Tenant == nil {
						require.Empty(t, row.Composite)
					} else {
						require.Len(t, row.Composite, 1)
						require.Equal(t, *row.Key, row.Composite[0].ID)
						require.Equal(t, *row.Tenant, row.Composite[0].Tenant)
					}
				}
				childArguments := 0
				for _, metric := range ec.Metrics {
					if metric.View == "parents" {
						continue
					}
					for _, query := range metric.Executions {
						require.False(t, allNull, "all-null relation must not execute child SQL")
						for _, arg := range query.Args {
							childArguments++
							require.NotNil(t, arg, "view=%s args=%v", metric.View, query.Args)
							v := reflect.ValueOf(arg)
							if v.Kind() == reflect.Ptr {
								require.False(t, v.IsNil())
							}
						}
					}
				}
				if !allNull && pass == 0 {
					require.Positive(t, childArguments, "regression must inspect actual child query arguments")
				}
				if pass == 0 {
					require.NoError(t, db.ExecStatements(ctx, "DROP TABLE parents"))
					if !allNull {
						require.NoError(t, db.ExecStatements(ctx, "DROP TABLE children"))
					}
				}
			}
		})
	}
}
