package reader_test

import (
	"context"
	"reflect"
	"slices"
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

type siblingSourceChild struct {
	ID     int
	Tenant int
}

type siblingSourceParent struct {
	ID          int                   `sqlx:"id"`
	Tenant      int                   `sqlx:"tenant"`
	Scanned     int                   `sqlx:"key_id"`
	Raw         int                   `sqlx:"-"`
	Hook        int                   `sqlx:"-" relationKey:"hook"`
	OtherHook   int                   `sqlx:"-" relationKey:"hook"`
	HookRows    []*siblingSourceChild `view:"HookRows" on:"Hook:key_id=ID:id" sql:"SELECT id,tenant FROM children WHERE $COLUMN_IN"`
	OtherRows   []*siblingSourceChild `view:"OtherRows" on:"OtherHook:key_id=ID:id" sql:"SELECT id,tenant FROM children WHERE $COLUMN_IN"`
	RawRows     []*siblingSourceChild `view:"RawRows" on:"Raw:key_id=ID:id" sql:"SELECT id,tenant FROM children WHERE $COLUMN_IN"`
	ScannedRows []*siblingSourceChild `view:"ScannedRows" on:"Scanned:key_id=ID:id" sql:"SELECT id,tenant FROM children WHERE $COLUMN_IN"`
	SharedRows  []*siblingSourceChild `view:"SharedRows" on:"Hook:key_id=ID:id" sql:"SELECT id,tenant FROM children WHERE $COLUMN_IN"`
	HookPair    []*siblingSourceChild `view:"HookPair" on:"Hook:key_id=ID:id,Tenant:tenant=Tenant:tenant" sql:"SELECT id,tenant FROM children WHERE $COLUMN_IN"`
	OtherPair   []*siblingSourceChild `view:"OtherPair" on:"OtherHook:key_id=ID:id,Tenant:tenant=Tenant:tenant" sql:"SELECT id,tenant FROM children WHERE $COLUMN_IN"`
	RawPair     []*siblingSourceChild `view:"RawPair" on:"Raw:key_id=ID:id,Tenant:tenant=Tenant:tenant" sql:"SELECT id,tenant FROM children WHERE $COLUMN_IN"`
	ScannedPair []*siblingSourceChild `view:"ScannedPair" on:"Scanned:key_id=ID:id,Tenant:tenant=Tenant:tenant" sql:"SELECT id,tenant FROM children WHERE $COLUMN_IN"`
	SharedPair  []*siblingSourceChild `view:"SharedPair" on:"Hook:key_id=ID:id,Tenant:tenant=Tenant:tenant" sql:"SELECT id,tenant FROM children WHERE $COLUMN_IN"`
	HookOne     *siblingSourceChild   `view:"HookOne" on:"Hook:key_id=ID:id" sql:"SELECT id,tenant FROM children WHERE $COLUMN_IN"`
	RawOne      *siblingSourceChild   `view:"RawOne" on:"Raw:key_id=ID:id" sql:"SELECT id,tenant FROM children WHERE $COLUMN_IN"`
}

func (r *siblingSourceParent) OnFetch(context.Context) error {
	// SQL-backed siblings must retain key_id, not this post-hook value.
	r.Scanned = r.ID + 199
	r.Hook = r.ID - 1
	r.OtherHook = r.ID + 1
	return nil
}

func TestSiblingKeySourcesRemainDistinctOnCacheReplay(t *testing.T) {
	for _, strategy := range []data.MatchStrategy{data.MatchSequential, data.MatchReadAll} {
		for _, reverse := range []bool{false, true} {
			name := "matched"
			if strategy == data.MatchReadAll {
				name = "read_all"
			}
			if reverse {
				name += "/reversed"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				db := sqlite.New(t)
				require.NoError(t, db.ExecStatements(ctx,
					"CREATE TABLE parents(id INTEGER, tenant INTEGER, key_id INTEGER, scanned_id INTEGER)",
					"INSERT INTO parents VALUES(1,7,99,200),(2,8,100,201)",
					"CREATE TABLE children(id INTEGER, tenant INTEGER)",
					"INSERT INTO children VALUES(0,7),(1,8),(2,7),(3,8),(99,7),(100,8),(200,7),(201,8)"))
				typ := reflect.TypeFor[[]*siblingSourceParent]()
				component := &spec.Component{RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT id,tenant,key_id,scanned_id FROM parents ORDER BY id"}}}
				plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
				require.NoError(t, err)
				if reverse {
					slices.Reverse(plan.Root.View.Relations)
				}
				for _, relation := range plan.Root.View.Relations {
					relation.Of.MatchStrategy = strategy
				}
				caches := map[*data.View]cache.Cache{}
				var addCache func(*data.View)
				addCache = func(view *data.View) {
					service, err := (cacheconfig.Config{Identity: "siblings/" + view.Spec.Name, Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
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
					result, err := execution.Read(ctx, &struct{}{}, nil, nil)
					require.NoError(t, err)
					rows := result.([]*siblingSourceParent)
					require.Len(t, rows, 2)
					for i, row := range rows {
						require.Zero(t, row.Raw)
						for _, check := range []struct {
							name string
							rows []*siblingSourceChild
							id   int
						}{
							{"HookRows", row.HookRows, i}, {"OtherRows", row.OtherRows, i + 2},
							{"RawRows", row.RawRows, i + 99}, {"ScannedRows", row.ScannedRows, i + 200}, {"SharedRows", row.SharedRows, i},
							{"HookPair", row.HookPair, i}, {"OtherPair", row.OtherPair, i + 2},
							{"RawPair", row.RawPair, i + 99}, {"ScannedPair", row.ScannedPair, i + 200}, {"SharedPair", row.SharedPair, i},
							{"HookOne", []*siblingSourceChild{row.HookOne}, i}, {"RawOne", []*siblingSourceChild{row.RawOne}, i + 99},
						} {
							t.Run(check.name, func(t *testing.T) {
								require.Equal(t, []*siblingSourceChild{{ID: check.id, Tenant: i + 7}}, check.rows, "pass=%d parent=%d", pass, i)
							})
						}
					}
					if pass == 0 {
						require.NoError(t, db.ExecStatements(ctx, "DROP TABLE parents", "DROP TABLE children"))
					}
				}
			})
		}
	}
}
