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

type sourceKeyChild struct {
	ID     int
	Code   string
	Tenant int
}
type sourceKeyParent struct {
	ID           int               `sqlx:"id"`
	HiddenTenant int               `sqlx:"-"`
	HookID       int               `sqlx:"-" relationKey:"hook"`
	HookCode     string            `sqlx:"-" relationKey:"hook"`
	ByID         []*sourceKeyChild `view:"ByID" on:"HookID:hook_id=ID:id" sql:"SELECT id,code,tenant FROM children WHERE $COLUMN_IN"`
	ByCode       []*sourceKeyChild `view:"ByCode" on:"HookCode:hook_code=Code:code" sql:"SELECT id,code,tenant FROM children WHERE $COLUMN_IN"`
	Mixed        []*sourceKeyChild `view:"Mixed" on:"HiddenTenant:tenant=Tenant:tenant,HookID:hook_id=ID:id" sql:"SELECT id,code,tenant FROM children WHERE $COLUMN_IN"`
}

func (r *sourceKeyParent) OnFetch(context.Context) error {
	if r.ID == 1 {
		r.HookID, r.HookCode = 0, ""
	} else {
		r.HookID, r.HookCode = 2, "b"
	}
	return nil
}

func TestExplicitHookKeysPreserveZeroAndEmptyOnCacheReplay(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE parents(id INTEGER, tenant INTEGER)",
		"INSERT INTO parents VALUES(1,7),(2,8)",
		"CREATE TABLE children(id INTEGER, code TEXT, tenant INTEGER)",
		"INSERT INTO children VALUES(0,'',7),(2,'b',8),(99,'placeholder',99)"))
	typ := reflect.TypeFor[[]*sourceKeyParent]()
	component := &spec.Component{RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT id,tenant,99 AS hook_id,'placeholder' AS hook_code FROM parents ORDER BY id"}}}
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
	require.NoError(t, err)
	caches := map[*data.View]cache.Cache{}
	var addCache func(*data.View)
	addCache = func(view *data.View) {
		service, err := (cacheconfig.Config{Identity: "sources/" + view.Spec.Name, Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
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
		rows := result.([]*sourceKeyParent)
		require.Len(t, rows, 2)
		for i, row := range rows {
			require.Zero(t, row.HiddenTenant, "hidden SQL keys must not be assigned to ignored fields")
			want := sourceKeyChild{ID: 0, Code: "", Tenant: 7}
			if i == 1 {
				want = sourceKeyChild{ID: 2, Code: "b", Tenant: 8}
			}
			for _, children := range [][]*sourceKeyChild{row.ByID, row.ByCode, row.Mixed} {
				require.Len(t, children, 1, "pass=%d parent=%d", pass, i)
				require.Equal(t, want, *children[0])
			}
		}
		if pass == 0 {
			require.NoError(t, db.ExecStatements(ctx, "DROP TABLE parents", "DROP TABLE children"))
		}
	}
}
