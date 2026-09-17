package reader_test

import (
	"context"
	"fmt"
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

type mappedSQLParent struct {
	ID       int               `sqlx:"id"`
	RawID    int               `sqlx:"-"`
	Children []*mappedSQLChild `view:"children" on:"RawID:id=RawID:id" sql:"SELECT id FROM children WHERE $COLUMN_IN ORDER BY id"`
}

func (p *mappedSQLParent) OnFetch(context.Context) error {
	p.ID += 100
	return nil
}

type mappedSQLChild struct {
	ID       int `sqlx:"id"`
	RawID    int `sqlx:"-"`
	ParentID int `sqlx:"-"`
}

func (c *mappedSQLChild) OnFetch(ctx context.Context) error {
	parent, ok := ctx.Value(reflect.TypeFor[*mappedSQLParent]()).(*mappedSQLParent)
	if !ok {
		return fmt.Errorf("published parent is missing")
	}
	c.ParentID = parent.ID
	c.ID += 200
	return nil
}

func TestMappedSQLKeysSurviveParentAndChildHooksOnCacheReplay(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE parents(id INTEGER)", "INSERT INTO parents VALUES(0),(99)",
		"CREATE TABLE children(id INTEGER)", "INSERT INTO children VALUES(0),(99)"))
	typ := reflect.TypeFor[[]*mappedSQLParent]()
	component := &spec.Component{RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT id FROM parents ORDER BY id"}}}
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
	require.NoError(t, err)
	plan.Root.View.Relations[0].Of.View.Spec.PublishParent = true
	caches := map[*data.View]cache.Cache{}
	for _, view := range []*data.View{plan.Root.View, plan.Root.View.Relations[0].Of.View} {
		service, err := (cacheconfig.Config{Identity: view.Spec.Name, Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
		require.NoError(t, err)
		caches[view] = service
	}
	execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}, ReadCaches: caches})
	require.NoError(t, err)
	for pass := 0; pass < 2; pass++ {
		result, err := execution.Read(ctx, &struct{}{}, nil, nil)
		require.NoError(t, err)
		rows := result.([]*mappedSQLParent)
		require.Len(t, rows, 2)
		for i, id := range []int{0, 99} {
			require.Equal(t, id+100, rows[i].ID)
			require.Zero(t, rows[i].RawID)
			require.Equal(t, []*mappedSQLChild{{ID: id + 200, ParentID: id + 100}}, rows[i].Children, "pass=%d", pass)
		}
		if pass == 0 {
			require.NoError(t, db.ExecStatements(ctx, "DROP TABLE parents", "DROP TABLE children"))
		}
	}
}
