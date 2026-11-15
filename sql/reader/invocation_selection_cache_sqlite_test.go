package reader_test

import (
	"context"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap/cacheconfig"
	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/testutil/sqlfault"
	xstate "github.com/viant/xdatly/state"
)

func TestInvocationOutputSelectionCannotReuseNarrowWarmupSQLite(t *testing.T) {
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(t.Context(), "CREATE TABLE parents(id INTEGER,name TEXT,kind TEXT)", "CREATE TABLE children(parent_id INTEGER,value INTEGER)", "INSERT INTO parents VALUES(1,'one','banner')", "INSERT INTO children VALUES(1,10)"))
	var rootQueries atomic.Int32
	db := h.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
		if call.Phase == "query" && strings.Contains(call.SQL, "FROM parents") {
			rootQueries.Add(1)
		}
		return nil
	})
	component := &spec.Component{RootView: &spec.View{Name: "parents", Selector: &spec.Selector{AllowFields: true}, Source: &spec.ViewSource{SQL: "SELECT id,name,kind FROM parents"}, Relations: []*spec.Relation{{Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany, View: &spec.View{Name: "children", Source: &spec.ViewSource{SQL: "SELECT parent_id,value FROM children"}}, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}}}}}
	typ := reflect.TypeFor[[]*selectionModeRow]()
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
	require.NoError(t, err)
	view := plan.Root.View
	view.Cache = &data.Cache{Warmup: &spec.CacheWarmupSettings{FieldNames: []string{"id"}}}
	native, err := (cacheconfig.Config{Identity: "invocation-selection", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
	require.NoError(t, err)
	execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: db}, ReadCaches: map[*data.View]cache.Cache{view: native}})
	require.NoError(t, err)
	_, err = execution.Warmup(t.Context(), dexec.ReaderWarmupInvocation{Input: &struct{}{}})
	require.NoError(t, err)
	narrow := sourceGuardBinder{xstate.Selectors{&xstate.NamedSelector{Name: "parents", Selector: xstate.Selector{Fields: []string{"id", "class", "assigned"}}}}}
	rootQueries.Store(0)
	old, err := execution.Read(t.Context(), &struct{}{}, narrow, nil)
	require.NoError(t, err)
	require.Equal(t, "unsupported", old.([]*selectionModeRow)[0].Class)
	require.Zero(t, rootQueries.Load(), "old query-shaped selection can reuse the narrow authored warmup")
	target := dexec.ComponentTarget{Component: component.Key}
	ctx, err := dexec.EnterSelectorPresentation(t.Context(), target, dexec.NewSelectorPresentation(target, func(reflect.Type, string) ([]dexec.SelectorFieldName, error) { return nil, nil }).WithFieldSelection(spec.FieldSelectionOutputOnly))
	require.NoError(t, err)
	for iteration := 0; iteration < 2; iteration++ {
		rootQueries.Store(0)
		value, err := execution.Read(ctx, &struct{}{}, narrow, nil)
		require.NoError(t, err)
		row := value.([]*selectionModeRow)[0]
		require.Equal(t, "banner", row.Class)
		require.True(t, row.Assigned)
		require.Len(t, row.Children, 1)
		if iteration == 0 {
			require.EqualValues(t, 1, rootQueries.Load(), "narrow cache cannot fulfill the complete internal root")
		} else {
			require.Zero(t, rootQueries.Load(), "complete exact-query cache is reusable")
		}
	}
}
