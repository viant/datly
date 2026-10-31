package reader_test

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
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
)

func TestQueryGuardCoversNativeCachePopulation(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(1)"))
	type row struct {
		ID int `sqlx:"id"`
	}
	type output struct{ Rows []row }
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Records"}, RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}}
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[output](), DirectViewField: "Rows"})
	require.NoError(t, err)
	view := plan.Root.View
	view.Cache = &data.Cache{Warmup: &spec.CacheWarmupSettings{}}
	nativeCache, err := (cacheconfig.Config{Identity: "query-guard/warmup", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
	require.NoError(t, err)
	execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[output](), Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}, ReadCaches: map[*data.View]cache.Cache{view: nativeCache}})
	require.NoError(t, err)
	denied := errors.New("warmup budget exceeded")
	calls := 0
	guarded := execution.WithQueryGuard(func(context.Context, *sql.DB, string, []any) error { calls++; return denied })
	_, err = guarded.(dexec.ReaderWarmer).Warmup(ctx, dexec.ReaderWarmupInvocation{Input: &struct{}{}})
	require.ErrorIs(t, err, denied)
	require.Equal(t, 1, calls)
	count, err := execution.Warmup(ctx, dexec.ReaderWarmupInvocation{Input: &struct{}{}})
	require.NoError(t, err)
	require.Equal(t, 1, count)
}
