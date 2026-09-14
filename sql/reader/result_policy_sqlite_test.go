package reader_test

import (
	"context"
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
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
)

func TestReadMetadataPreservesNativeResultPolicySQLite(t *testing.T) {
	type row struct{ ID int }
	type output struct{ Rows []*row }
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(context.Background(), "CREATE TABLE proof(id INTEGER)", "INSERT INTO proof VALUES(7)"))
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Proof"}, RootView: &spec.View{Name: "Proof", Source: &spec.ViewSource{SQL: "SELECT id FROM proof"}}}
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeFor[output](), DirectViewField: "Rows"})
	require.NoError(t, err)
	native, err := (cacheconfig.Config{Identity: "proof", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
	require.NoError(t, err)
	execution, err := reader.NewExecution(reader.Config{Component: component, Plan: plan, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeFor[output](), SQL: &dsql.SQLComponent{DB: db.DB}, ReadCaches: map[*data.View]cache.Cache{plan.Root.View: native}})
	require.NoError(t, err)
	_, err = execution.ReadResult(context.Background(), &struct{}{}, nil, nil)
	require.NoError(t, err)
	require.NoError(t, db.ExecStatements(context.Background(), "DROP TABLE proof"))
	cached, err := execution.ReadResult((dexec.ReaderOptions{CacheOnly: true}).Context(context.Background()), &struct{}{}, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, cached.Projection)
	require.Len(t, cached.Data.(*output).Rows, 1)
	disallowed, err := sqlxread.NewQueryScope([]cache.ParmetrizedQuery{{SQL: "SELECT secret FROM elsewhere"}})
	require.NoError(t, err)
	_, err = execution.ReadResult((dexec.ReaderOptions{CacheOnly: true, QueryScope: disallowed}).Context(context.Background()), &struct{}{}, nil, nil)
	require.ErrorIs(t, err, sqlxread.ErrQueryOutsideScope)
}
