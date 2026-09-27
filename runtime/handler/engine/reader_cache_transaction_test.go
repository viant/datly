package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap/cacheconfig"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqldml "github.com/viant/datly/sql/dml"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/sqlx/io/read/cache"
	xhandler "github.com/viant/xdatly/handler"
)

type managedCacheChild struct {
	ID   int    `sqlx:"id"`
	Name string `sqlx:"name"`
}
type managedCacheParent struct {
	ID    int                `sqlx:"id"`
	Child *managedCacheChild `view:"child" on:"ID:id=ID:id" sql:"SELECT id,name FROM child WHERE $COLUMN_IN"`
}

func TestManagedTransactionChildReaderBypassesConfiguredCache(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(fmt.Sprintf("rollback=%v", rollback), func(t *testing.T) {
			ctx := context.Background()
			h := testharness.NewSQLiteHarness(t)
			require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE parent(id INTEGER)", "INSERT INTO parent VALUES(1)", "CREATE TABLE child(id INTEGER,name TEXT)", "INSERT INTO child VALUES(1,'committed')"))
			component := &spec.Component{RootView: &spec.View{Name: "parent", Source: &spec.ViewSource{SQL: "SELECT id FROM parent"}}}
			output := reflect.TypeFor[[]*managedCacheParent]()
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: output, DirectViewType: output})
			require.NoError(t, err)
			childView := plan.Root.View.Relations[0].Of.View
			native, err := (cacheconfig.Config{Identity: "managed/child", Settings: &spec.CacheSettings{Enabled: true, Provider: "afs", Location: t.TempDir(), TTL: "1h"}}).New()
			require.NoError(t, err)
			connector := &dsql.SQLComponent{DB: h.DB}
			execution, err := reader.NewExecution(reader.Config{Component: component, Plan: plan, SQL: connector, InputType: reflect.TypeFor[struct{}](), OutputType: output, ReadCaches: map[*data.View]cache.Cache{childView: native}})
			require.NoError(t, err)
			read := func(ctx context.Context) (string, error) {
				value, err := execution.Read(ctx, &struct{}{}, nil, nil)
				if err != nil {
					return "", err
				}
				rows := value.([]*managedCacheParent)
				if len(rows) != 1 || rows[0].Child == nil {
					return "", fmt.Errorf("missing child")
				}
				return rows[0].Child.Name, nil
			}
			name, err := read(ctx)
			require.NoError(t, err)
			require.Equal(t, "committed", name)
			capabilities := rhandler.InvocationCapabilities{Connector: connector}
			source := sqldml.Source{DB: h.DB}
			abort := errors.New("abort after child read")
			_, err = New().Execute(ctx, Request{
				Input: testRouteInput(t, reflect.TypeFor[struct{}]()), DataSource: source, Capabilities: capabilities,
				Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
					value, found, err := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
					if err != nil || !found {
						return nil, fmt.Errorf("DML unavailable: %v", err)
					}
					if err := value.(xhandler.DML).Execute("UPDATE child SET name=?", "uncommitted"); err != nil {
						return nil, err
					}
					value, found, err = invocation.Binder.Lookup(ctx, xhandler.FlusherKey)
					if err != nil || !found {
						return nil, fmt.Errorf("flusher unavailable: %v", err)
					}
					if err := value.(xhandler.Flusher).Flush(ctx, ""); err != nil {
						return nil, err
					}
					_, err = New().Execute(PrepareComponent(ctx, ComponentImperative, ""), Request{
						Input: testRouteInput(t, reflect.TypeFor[struct{}]()), DataSource: source, Capabilities: capabilities,
						Handler: rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
							name, err := read(ctx)
							if err != nil {
								return nil, err
							}
							if name != "uncommitted" {
								return nil, fmt.Errorf("transactional child read stale cache: %q", name)
							}
							return nil, nil
						}),
					})
					if err != nil {
						return nil, err
					}
					if rollback {
						return nil, abort
					}
					return nil, nil
				}),
			})
			if rollback {
				require.ErrorIs(t, err, abort)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, h.DB.QueryRowContext(ctx, "SELECT name FROM child").Scan(&name))
			if rollback {
				require.Equal(t, "committed", name)
				name, err = read(ctx)
				require.NoError(t, err)
				require.Equal(t, "committed", name)
			} else {
				require.Equal(t, "uncommitted", name)
			}
		})
	}
}
