package transcribe

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
)

type discoveryConnections struct {
	column.Connections
	names []string
}

func (r *discoveryConnections) ResolveDB(ctx context.Context, name string) (*sql.DB, error) {
	r.names = append(r.names, name)
	return r.Connections.ResolveDB(ctx, name)
}

func TestDQLDiscoveryWithoutDefaultConnector(t *testing.T) {
	ctx := context.Background()
	textDB, numberDB := testharness.NewSQLiteHarness(t), testharness.NewSQLiteHarness(t)
	require.NoError(t, textDB.ExecStatements(ctx, "CREATE TABLE records(id INTEGER NOT NULL, value TEXT NOT NULL)"))
	require.NoError(t, numberDB.ExecStatements(ctx, "CREATE TABLE records(id INTEGER NOT NULL, value INTEGER NOT NULL)"))
	resolver := &discoveryConnections{Connections: column.Connections{"text_db": textDB.DB, "number_db": numberDB.DB}}
	source := &Source{
		Name: "MultiDB", Scope: "example.com/discovery", ColumnRefiner: column.New(resolver),
		Text: `#setting($_ = $route('/records', 'GET'))
SELECT p.*, inherited.*, signals.*, perf.*,
use_connector(p, 'text_db'), use_connector(perf, 'number_db'), in_memory(signals)
FROM (SELECT id, value FROM records) p
JOIN (SELECT id, value FROM records) inherited ON p.id = inherited.id
JOIN (SELECT 0 AS id, '' AS value) signals ON p.id = signals.id
JOIN (SELECT id, value FROM records) perf ON signals.id = perf.id`,
	}
	compiled, err := NewCompiler().Compile(ctx, source)
	require.NoError(t, err)
	require.Empty(t, source.Connector)
	if compiled.Component.Settings != nil {
		require.Empty(t, compiled.Component.Settings.DefaultConnector)
	}
	require.Equal(t, []string{"text_db", "text_db", "text_db", "number_db"}, resolver.names)
	views := map[string]*spec.View{}
	var collect func(*spec.View)
	collect = func(view *spec.View) {
		views[view.CanonicalName()] = view
		for _, relation := range view.Relations {
			collect(relation.View)
		}
	}
	collect(compiled.Component.RootView)
	for _, expected := range []struct {
		view *spec.View
		typ  string
	}{{compiled.Component.RootView, "string"}, {views["inherited"], "string"}, {views["perf"], "int"}} {
		require.NotNil(t, expected.view)
		require.Len(t, expected.view.Columns, 2)
		require.Equal(t, expected.typ, expected.view.Columns[1].Type.Name)
		require.NotEmpty(t, expected.view.Columns[1].DatabaseType)
	}
	require.NotNil(t, views["signals"])
	require.True(t, views["signals"].InMemory)
	require.Len(t, views["signals"].Columns, 2)
	require.NotEmpty(t, views["signals"].Source.SQL)
	require.Empty(t, views["signals"].RuntimeSource().SQL)

	// A selected but unavailable database must fail rather than silently using
	// parser-derived types or retrying another named database.
	require.NoError(t, numberDB.DB.Close())
	resolver.names = nil
	_, err = NewCompiler().Compile(ctx, source)
	require.ErrorContains(t, err, `component "MultiDB"`)
	require.ErrorContains(t, err, `view "perf"`)
	require.ErrorContains(t, err, `connector "number_db"`)
	require.Equal(t, []string{"text_db", "text_db", "text_db", "number_db"}, resolver.names)
}
