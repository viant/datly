package column

import (
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

func TestRefinerCompositePredicateUsesMetadataWithoutRuntimeHelper(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	require.NoError(t, h.ExecStatements(t.Context(), `CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)`))
	const source = `SELECT r.id,r.name FROM (SELECT * FROM records WHERE id>0) r WHERE $criteria.CompositeIn("r", $Unsafe.ProjectKeys($Body, $Current))`
	view := &spec.View{Name: "Current", Source: &spec.ViewSource{SQL: source, Table: "records"}}
	component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: view}
	// There is deliberately no input/method authority. Metadata discovery must
	// neither require nor execute the runtime key projection.
	require.NoError(t, New(Connections{"main": h.DB}).Refine(t.Context(), component, nil, nil))
	require.Equal(t, source, view.Source.SQL)
	require.Len(t, view.Columns, 2)
	require.Equal(t, "id", view.Columns[0].Name)
	require.True(t, view.Columns[0].PrimaryKey)
	require.Equal(t, "name", view.Columns[1].Name)
	require.NoError(t, h.ExecStatements(t.Context(), `ALTER TABLE records ADD COLUMN score INTEGER`))
	view.Source.SQL = `SELECT r.* FROM records r WHERE $criteria.CompositeIn("r", $Unsafe.ProjectKeys($Body, $Current))`
	require.NoError(t, New(Connections{"main": h.DB}).Refine(t.Context(), component, nil, nil))
	require.Len(t, view.Columns, 3)
}

func TestRefinerCompositePredicateDoesNotHideMissingResource(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	view := &spec.View{Name: "Current", Source: &spec.ViewSource{URI: "missing.sql"}}
	component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: view}
	err := New(Connections{"main": h.DB}).Refine(t.Context(), component, fstest.MapFS{}, nil)
	require.Error(t, err)
}
