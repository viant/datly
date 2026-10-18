package compile

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/sqlparser"
)

func TestAnnotationPlaceholderKeepsWildcardDiscoveryOrder(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	require.NoError(t, h.ExecStatements(ctx, `CREATE TABLE records (id INTEGER NOT NULL, name TEXT NOT NULL)`))
	SQL := `SELECT r.*, optional(r.display_name), CAST(r.display_name AS string) FROM (SELECT id, name, name || '!' AS display_name FROM records) r`
	view, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: SQL}}, SQL: SQL})
	require.NoError(t, err)
	component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: view}
	require.NoError(t, column.New(column.Connections{"main": h.DB}).Refine(ctx, component, nil, nil))
	var names []string
	for _, c := range view.Columns {
		names = append(names, c.Name)
	}
	require.Equal(t, []string{"id", "name", "display_name"}, names)
	require.True(t, view.Columns[2].Optional)
	require.True(t, view.Columns[2].EffectiveType().Pointer)
}

func TestAnnotationKeepsExistingColumnOwnership(t *testing.T) {
	existing := &spec.Column{Name: "name", Source: "name"}
	view := &spec.View{Name: "Records", Namespace: "r", Columns: []*spec.Column{existing}}
	parsed, err := sqlparser.ParseQuery("SELECT r.*, optional(r.name) FROM records r")
	require.NoError(t, err)
	_, err = lowerColumnDeclarations(parsed, view, nil, nil)
	require.NoError(t, err)
	require.False(t, existing.AnnotationOnly)
	require.True(t, existing.Optional)
}

func TestAnnotationAliasTakesCanonicalOwnership(t *testing.T) {
	placeholder := &spec.Column{Name: "name", Source: "name", AnnotationOnly: true, Optional: true}
	view := &spec.View{Name: "Records", Namespace: "r", Columns: []*spec.Column{placeholder}}
	parsed, err := sqlparser.ParseQuery("SELECT r.*, r.name AS DisplayName FROM (SELECT name FROM records) r")
	require.NoError(t, err)
	_, err = readViewProjection(parsed, view, view)
	require.NoError(t, err)
	require.Len(t, view.Columns, 1)
	require.Equal(t, "DisplayName", view.Columns[0].Name)
	require.False(t, view.Columns[0].AnnotationOnly)
	require.True(t, view.Columns[0].Optional)
}
