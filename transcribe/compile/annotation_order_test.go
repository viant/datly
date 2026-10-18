package compile

import (
	"context"
	"reflect"
	"strings"
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

// EXCEPT is visibility metadata: physical fields still follow the SQL result
// order unless an explicit projection already owns their position and name.
func TestProjectionExclusionKeepsWildcardDiscoveryOrder(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	require.NoError(t, h.ExecStatements(ctx, `CREATE TABLE records (
 ID INTEGER, OWNER_ID INTEGER, NAME TEXT, DESCRIPTION TEXT, COUNTRY TEXT,
 EXPIRATION_DT TEXT, ACTIVE INTEGER, CREATED TEXT, UPDATED TEXT, COUNT INTEGER)`))
	sourceSQL := `SELECT ID, OWNER_ID, NAME, DESCRIPTION, COUNTRY, EXPIRATION_DT, ACTIVE, CREATED, UPDATED, COUNT FROM records`
	sqlOrder := []string{"ID", "OWNER_ID", "NAME", "DESCRIPTION", "COUNTRY", "EXPIRATION_DT", "ACTIVE", "CREATED", "UPDATED", "COUNT"}
	for _, tc := range []struct {
		name, projection string
		want             []string
	}{
		{"wildcard", `r.* EXCEPT(ACTIVE, EXPIRATION_DT)`, sqlOrder},
		{"explicit alias", `r.* EXCEPT(ACTIVE, EXPIRATION_DT), r.ACTIVE AS Enabled`, []string{"Enabled", "ID", "OWNER_ID", "NAME", "DESCRIPTION", "COUNTRY", "EXPIRATION_DT", "CREATED", "UPDATED", "COUNT"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			SQL := `SELECT ` + tc.projection + ` FROM (` + sourceSQL + `) r`
			view, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: SQL}}, SQL: SQL})
			require.NoError(t, err)
			baselineSQL := strings.ReplaceAll(SQL, " EXCEPT(ACTIVE, EXPIRATION_DT)", "")
			baseline, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: baselineSQL}}, SQL: baselineSQL})
			require.NoError(t, err)
			require.Equal(t, strings.Fields(baseline.Source.SQL), strings.Fields(view.Source.SQL))
			compiledSQL := view.Source.SQL
			component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: view}
			require.NoError(t, column.New(column.Connections{"main": h.DB}).Refine(ctx, component, nil, nil))
			require.Equal(t, compiledSQL, view.Source.SQL)
			rows, err := h.DB.QueryContext(ctx, view.Source.SQL)
			require.NoError(t, err)
			defer rows.Close()
			actualSQLOrder, err := rows.Columns()
			require.NoError(t, err)
			require.Equal(t, sqlOrder, actualSQLOrder)
			var names []string
			for _, c := range view.Columns {
				names = append(names, c.Name)
				tag := reflect.StructTag(c.Tag)
				if c.Source == "ACTIVE" || c.Source == "EXPIRATION_DT" {
					require.Equal(t, "true", tag.Get("internal"), c.Name)
					if c.Name == "Enabled" {
						require.Equal(t, "ACTIVE", tag.Get("sqlx"))
					} else {
						require.Empty(t, tag.Get("sqlx"), c.Name)
					}
					require.Empty(t, tag.Get("json"), c.Name)
				} else {
					require.Empty(t, tag.Get("internal"), c.Name)
				}
				require.False(t, c.AnnotationOnly, c.Name)
			}
			require.Equal(t, tc.want, names)
		})
	}
}

func TestProjectionExclusionKeepsExistingColumnOwnership(t *testing.T) {
	existing := &spec.Column{Name: "Enabled", Source: "ACTIVE", Tag: `json:"enabled"`}
	view := &spec.View{Columns: []*spec.Column{existing}}
	markInternalColumn(view, "active")
	require.Same(t, existing, view.Columns[0])
	require.False(t, existing.AnnotationOnly)
	require.Equal(t, "Enabled", existing.Name)
	require.Equal(t, "ACTIVE", existing.Source)
	require.Equal(t, "enabled", reflect.StructTag(existing.Tag).Get("json"))
	require.Equal(t, "true", reflect.StructTag(existing.Tag).Get("internal"))
}
