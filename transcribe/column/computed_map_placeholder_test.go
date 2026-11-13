package column

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"testing"
)

// Public computed maps can retain a literal SQL placeholder in existing DQL.
// Discovery must preserve their authored type without duplicating the field.
func TestComputedMapPlaceholderDiscovery(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		name := "database"
		if prepared {
			name = "prepared"
		}
		t.Run(name, func(t *testing.T) {
			SQL := "SELECT id, '' AS fee_map FROM records"
			view := &spec.View{Name: "Records", Namespace: "r", Source: &spec.ViewSource{SQL: SQL}, Columns: []*spec.Column{
				{Name: "fee_map", Source: "fee_map", Type: spec.TypeRef{Name: "map[int][]string"}, ExplicitType: true, Tag: `sqlx:"-" json:"fees"`},
			}}
			component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: view}
			var err error
			if prepared {
				err = RefinePrepared(t.Context(), component, nil, func(context.Context, PreparedColumnRequest) (*PreparedColumnResult, error) {
					return &PreparedColumnResult{Columns: []*spec.Column{
						{Name: "id", Source: "id", Type: spec.TypeRef{Name: "int"}},
						{Name: "fee_map", Source: "fee_map", Type: spec.TypeRef{Name: "string"}},
					}}, nil
				}, nil)
			} else {
				h := testharness.NewSQLiteHarness(t)
				require.NoError(t, h.ExecStatements(t.Context(), "CREATE TABLE records (id INTEGER)"))
				err = New(Connections{"main": h.DB}).Refine(t.Context(), component, nil, nil)
			}
			require.NoError(t, err)
			require.Equal(t, SQL, view.Source.SQL)
			require.Len(t, view.Columns, 2, "one map field plus id; the placeholder must not create a second map field")
			require.Equal(t, "map[int][]string", view.Columns[0].Type.Name)
			require.Equal(t, `sqlx:"-" json:"fees"`, view.Columns[0].Tag)
		})
	}
}

func TestPreparedComputedMapWithoutSQLPlaceholder(t *testing.T) {
	const SQL = "SELECT id FROM records"
	view := &spec.View{Name: "Records", Namespace: "r", Source: &spec.ViewSource{SQL: SQL}, Columns: []*spec.Column{
		{Name: "fee_map", Source: "fee_map", Type: spec.TypeRef{Name: "map[int][]string"}, ExplicitType: true, Tag: `sqlx:"-" json:"fees"`},
	}}
	component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: view}
	err := RefinePrepared(t.Context(), component, nil, func(context.Context, PreparedColumnRequest) (*PreparedColumnResult, error) {
		return &PreparedColumnResult{Columns: []*spec.Column{{Name: "id", Source: "id", Type: spec.TypeRef{Name: "int"}}}}, nil
	}, nil)
	require.NoError(t, err)
	require.Equal(t, SQL, view.Source.SQL)
	require.Len(t, view.Columns, 2)
	require.Equal(t, "map[int][]string", view.Columns[0].Type.Name)
	require.Equal(t, `sqlx:"-" json:"fees"`, view.Columns[0].Tag)
}
