package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func TestNamedWildcardPseudoProjection(t *testing.T) {
	for _, tc := range []struct {
		name, source, selected string
		inferred, reject       bool
	}{
		{"physical", `SELECT n.* FROM (SELECT r.*, '' AS pseudo_column FROM records r) n`, "id", false, false},
		{"pseudo output", `SELECT n.* FROM (SELECT r.*, '' AS pseudo_column FROM records r) n`, "pseudo_column", false, false},
		{"authored mapping", `SELECT n.* FROM (SELECT r.*, '' AS pseudo_column FROM records r) n`, "Logical", false, false},
		{"inferred mapping", `SELECT n.* FROM (SELECT r.*, '' AS pseudo_column FROM records r) n`, "Logical", true, true},
		{"authored output replaces inferred mapping", `SELECT n.* FROM (SELECT r.*, '' AS Logical FROM records r) n`, "pseudo_column", true, true},
		{"inner alias inaccessible", `SELECT n.* FROM (SELECT r.*, '' AS pseudo_column FROM records r) n`, "r.id", false, true},
		{"wrong wildcard alias", `SELECT n.* FROM (SELECT wrong.*, '' AS pseudo_column FROM records r) n`, "id", false, true},
		{"unmapped alias", `SELECT n.* FROM (SELECT r.*, '' AS another FROM records r) n`, "pseudo_column", false, true},
		{"renamed physical", `SELECT n.* FROM (SELECT r.id AS renamed, '' AS pseudo_column FROM records r) n`, "id", false, true},
		{"renamed output", `SELECT n.* FROM (SELECT r.id AS renamed, '' AS pseudo_column FROM records r) n`, "renamed", false, false},
		{"computed physical", `SELECT n.* FROM (SELECT r.*, r.id + 1 AS pseudo_column FROM records r) n`, "id", false, true},
		{"unnamed literal", `SELECT n.* FROM (SELECT r.*, '' FROM records r) n`, "id", false, true},
		{"duplicate literal aliases", `SELECT n.* FROM (SELECT r.*, '' AS pseudo_column, 1 AS pseudo_column FROM records r) n`, "id", false, true},
		{"duplicate wildcard", `SELECT n.* FROM (SELECT r.*, r.*, '' AS pseudo_column FROM records r) n`, "id", false, true},
		{"set source", `SELECT n.* FROM (SELECT r.*, '' AS pseudo_column FROM records r UNION SELECT r.*, '' AS pseudo_column FROM records r) n`, "id", false, true},
		{"joined source", `SELECT n.* FROM (SELECT r.*, '' AS pseudo_column FROM records r JOIN other o ON r.id=o.id) n`, "id", false, true},
		{"excluded source", `SELECT n.* FROM (SELECT r.* EXCEPT(id), '' AS pseudo_column FROM records r) n`, "id", false, true},
		{"nested wildcard", `SELECT n.* FROM (SELECT s.* FROM (SELECT r.*, '' AS pseudo_column FROM records r) s) n`, "id", false, false},
		{"cte wildcard", `WITH s AS (SELECT r.*, '' AS pseudo_column FROM records r) SELECT n.* FROM s n`, "pseudo_column", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := &data.View{Spec: spec.View{Columns: []*spec.Column{{Name: "Logical", NameInferred: tc.inferred, Source: "pseudo_column", ExplicitType: true, Type: spec.TypeRef{Name: "int"}, Tag: `sqlx:"-"`}}}, Columns: []*data.Column{{Name: "ID", Column: "id"}}}
			projection := SelectorProjection{SQL: tc.source, View: view}
			_, err := projection.Columns([]string{tc.selected})
			if tc.reject {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			result, err := projection.Prepare([]string{tc.selected})
			require.NoError(t, err)
			require.Contains(t, result.Render(result.Source), "pseudo_column")
			require.Equal(t, "id", view.Columns[0].Column)
		})
	}
}
