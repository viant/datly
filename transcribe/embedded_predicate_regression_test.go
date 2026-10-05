package transcribe

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Explicit CAST metadata must survive the analysis-only predicate substitution.
func TestEmbeddedPredicateExplicitProjectedType(t *testing.T) {
	grouped := "SELECT e.category, COUNT(*) AS total FROM events e " + analysisWhere + " GROUP BY e.category " + analysisHaving
	for _, tc := range []struct{ name, SQL string }{
		{"aggregate", grouped},
		{"CTE", "WITH filtered AS (SELECT e.category FROM events e " + analysisWhere + ") SELECT f.category, COUNT(*) AS total FROM filtered f GROUP BY f.category " + analysisHaving},
		{"join", "SELECT e.category, COUNT(*) AS total FROM events e JOIN event_types t ON e.type_id=t.id " + analysisWhere + " GROUP BY e.category " + analysisHaving},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := embeddedAnalysisSource(t, tc.SQL, "total")
			result, err := NewCompiler().Compile(context.Background(), source)
			require.NoError(t, err)
			found := false
			for _, col := range result.Component.RootView.Columns {
				if col.Name == "total" {
					found = true
					require.True(t, col.ExplicitType)
					require.Equal(t, "int", col.Type.Name)
				}
			}
			require.True(t, found, "missing explicit projected total")
			stored, err := source.Resources.ReadFile("sql/events.sql")
			require.NoError(t, err)
			require.Equal(t, tc.SQL, string(stored))
		})
	}
}

func TestEmbeddedPredicateRejectsProjectionChangingBuildPrefix(t *testing.T) {
	source := embeddedAnalysisSource(t, `SELECT e.total ${predicate.Builder().CombineAnd("1").Build(",")} FROM events e`, "total")
	_, err := NewCompiler().Compile(context.Background(), source)
	require.ErrorContains(t, err, "requires a static")
}

func TestEmbeddedPredicateTupleConditions(t *testing.T) {
	for _, SQL := range []string{
		`SELECT e.id FROM events e WHERE e.id IN (1, 2) AND ${predicate.FilterGroup(0, "AND")}`,
		`SELECT e.id FROM events e GROUP BY e.id HAVING COUNT(*) IN (1, 2) AND ${predicate.FilterGroup(0, "AND")}`,
		`SELECT e.id FROM events e JOIN labels l ON l.id IN (1, 2) AND ${predicate.FilterGroup(0, "AND")}`,
		`SELECT e.id FROM events e WHERE e.id IN (1, 2) AND e.id > $ID`,
	} {
		t.Run(SQL, func(t *testing.T) {
			_, err := NewCompiler().Compile(context.Background(), embeddedAnalysisSource(t, SQL, "id"))
			require.NoError(t, err)
		})
	}
}
