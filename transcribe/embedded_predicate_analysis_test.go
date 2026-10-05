package transcribe

import (
	"context"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe/column"
)

const analysisWhere = `${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")}`
const analysisHaving = `${predicate.Builder().CombineOr($predicate.FilterGroup(1, "AND")).Build("HAVING")}`

func embeddedAnalysisSource(t *testing.T, SQL, target string) *Source {
	t.Helper()
	resources := resource.New()
	require.NoError(t, resources.Register("", fstest.MapFS{"sql/events.sql": &fstest.MapFile{Data: []byte(SQL)}}))
	projection := "events.*"
	if target != "" {
		projection += ", cast(events." + target + " AS int)"
	}
	return &Source{Scope: "example.com/analysis", Name: "Events", Resources: resources,
		Text: "#setting($_ = $route('/events', 'GET'))\nSELECT " + projection + " FROM (${embed:sql/events.sql}) events"}
}

func TestCompileEmbeddedPredicateAnalysis(t *testing.T) {
	grouped := "SELECT e.category, COUNT(*) AS total FROM events e " + analysisWhere + " GROUP BY e.category " + analysisHaving
	for _, tc := range []struct{ name, SQL string }{
		{"aggregate", grouped},
		{"CTE", "WITH grouped AS (" + grouped + ") SELECT g.category, g.total FROM grouped g"},
		{"derived", "SELECT g.category, g.total FROM (" + grouped + ") g"},
		{"nested parentheses", "SELECT g.category, g.total FROM (((" + grouped + "))) g"},
		{"commented derived", "SELECT g.category, g.total FROM (/* analysis must ignore SELECT in comments */ " + grouped + ") g"},
		{"union", "SELECT g.category, g.total FROM (" + grouped + " UNION ALL " + grouped + ") g"},
		{"join", "SELECT e.category, COUNT(*) AS total FROM events e JOIN labels l ON l.id=e.id " + analysisWhere + " GROUP BY e.category " + analysisHaving},
		{"joined subquery", "SELECT e.category, COUNT(*) AS total FROM events e JOIN (SELECT l.id FROM labels l " + analysisWhere + ") l ON l.id=e.id GROUP BY e.category " + analysisHaving},
		{"conditions", `SELECT e.category, COUNT(*) AS total FROM events e WHERE ${predicate.FilterGroup(0, "AND")} GROUP BY e.category HAVING ${predicate.FilterGroup(1, "AND")}`},
		{"unbraced predicates", `SELECT e.category, COUNT(*) AS total FROM events e $predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE") GROUP BY e.category`},
		{"empty keyword", `SELECT e.category, COUNT(*) AS total FROM events e WHERE ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("")} GROUP BY e.category`},
		{"suffix", `SELECT e.category, COUNT(*) AS total FROM events e WHERE e.id > $ID ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("AND")} GROUP BY e.category`},
		{"join condition", `SELECT e.category, COUNT(*) AS total FROM events e JOIN labels l ON l.id=e.id AND ${predicate.FilterGroup(0, "AND")} GROUP BY e.category`},
		{"builder methods", `SELECT e.category, COUNT(*) AS total FROM events e ${predicate.Builder().Combine($predicate.Expand(0)).Or().CombineAnd($predicate.ExpandWith(1, "AND")).And().CombineOr($predicate.FilterGroup(2, "OR")).Build("WHERE")} GROUP BY e.category`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			source := embeddedAnalysisSource(t, tc.SQL, "total")
			result, err := NewCompiler().Compile(ctx, source)
			require.NoError(t, err)
			control, err := NewCompiler().Compile(ctx, embeddedAnalysisSource(t, tc.SQL, ""))
			require.NoError(t, err)
			// Removing the CAST directive can normalize outer wrapper spacing.
			require.Equal(t, strings.Fields(control.Component.RootView.Source.SQL), strings.Fields(result.Component.RootView.Source.SQL))
			control.Component.RootView.Source.SQL = result.Component.RootView.Source.SQL
			require.Equal(t, control.Component.RootView.Source, result.Component.RootView.Source)
			before := result.Component.Clone()
			require.NoError(t, column.New(nil).ValidateSourceProjections(result.Component, source.Resources))
			require.Equal(t, before, result.Component, "analysis must not mutate executable metadata")
			stored, err := source.Resources.ReadFile("sql/events.sql")
			require.NoError(t, err)
			require.Equal(t, tc.SQL, string(stored))
			resolved := result.Component.RootView.Source.Clone()
			require.Len(t, resolved.Embeds, 1)
			require.NoError(t, dsql.ResolveSource("events", resolved, source.Resources))
			require.Contains(t, resolved.SQL, tc.SQL)
			require.NotContains(t, resolved.SQL, "__datly_analysis_predicate_")
		})
	}
}

func TestCompileEmbeddedPredicateAnalysisRejectsUnprovenStructure(t *testing.T) {
	for _, tc := range []struct{ name, SQL, target, message string }{
		{"missing column", "SELECT e.category, COUNT(*) AS total FROM events e " + analysisWhere + " GROUP BY e.category", "missing", "absent from the SQL projection"},
		{"predicate is not output evidence", `SELECT e.category FROM events e ${predicate.Builder().CombineAnd("missing=1").Build("WHERE")}`, "missing", "absent from the SQL projection"},
		{"ambiguous output", "SELECT e.id AS total, e.other AS total FROM events e " + analysisWhere, "total", "duplicate output column"},
		{"projection template", `SELECT ${predicate.FilterGroup(0, "AND")} AS total FROM events e`, "total", "unsupported dynamic SQL structure"},
		{"nested projection template", `SELECT e.total FROM (SELECT ${predicate.FilterGroup(0, "AND")} AS total FROM events) e`, "total", "unsupported dynamic SQL structure"},
		{"projection in condition subquery", `SELECT e.total FROM events e WHERE e.id IN (SELECT ${predicate.FilterGroup(0, "AND")} FROM labels)`, "total", "unsupported dynamic SQL structure"},
		{"CTE projection template", `WITH invalid AS (SELECT ${predicate.FilterGroup(0, "AND")} AS total FROM events) SELECT e.total FROM invalid e`, "total", "unsupported dynamic SQL structure"},
		{"source template", `SELECT e.total FROM ${predicate.FilterGroup(0, "AND")} e`, "total", "unsupported dynamic SQL structure"},
		{"dynamic source", `SELECT e.total FROM ${Unsafe.Table} e`, "total", "unsupported dynamic SQL structure"},
		{"unbraced dynamic source", `SELECT e.total FROM $Unsafe.Table e`, "total", "unsupported dynamic SQL structure"},
		{"dynamic source with predicate", `SELECT e.total FROM $Unsafe.Table e ` + analysisWhere, "total", "unsupported dynamic SQL structure"},
		{"dynamic projection", `SELECT $Columns FROM events e`, "total", "unsupported dynamic SQL structure"},
		{"unsafe aliased projection", `SELECT $Unsafe.Columns AS total FROM events e`, "total", "unsupported dynamic SQL structure"},
		{"projection clause template", `SELECT ${predicate.Builder().Build("WHERE")} AS total FROM events e`, "total", ""},
		{"source clause template", `SELECT e.total FROM ${predicate.Builder().Build("WHERE")} e`, "total", ""},
		{"dynamic keyword", `SELECT e.total FROM events e ${predicate.Builder().Build($Keyword)}`, "total", "requires a static"},
		{"unknown keyword", `SELECT e.total FROM events e ${predicate.Builder().Build("ORDER BY")}`, "total", "requires a static"},
		{"malformed SQL", "SELECT e.total FROM events e " + analysisWhere + " GROUP BY )", "total", "unexpected ')'"},
		{"malformed template", `SELECT e.total FROM events e ${predicate.Builder(`, "total", "unclosed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := embeddedAnalysisSource(t, tc.SQL, tc.target)
			_, err := NewCompiler().Compile(context.Background(), source)
			require.Error(t, err)
			if tc.message != "" {
				require.Contains(t, err.Error(), tc.message)
			}
			stored, readErr := source.Resources.ReadFile("sql/events.sql")
			require.NoError(t, readErr)
			require.Equal(t, tc.SQL, string(stored))
			require.True(t, strings.Contains(source.Text, "${embed:sql/events.sql}"))
		})
	}
}
