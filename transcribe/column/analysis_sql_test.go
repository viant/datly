package column

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

const staticWhere = `${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")}`

func TestProjectionAnalysisPreservesProtectedText(t *testing.T) {
	for _, SQL := range []string{
		`SELECT '${predicate.FilterGroup(0, "AND")}' AS literal FROM events`,
		"SELECT e.id FROM events e -- " + staticWhere + "\n",
		"SELECT e.id FROM events e /* " + staticWhere + " */",
		`SELECT $quoted$${predicate.FilterGroup(0, "AND")}$quoted$ AS literal FROM events`,
		"SELECT e.id FROM events e WHERE e.id=$ID",
		"SELECT $ID AS id FROM events e",
		"SELECT $1 AS id FROM events e",
		"SELECT '$Unsafe.Columns' AS literal FROM events e",
	} {
		actual, err := projectionAnalysisSQL(SQL)
		require.NoError(t, err)
		require.Equal(t, SQL, actual)
	}
}

func TestProjectionAnalysisKeepsSourceOwnership(t *testing.T) {
	SQL := "SELECT e.id AS EventId, l.id AS LabelId, COUNT(*) AS total FROM events e JOIN labels l ON l.id=e.id " + staticWhere + " GROUP BY e.id,l.id"
	source := &spec.ViewSource{SQL: SQL, Table: "events"}
	lineage, err := directProjectionLineage(source)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"eventid": "id"}, lineage.direct)
	require.True(t, lineage.blocked["labelid"])
	require.True(t, lineage.blocked["total"])
	require.Equal(t, SQL, source.SQL)
	_, err = resolveResultSources([]*spec.Column{{Name: "A", Source: "EventId"}, {Name: "B", Source: "EventId"}}, source)
	require.ErrorContains(t, err, "ambiguous SQL result ownership")
}

func TestProjectionAnalysisSourceDiscovery(t *testing.T) {
	inner := "SELECT e.id FROM events e " + staticWhere
	for _, SQL := range []string{inner, "SELECT e.id FROM (" + inner + ") e", "WITH e AS (" + inner + ") SELECT e.id FROM e"} {
		require.Equal(t, directSourceTable(strings.ReplaceAll(SQL, staticWhere, "")), directSourceTable(SQL), SQL)
		lineage, err := directProjectionLineage(&spec.ViewSource{SQL: SQL, Table: "events"})
		require.NoError(t, err)
		require.Equal(t, map[string]string{"id": "id"}, lineage.direct)
	}
	SQL := "SELECT e.id FROM (events) e " + staticWhere
	require.Equal(t, "events", explicitAuxiliarySource(SQL))
}

func TestProjectionAnalysisBoundsAndMarkerIsolation(t *testing.T) {
	SQL := "SELECT e.id FROM events e " + staticWhere
	for i := 0; i < 34; i++ {
		SQL = "SELECT e.id FROM (" + SQL + ") e"
	}
	_, err := projectionAnalysisSQL(SQL)
	require.ErrorContains(t, err, "recursive")
	SQL = "SELECT e.id AS __datly_analysis_predicate_0, '" + staticWhere + "' AS literal FROM events e " + staticWhere
	analysis, err := projectionAnalysisSQL(SQL)
	require.NoError(t, err)
	require.Contains(t, analysis, "e.id AS __datly_analysis_predicate_0")
	require.Contains(t, analysis, "'"+staticWhere+"'")
	require.Contains(t, analysis, "__datly_analysis_predicate__0 = 1")
}

func TestProjectionAnalysisTemplateLexicalBoundaries(t *testing.T) {
	for _, SQL := range []string{
		"SELECT `" + staticWhere + "` FROM events",
		"SELECT [" + staticWhere + "] FROM events",
		"SELECT 'literal } " + staticWhere + "' AS literal FROM events",
	} {
		analysis, err := projectionAnalysisSQL(SQL)
		require.NoError(t, err)
		require.Equal(t, SQL, analysis)
	}
	SQL := `SELECT e.id FROM events e ${predicate.Builder().CombineAnd("e.label = '}'").Build("WHERE")}`
	analysis, err := projectionAnalysisSQL(SQL)
	require.NoError(t, err)
	require.Contains(t, analysis, "WHERE (__datly_analysis_predicate_0 = 1)")
	for _, SQL := range []string{
		`SELECT e.id FROM events e ${predicate.Builder().Build("WHERE")`,
		`SELECT e.id FROM events e ${Other.Template}`,
		`SELECT e.id ${predicate.Builder().CombineAnd("1").Build(",")} FROM events e`,
	} {
		_, err := projectionAnalysisSQL(SQL)
		require.Error(t, err, SQL)
	}
}

func TestProjectionAnalysisInputValuesDoNotSupplyColumnAuthority(t *testing.T) {
	component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Jwt", Source: spec.BindSource{Kind: "header", Name: "Authorization"}, TypeExpr: "string", Codec: &spec.Codec{Body: "UnregisteredCodec"}}}}
	inputs := newAnalysisInputs(component, nil)
	SQL := `SELECT e.id, ${Jwt.UserID} AS owner FROM events e WHERE e.owner=${Jwt.UserID}`
	analysis, err := projectionAnalysisSQL(SQL, inputs)
	require.NoError(t, err, "unknown codec output type must not require construction during static analysis")
	require.Contains(t, analysis, ":__datly_analysis_predicate_value_")
	lineage, err := directProjectionLineage(&spec.ViewSource{SQL: analysis, Table: "events"})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"id": "id"}, lineage.direct)
	require.True(t, lineage.blocked["owner"])
	_, err = resolveResultSources([]*spec.Column{{Name: "First", Source: "id"}, {Name: "Second", Source: "id"}}, &spec.ViewSource{SQL: analysis, Table: "events"})
	require.ErrorContains(t, err, "ambiguous SQL result ownership")
	component.Parameters[0].Codec.OutputType = "int"
	_, err = projectionAnalysisSQL(SQL, inputs)
	require.ErrorContains(t, err, "struct type int", "known codec output metadata must override its string input type")
}
