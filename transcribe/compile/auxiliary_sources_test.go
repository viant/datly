package compile

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
	"strings"
	"testing"
)

func TestNestedAuxiliarySourcesLowerToExecutableSQL(t *testing.T) {
	type useCase struct {
		description, input                  string
		expectedAuxiliary                   bool
		expectedSource, expectedRestriction string
	}
	for _, tc := range []useCase{
		{"derived root", "SELECT b.* FROM (SELECT r.id AS SourceId FROM (records) r WHERE r.id>0) b", true, "records r", "r.id"},
		{"nested join", "SELECT b.* FROM (SELECT r.* FROM (records) r JOIN (kinds) k ON k.id=r.kind_id WHERE k.id>0) b", true, "kinds k", "k.id"},
		{"CTE", "WITH rows AS (SELECT r.* FROM (records) r WHERE r.id>0) SELECT x.* FROM rows x", true, "records r", "r.id"},
	} {
		t.Run(tc.description, func(t *testing.T) {
			input := &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: tc.input}}
			result, err := NewReader().Compile(ReadInput{View: input, SQL: tc.input})
			require.NoError(t, err)
			require.Equal(t, tc.expectedAuxiliary, result.Auxiliary)
			require.NotContains(t, result.Source.SQL, "(records)")
			require.NotContains(t, result.Source.SQL, "(kinds)")
			require.Contains(t, result.Source.SQL, tc.expectedSource)
			require.Contains(t, result.Source.SQL, tc.expectedRestriction)
			require.Equal(t, tc.input, input.Source.SQL)
		})
	}
	const source = "SELECT p.*, child.* FROM parents p LEFT JOIN (SELECT c.* FROM (children) c WHERE c.id>0) child ON p.id=child.parent_id"
	result, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Parents", Source: &spec.ViewSource{SQL: source}}, SQL: source})
	require.NoError(t, err)
	require.False(t, result.Auxiliary)
	require.Len(t, result.Relations, 1)
	child := result.Relations[0].View
	require.True(t, child.Auxiliary)
	require.NotContains(t, child.Source.SQL, "(children)")
	require.Contains(t, child.Source.SQL, "children c")
	require.True(t, strings.Contains(child.Source.SQL, "c.id"))
}

func TestAuxiliaryLoweringPreservesLiteralAndExpressionParentheses(t *testing.T) {
	const source = "SELECT r.id, '(records)' AS note FROM (SELECT r.* FROM (records) r WHERE (r.id+1)>0) r"
	result, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: source}}, SQL: source})
	require.NoError(t, err)
	require.True(t, result.Auxiliary)
	require.Contains(t, result.Source.SQL, "'(records)'")
	require.Contains(t, result.Source.SQL, "(r.id+1)")
	require.NotContains(t, result.Source.SQL, "FROM (records)")
}

func TestAuxiliaryLoweringPreservesUnionOperator(t *testing.T) {
	parsed, err := parseReadSQL("SELECT r.id FROM (records) r UNION ALL SELECT k.id FROM (kinds) k")
	require.NoError(t, err)
	changed, err := lowerAuxiliarySources(parsed)
	require.NoError(t, err)
	require.True(t, changed)
	sql := (sqlparser.Stringifier{PreserveWindow: true}).String(parsed)
	require.Contains(t, sql, "UNION ALL")
	require.Contains(t, sql, "records r")
	require.Contains(t, sql, "kinds k")
	require.NotContains(t, sql, "(records)")
	require.NotContains(t, sql, "(kinds)")
}
