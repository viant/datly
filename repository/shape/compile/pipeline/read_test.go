package pipeline

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
)

func TestBuildRead(t *testing.T) {
	view, diags, err := BuildRead("orders_report", "SELECT o.id, i.sku FROM orders o JOIN items i ON o.id = i.order_id")
	require.NoError(t, err)
	require.NotNil(t, view)
	assert.Equal(t, "o", view.Name)
	assert.Equal(t, "orders", view.Table)
	assert.Equal(t, "many", view.Cardinality)
	require.Len(t, view.Relations, 1)
	assert.Equal(t, "i", view.Relations[0].Ref)
	assert.Empty(t, diags)
}

func TestBuildRead_SubqueryJoin_UsesParentNamespaceAsRoot(t *testing.T) {
	sqlText := `SELECT session.*
FROM (SELECT * FROM session WHERE user_id = $criteria.AppendBinding($Unsafe.Jwt.UserID)) session
JOIN (SELECT * FROM session/attributes) attribute ON attribute.user_id = session.user_id`
	view, _, err := BuildRead("system/session", sqlText)
	require.NoError(t, err)
	require.NotNil(t, view)
	assert.Equal(t, "session", view.Name)
	assert.Equal(t, "session", view.Table)
	require.NotEmpty(t, view.Relations)
	assert.Equal(t, "attribute", view.Relations[0].Ref)
}

func TestNormalizeParserSQL(t *testing.T) {
	input := "SELECT * FROM session WHERE user_id = $criteria.AppendBinding($Unsafe.Jwt.UserID) AND x = $Jwt.UserID"
	actual := normalizeParserSQL(input)
	assert.NotContains(t, actual, "$criteria.AppendBinding")
	assert.NotContains(t, actual, "$Jwt.UserID")
	assert.Contains(t, actual, "user_id = 1")
}

func TestNormalizeParserSQL_VeltyBlockExpression(t *testing.T) {
	input := `SELECT b.* FROM CI_BROWSER b ${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")} AND b.ARCHIVED = 0`
	actual := normalizeParserSQL(input)
	assert.NotContains(t, actual, "${predicate.Builder()")
	assert.Contains(t, actual, "SELECT b.* FROM CI_BROWSER b  WHERE 1  AND b.ARCHIVED = 0")
}

func TestNormalizeParserSQL_PrivateShorthand(t *testing.T) {
	input := `SELECT private(audience.FREQ_CAPPING) AS freq_capping FROM CI_AUDIENCE audience`
	actual := normalizeParserSQL(input)
	assert.NotContains(t, strings.ToLower(actual), "private(")
	assert.Contains(t, actual, "SELECT audience.FREQ_CAPPING AS freq_capping FROM CI_AUDIENCE audience")
}

func TestNeedsFallbackParse(t *testing.T) {
	assert.True(t, needsFallbackParse("SELECT * FROM t JOIN x ON t.id = x.id", &query.Select{}))
	assert.False(t, needsFallbackParse("SELECT * FROM t", &query.Select{From: query.From{X: expr.NewSelector("t")}}))
}

func TestBuildRead_FallbackWhenInitialParseFails(t *testing.T) {
	sqlText := `SELECT b.* FROM CI_BROWSER b ${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")} AND b.ARCHIVED = 0`
	view, diags, err := BuildRead("browser", sqlText)
	require.NoError(t, err)
	require.NotNil(t, view)
	assert.Equal(t, "b", view.Name)
	assert.Equal(t, "CI_BROWSER", view.Table)
	assert.Empty(t, diags)
}
