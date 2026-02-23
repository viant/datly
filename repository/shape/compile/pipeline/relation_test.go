package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	"github.com/viant/sqlparser"
)

func TestExtractJoinRelations(t *testing.T) {
	sqlText := "SELECT o.id FROM orders o JOIN order_items i ON o.id = i.order_id"
	queryNode, err := sqlparser.ParseQuery(sqlText)
	require.NoError(t, err)
	relations, diags := ExtractJoinRelations(sqlText, queryNode)
	require.Len(t, relations, 1)
	assert.Equal(t, "i", relations[0].Ref)
	require.Len(t, relations[0].On, 1)
	assert.Equal(t, "id", relations[0].On[0].ParentColumn)
	assert.Equal(t, "order_id", relations[0].On[0].RefColumn)
	assert.Empty(t, diags)
}

func TestExtractJoinRelations_UnsupportedPredicate(t *testing.T) {
	sqlText := "SELECT o.id FROM orders o JOIN order_items i ON o.id > i.order_id"
	queryNode, err := sqlparser.ParseQuery(sqlText)
	require.NoError(t, err)
	_, diags := ExtractJoinRelations(sqlText, queryNode)
	require.NotEmpty(t, diags)
	assert.Equal(t, dqldiag.CodeRelUnsupported, diags[0].Code)
}

func TestExtractJoinRelations_WithAndLiteral(t *testing.T) {
	sqlText := "SELECT t.id FROM taxonomy t LEFT JOIN provider p ON p.id = t.provider_id AND 1=1"
	queryNode, err := sqlparser.ParseQuery(sqlText)
	require.NoError(t, err)
	relations, diags := ExtractJoinRelations(sqlText, queryNode)
	require.Len(t, relations, 1)
	require.Len(t, relations[0].On, 1)
	assert.Equal(t, "provider_id", relations[0].On[0].ParentColumn)
	assert.Equal(t, "id", relations[0].On[0].RefColumn)
	assert.Empty(t, diags)
}

func TestExtractJoinRelations_NonRootParentChain(t *testing.T) {
	sqlText := "SELECT sl.id FROM site_list sl JOIN site_list_match m ON m.site_list_id = sl.id JOIN ci_site s ON s.id = m.site_id JOIN ci_publisher p ON p.id = s.publisher_id"
	queryNode, err := sqlparser.ParseQuery(sqlText)
	require.NoError(t, err)
	relations, diags := ExtractJoinRelations(sqlText, queryNode)
	require.Len(t, relations, 3)

	require.Len(t, relations[0].On, 1)
	assert.Equal(t, "sl", relations[0].On[0].ParentNamespace)
	assert.Equal(t, "id", relations[0].On[0].ParentColumn)
	assert.Equal(t, "m", relations[0].On[0].RefNamespace)
	assert.Equal(t, "site_list_id", relations[0].On[0].RefColumn)

	require.Len(t, relations[1].On, 1)
	assert.Equal(t, "m", relations[1].On[0].ParentNamespace)
	assert.Equal(t, "site_id", relations[1].On[0].ParentColumn)
	assert.Equal(t, "s", relations[1].On[0].RefNamespace)
	assert.Equal(t, "id", relations[1].On[0].RefColumn)

	require.Len(t, relations[2].On, 1)
	assert.Equal(t, "s", relations[2].On[0].ParentNamespace)
	assert.Equal(t, "publisher_id", relations[2].On[0].ParentColumn)
	assert.Equal(t, "p", relations[2].On[0].RefNamespace)
	assert.Equal(t, "id", relations[2].On[0].RefColumn)
	assert.Empty(t, diags)
}

func TestExtractJoinRelations_DoesNotFallbackForComplexRawPredicate(t *testing.T) {
	sqlText := "SELECT o.id FROM orders o JOIN order_items i ON COALESCE(o.id, 0) = i.order_id"
	queryNode, err := sqlparser.ParseQuery(sqlText)
	require.NoError(t, err)
	relations, diags := ExtractJoinRelations(sqlText, queryNode)
	require.Len(t, relations, 1)
	assert.Empty(t, relations[0].On)
	require.NotEmpty(t, diags)
	assert.Equal(t, dqldiag.CodeRelUnsupported, diags[0].Code)
}

func TestShouldFallbackToRawJoinPairs(t *testing.T) {
	assert.True(t, shouldFallbackToRawJoinPairs("o.id = i.order_id"))
	assert.False(t, shouldFallbackToRawJoinPairs("COALESCE(o.id, 0) = i.order_id"))
	assert.False(t, shouldFallbackToRawJoinPairs("`o`.`id` = `i`.`order_id`"))
	assert.False(t, shouldFallbackToRawJoinPairs(`"o"."id" = "i"."order_id"`))
}
