package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlparser"
)

func TestInferSubqueryAlias(t *testing.T) {
	assert.Equal(t, "session", inferSubqueryAlias("(SELECT * FROM session) session JOIN (SELECT * FROM attr) attribute ON attribute.id = session.id"))
	assert.Equal(t, "x", inferSubqueryAlias("(SELECT 1) AS x"))
	assert.Equal(t, "publisherglobaloverride", inferSubqueryAlias(`(
    SELECT MIN(g.BUSINESS_MODEL_ID) AS BUSINESS_MODEL_ID
    FROM CI_GLOBAL_PUBLISHER_OVERRIDE g
) publisherglobaloverride`))
	assert.Equal(t, "", inferSubqueryAlias("orders o"))
}

func TestSanitizeName_AllCapsToLower(t *testing.T) {
	assert.Equal(t, "value", SanitizeName("VALUE"))
	assert.Equal(t, "status", SanitizeName("STATUS"))
}

func TestInferSubqueryTable(t *testing.T) {
	assert.Equal(t, "CI_ADVERTISER", inferSubqueryTable("(SELECT a.* FROM CI_ADVERTISER a) advertiser"))
	assert.Equal(t, "", inferSubqueryTable("orders o"))
}

func TestInferRoot_SubqueryFrom(t *testing.T) {
	queryNode, err := sqlparser.ParseQuery(`SELECT advertiser.* FROM (SELECT a.* FROM CI_ADVERTISER a) advertiser`)
	assert.NoError(t, err)
	name, table, err := InferRoot(queryNode, "fallback")
	assert.NoError(t, err)
	assert.Equal(t, "advertiser", name)
	assert.Equal(t, "CI_ADVERTISER", table)
}

func TestInferTableFromSQL_ResolvesTopLevelFrom(t *testing.T) {
	sqlText := `SELECT a.*, EXISTS(SELECT 1 FROM CI_ENTITY_WATCHLIST w WHERE w.ENTITY_ID = a.ID) AS watching FROM (SELECT x.* FROM CI_ADVERTISER x) a`
	assert.Equal(t, "CI_ADVERTISER", InferTableFromSQL(sqlText))
}
