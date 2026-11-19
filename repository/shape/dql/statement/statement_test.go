package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_ReadStatement(t *testing.T) {
	stmts := New("SELECT id FROM orders")
	require.Len(t, stmts, 1)
	assert.False(t, stmts[0].IsExec)
	assert.Equal(t, KindRead, stmts[0].Kind)
}

func TestNew_ExecStatements(t *testing.T) {
	sqlText := "INSERT INTO orders(id) VALUES (1)\nUPDATE orders SET name = 'x' WHERE id = 1"
	stmts := New(sqlText)
	require.Len(t, stmts, 2)
	assert.True(t, stmts[0].IsExec)
	assert.True(t, stmts[1].IsExec)
	assert.Equal(t, KindExec, stmts[0].Kind)
}

func TestNew_ServiceExec(t *testing.T) {
	stmts := New(`$sql.Insert("ORDERS", $rec)`)
	require.Len(t, stmts, 1)
	assert.True(t, stmts[0].IsExec)
	assert.Equal(t, KindService, stmts[0].Kind)
	assert.Equal(t, "Insert", stmts[0].SelectorMethod)
}

func TestStatements_DMLTables(t *testing.T) {
	stmts := New(`INSERT INTO orders(id) VALUES (1)
UPDATE orders SET id = 2
DELETE FROM items WHERE id = 1
$sql.Insert("ORDERS_AUDIT", $rec)`)
	tables := stmts.DMLTables(`INSERT INTO orders(id) VALUES (1)
UPDATE orders SET id = 2
DELETE FROM items WHERE id = 1
$sql.Insert("ORDERS_AUDIT", $rec)`)
	assert.Equal(t, []string{"orders", "items", "ORDERS_AUDIT"}, tables)
}

func TestNew_IgnoreKeywordsInCommentsAndStrings(t *testing.T) {
	sqlText := "-- insert into x\nSELECT 'update x' as txt FROM orders"
	stmts := New(sqlText)
	require.Len(t, stmts, 1)
	assert.Equal(t, KindRead, stmts[0].Kind)
	assert.False(t, stmts[0].IsExec)
}

func TestNew_DefaultUnknownIsNotExec(t *testing.T) {
	stmts := New("$foo.Bar($baz)")
	require.Len(t, stmts, 1)
	assert.Equal(t, "", stmts[0].Kind)
	assert.False(t, stmts[0].IsExec)
}

func TestNew_DefaultNopIsExec(t *testing.T) {
	stmts := New("$Nop($Unsafe.Id)")
	require.Len(t, stmts, 1)
	assert.Equal(t, KindExec, stmts[0].Kind)
	assert.True(t, stmts[0].IsExec)
	assert.Equal(t, "Nop", stmts[0].SelectorMethod)
}

func TestNew_NestedSubquerySelect_IsSingleReadStatement(t *testing.T) {
	sqlText := `SELECT session.*
FROM (SELECT * FROM session WHERE user_id = $criteria.AppendBinding($Unsafe.Jwt.UserID)) session
JOIN (SELECT * FROM session/attributes) attribute ON attribute.user_id = session.user_id`
	stmts := New(sqlText)
	require.Len(t, stmts, 1)
	assert.Equal(t, KindRead, stmts[0].Kind)
	assert.False(t, stmts[0].IsExec)
}
