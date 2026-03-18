package dml

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlstmt "github.com/viant/datly/repository/shape/dql/statement"
)

func TestCompile_Insert(t *testing.T) {
	sqlText := "INSERT INTO ORDERS(id) VALUES (1)"
	view, diags := Compile("orders_exec", sqlText, dqlstmt.New(sqlText))
	require.NotNil(t, view)
	assert.Equal(t, "ORDERS", view.Table)
	assert.Empty(t, diags)
}

func TestCompile_ServiceMissingArg(t *testing.T) {
	sqlText := "$sql.Insert($rec)"
	_, diags := Compile("orders_exec", sqlText, dqlstmt.New(sqlText))
	require.NotEmpty(t, diags)
	assert.Equal(t, dqldiag.CodeDMLServiceArg, diags[0].Code)
}
