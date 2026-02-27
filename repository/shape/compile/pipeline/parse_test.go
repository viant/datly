package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
)

func TestParseSelectWithDiagnostic_OK(t *testing.T) {
	queryNode, diag, err := ParseSelectWithDiagnostic("SELECT id FROM orders o")
	require.NoError(t, err)
	require.Nil(t, diag)
	require.NotNil(t, queryNode)
	assert.Equal(t, "o", queryNode.From.Alias)
}

func TestParseSelectWithDiagnostic_Syntax(t *testing.T) {
	queryNode, diag, err := ParseSelectWithDiagnostic("SELECT id FROM orders WHERE (")
	require.Error(t, err)
	require.Nil(t, queryNode)
	require.NotNil(t, diag)
	assert.Equal(t, dqldiag.CodeParseSyntax, diag.Code)
	assert.Equal(t, 1, diag.Span.Start.Line)
	assert.Greater(t, diag.Span.Start.Char, 1)
}

func TestParseSelectWithDiagnostic_LeadingBlockComment(t *testing.T) {
	queryNode, diag, err := ParseSelectWithDiagnostic("/* {\"URI\":\"/x\"} */\nSELECT id FROM orders o")
	require.NoError(t, err)
	require.Nil(t, diag)
	require.NotNil(t, queryNode)
	assert.Equal(t, "o", queryNode.From.Alias)
}
