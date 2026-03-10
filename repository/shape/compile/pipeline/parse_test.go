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
	assert.Equal(t, 29, diag.Span.Start.Char)
}

func TestParseSelectWithDiagnostic_LeadingBlockComment(t *testing.T) {
	queryNode, diag, err := ParseSelectWithDiagnostic("/* {\"URI\":\"/x\"} */\nSELECT id FROM orders o")
	require.NoError(t, err)
	require.Nil(t, diag)
	require.NotNil(t, queryNode)
	assert.Equal(t, "o", queryNode.From.Alias)
}

func TestParseSelectWithDiagnostic_SyntaxPositionMatrix(t *testing.T) {
	testCases := []struct {
		name         string
		sql          string
		expectedLine int
		expectedChar int
	}{
		{
			name:         "plain sql",
			sql:          "SELECT id FROM orders WHERE (",
			expectedLine: 1,
			expectedChar: 29,
		},
		{
			name:         "with leading block comment",
			sql:          "/* {\"URI\":\"/x\"} */\nSELECT id FROM orders WHERE (",
			expectedLine: 2,
			expectedChar: 29,
		},
		{
			name:         "with multiple leading lines and comments",
			sql:          "\n\n/*a*/\n/*b*/\nSELECT id FROM orders WHERE (",
			expectedLine: 5,
			expectedChar: 29,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			queryNode, diag, err := ParseSelectWithDiagnostic(testCase.sql)
			require.Error(t, err)
			require.Nil(t, queryNode)
			require.NotNil(t, diag)
			assert.Equal(t, dqldiag.CodeParseSyntax, diag.Code)
			assert.Equal(t, testCase.expectedLine, diag.Span.Start.Line)
			assert.Equal(t, testCase.expectedChar, diag.Span.Start.Char)
		})
	}
}
