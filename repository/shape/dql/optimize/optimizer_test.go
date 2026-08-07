package optimize

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
)

func TestRewrite_SelectorInterpolationReportsPosition(t *testing.T) {
	input := "SELECT id FROM orders WHERE id = $Unsafe.Id"
	_, diagnostics := Rewrite(input)
	require.NotEmpty(t, diagnostics)
	diag := diagnostics[0]
	assert.Equal(t, dqldiag.CodeSQLIRawSelector, diag.Code)
	assert.Equal(t, 1, diag.Span.Start.Line)
	assert.Greater(t, diag.Span.Start.Char, 1)
}

func TestRewrite_ParseFailureFallsBack(t *testing.T) {
	input := "#if(true)"
	out, diagnostics := Rewrite(input)
	assert.Equal(t, input, out)
	require.NotEmpty(t, diagnostics)
	assert.Equal(t, dqldiag.CodeOptParse, diagnostics[len(diagnostics)-1].Code)
	assert.True(t, strings.Contains(strings.ToLower(diagnostics[len(diagnostics)-1].Message), "optimization pass"))
}

func TestRewrite_NopDoesNotReportSelectorInterpolation(t *testing.T) {
	input := "SELECT 1 WHERE 1=1 $Nop($Unsafe.Id)"
	_, diagnostics := Rewrite(input)
	for _, item := range diagnostics {
		if item == nil {
			continue
		}
		assert.NotEqual(t, dqldiag.CodeSQLIRawSelector, item.Code)
	}
}
