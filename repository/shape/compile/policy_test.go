package compile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
)

func TestPolicy_HasEscalationWarnings(t *testing.T) {
	diags := []*dqlshape.Diagnostic{
		{Code: dqldiag.CodeRelAmbiguous, Severity: dqlshape.SeverityWarning},
	}
	assert.True(t, hasEscalationWarnings(diags))
	assert.False(t, hasEscalationWarnings([]*dqlshape.Diagnostic{
		{Code: dqldiag.CodeViewMissingSQL, Severity: dqlshape.SeverityWarning},
	}))
}

func TestPolicy_HasErrorDiagnostics(t *testing.T) {
	assert.True(t, hasErrorDiagnostics([]*dqlshape.Diagnostic{
		{Code: dqldiag.CodeParseSyntax, Severity: dqlshape.SeverityError},
	}))
	assert.False(t, hasErrorDiagnostics([]*dqlshape.Diagnostic{
		{Code: dqldiag.CodeRelAmbiguous, Severity: dqlshape.SeverityWarning},
	}))
}

func TestPolicy_FilterEscalationDiagnostics(t *testing.T) {
	diags := []*dqlshape.Diagnostic{
		{Code: dqldiag.CodeViewMissingSQL, Severity: dqlshape.SeverityWarning},
		{Code: dqldiag.CodeSQLIRawSelector, Severity: dqlshape.SeverityWarning},
		{Code: dqldiag.CodeRelNoLinks, Severity: dqlshape.SeverityWarning},
	}
	filtered := filterEscalationDiagnostics(diags)
	assert.Len(t, filtered, 2)
	assert.Equal(t, dqldiag.CodeSQLIRawSelector, filtered[0].Code)
	assert.Equal(t, dqldiag.CodeRelNoLinks, filtered[1].Code)
}
