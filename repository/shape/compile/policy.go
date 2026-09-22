package compile

import (
	"strings"

	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
)

func hasEscalationWarnings(diags []*dqlshape.Diagnostic) bool {
	for _, item := range diags {
		if item == nil {
			continue
		}
		if item.Severity != dqlshape.SeverityWarning {
			continue
		}
		if strings.HasPrefix(item.Code, dqldiag.PrefixRel) || strings.HasPrefix(item.Code, dqldiag.PrefixSQLI) {
			return true
		}
	}
	return false
}

func hasErrorDiagnostics(diags []*dqlshape.Diagnostic) bool {
	for _, item := range diags {
		if item == nil {
			continue
		}
		if item.Severity == dqlshape.SeverityError {
			return true
		}
	}
	return false
}

func filterEscalationDiagnostics(diags []*dqlshape.Diagnostic) []*dqlshape.Diagnostic {
	var result []*dqlshape.Diagnostic
	for _, item := range diags {
		if item == nil {
			continue
		}
		if strings.HasPrefix(item.Code, dqldiag.PrefixRel) || strings.HasPrefix(item.Code, dqldiag.PrefixSQLI) {
			result = append(result, item)
		}
	}
	return result
}
