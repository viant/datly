package preprocess

import dqlshape "github.com/viant/datly/repository/shape/dql/shape"

func directiveDiagnostic(code, message, hint, text string, offset int) *dqlshape.Diagnostic {
	return &dqlshape.Diagnostic{
		Code:     code,
		Severity: dqlshape.SeverityError,
		Message:  message,
		Hint:     hint,
		Span:     pointSpan(text, offset),
	}
}
