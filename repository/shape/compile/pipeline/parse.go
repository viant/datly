package pipeline

import (
	"strings"

	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
)

func ParseSelectWithDiagnostic(sqlText string) (*query.Select, *dqlshape.Diagnostic, error) {
	sqlText = trimLeadingBlockComments(sqlText)
	var diagnostic *dqlshape.Diagnostic
	onError := func(err error, cur *parsly.Cursor, _ interface{}) error {
		offset := 0
		if cur != nil {
			offset = cur.Pos
		}
		if offset < 0 {
			offset = 0
		}
		diagnostic = &dqlshape.Diagnostic{
			Code:     dqldiag.CodeParseSyntax,
			Severity: dqlshape.SeverityError,
			Message:  strings.TrimSpace(err.Error()),
			Hint:     "check SQL syntax near the reported location",
			Span:     pointSpan(sqlText, offset),
		}
		return err
	}
	result, err := sqlparser.ParseQuery(sqlText, sqlparser.WithErrorHandler(onError))
	if err != nil {
		if diagnostic == nil {
			diagnostic = &dqlshape.Diagnostic{
				Code:     dqldiag.CodeParseSyntax,
				Severity: dqlshape.SeverityError,
				Message:  strings.TrimSpace(err.Error()),
				Hint:     "check SQL syntax near the reported location",
				Span:     pointSpan(sqlText, 0),
			}
		}
		return nil, diagnostic, err
	}
	if result == nil {
		return nil, nil, nil
	}
	return result, nil, nil
}

func trimLeadingBlockComments(sqlText string) string {
	remaining := strings.TrimLeft(sqlText, " \t\r\n")
	for strings.HasPrefix(remaining, "/*") {
		end := strings.Index(remaining, "*/")
		if end == -1 {
			return remaining
		}
		remaining = strings.TrimLeft(remaining[end+2:], " \t\r\n")
	}
	return remaining
}
