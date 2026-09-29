package compile

import (
	"fmt"

	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/typectx"
)

func typeContextDiagnostics(ctx *typectx.Context, strict bool) []*dqlshape.Diagnostic {
	issues := typectx.Validate(ctx)
	if len(issues) == 0 {
		return nil
	}
	severity := dqlshape.SeverityWarning
	if strict {
		severity = dqlshape.SeverityError
	}
	diags := make([]*dqlshape.Diagnostic, 0, len(issues))
	for _, issue := range issues {
		if issue.Field == "" || issue.Message == "" {
			continue
		}
		diags = append(diags, &dqlshape.Diagnostic{
			Code:     dqldiag.CodeTypeCtxInvalid,
			Severity: severity,
			Message:  fmt.Sprintf("type context %s: %s", issue.Field, issue.Message),
			Hint:     "set consistent TypeContext package fields or use compile overrides",
			Span: dqlshape.Span{
				Start: dqlshape.Position{Line: 1, Char: 1},
				End:   dqlshape.Position{Line: 1, Char: 1},
			},
		})
	}
	return diags
}
