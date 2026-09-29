package optimize

import (
	"strings"

	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/velty"
	"github.com/viant/velty/ast"
	aexpr "github.com/viant/velty/ast/expr"
)

// Rewrite applies lightweight template simplification and emits diagnostics.
// It is intentionally conservative: only dead #if(false) blocks without else are blanked.
func Rewrite(dql string) (string, []*dqlshape.Diagnostic) {
	if strings.TrimSpace(dql) == "" {
		return dql, nil
	}
	adjuster := &hookAdjuster{source: []byte(dql), seenOffset: map[int]struct{}{}}
	out, err := velty.TransformTemplate([]byte(dql), adjuster)
	if err != nil {
		adjuster.diagnostics = append(adjuster.diagnostics, &dqlshape.Diagnostic{
			Code:     dqldiag.CodeOptParse,
			Severity: dqlshape.SeverityWarning,
			Message:  "velty optimization pass skipped due to parse issue",
			Hint:     "check template syntax near directives and expressions",
			Span:     dqlshape.Span{Start: dqlshape.Position{Line: 1, Char: 1}, End: dqlshape.Position{Line: 1, Char: 1}},
		})
		return dql, adjuster.diagnostics
	}
	return string(out), adjuster.diagnostics
}

type hookAdjuster struct {
	source      []byte
	seenOffset  map[int]struct{}
	diagnostics []*dqlshape.Diagnostic
}

func (a *hookAdjuster) Adjust(node ast.Node, ctx *velty.ParserContext) (velty.Action, error) {
	switch actual := node.(type) {
	case *aexpr.Select:
		a.captureSQLInjectionRisk(actual, ctx)
	}
	return velty.Keep(), nil
}

func (a *hookAdjuster) captureSQLInjectionRisk(sel *aexpr.Select, ctx *velty.ParserContext) {
	if sel == nil || ctx == nil {
		return
	}
	if ctx.CurrentExprContext().Kind == velty.CtxSetLHS {
		return
	}
	span, ok := ctx.GetSpan(sel)
	if !ok {
		return
	}
	if strings.EqualFold(sel.ID, "Nop") {
		return
	}
	if a.inNoopCall(span.Start) {
		return
	}
	if _, exists := a.seenOffset[span.Start]; exists {
		return
	}
	a.seenOffset[span.Start] = struct{}{}
	pos := ctx.ResolvePosition(span)
	a.diagnostics = append(a.diagnostics, &dqlshape.Diagnostic{
		Code:     dqldiag.CodeSQLIRawSelector,
		Severity: dqlshape.SeverityWarning,
		Message:  "raw selector interpolation detected in SQL template",
		Hint:     "prefer bind parameters or validated allow-listed fragments",
		Span: dqlshape.Span{
			Start: dqlshape.Position{Offset: span.Start, Line: pos.Line, Char: pos.Col},
			End:   dqlshape.Position{Offset: span.End, Line: pos.EndLine, Char: pos.EndCol},
		},
	})
}

func (a *hookAdjuster) inNoopCall(pos int) bool {
	if pos <= 0 || pos > len(a.source) {
		return false
	}
	prefix := string(a.source[:pos])
	nopPos := strings.LastIndex(prefix, "$Nop(")
	if nopPos == -1 {
		nopPos = strings.LastIndex(prefix, "$nop(")
	}
	if nopPos == -1 {
		return false
	}
	if nl := strings.LastIndex(prefix, "\n"); nl > nopPos {
		return false
	}
	segment := prefix[nopPos:pos]
	return strings.Count(segment, "(") > strings.Count(segment, ")")
}
