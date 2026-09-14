package sql

import (
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	sqltext "github.com/viant/sqlparser/source"
)

func normalizeProjectionItem(item *query.Item, original string) (string, bool, bool) {
	original = strings.TrimSpace(original)
	if item == nil || item.Expr == nil {
		return "", false, false
	}
	call, ok := item.Expr.(*expr.Call)
	if !ok {
		return original, true, false
	}
	name := projectionCallName(call, original)
	lowerName := strings.ToLower(name)
	if removableSelectCalls[lowerName] {
		return "", false, true
	}
	if (lowerName == "tag" || lowerName == "required") && len(call.Args) > 0 {
		normalized := applyAlias(strings.TrimSpace(sqlparser.Stringify(call.Args[0])), item.Alias)
		return normalized, true, normalized != original
	}
	if lowerName == "cast" && len(call.Raw) >= 2 && call.Raw[0] == '(' && call.Raw[len(call.Raw)-1] == ')' {
		if castExpr := normalizeCastArg(call.Raw[1 : len(call.Raw)-1]); castExpr != "" {
			normalized := applyAlias(castExpr, item.Alias)
			return normalized, true, normalized != original
		}
	}
	return original, true, false
}

func projectionCallName(call *expr.Call, original string) string {
	if call != nil {
		if candidate := strings.TrimSpace(sqlparser.Stringify(call.X)); candidate != "" {
			if _, ok := removableSelectCalls[strings.ToLower(candidate)]; ok {
				return candidate
			}
			switch strings.ToLower(candidate) {
			case "tag", "required", "cast":
				return candidate
			}
		}
	}
	core, _ := sqltext.SplitTopLevelAlias(original)
	core = strings.TrimSpace(core)
	if idx := strings.IndexByte(core, '('); idx > 0 {
		return strings.TrimSpace(core[:idx])
	}
	return core
}

func normalizeCastArg(arg string) string {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return ""
	}
	lower := strings.ToLower(arg)
	asIndex := sqltext.FindLastTopLevelKeyword(lower, "as", 0)
	if asIndex == -1 {
		return ""
	}
	valueExpr := strings.TrimSpace(arg[:asIndex])
	typeExpr := strings.TrimSpace(arg[asIndex+len("as"):])
	typeExpr = strings.Trim(typeExpr, `"'`)
	if valueExpr == "" || typeExpr == "" {
		return ""
	}
	return "CAST(" + valueExpr + " AS " + typeExpr + ")"
}

func applyAlias(exprText string, alias string) string {
	exprText = strings.TrimSpace(exprText)
	alias = strings.TrimSpace(alias)
	if exprText == "" {
		return exprText
	}
	if alias == "" {
		return exprText
	}
	return exprText + " AS " + alias
}
