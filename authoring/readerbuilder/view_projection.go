package readerbuilder

import (
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
)

func selectsAllPhysicalColumns(source string) bool {
	query, err := sqlparser.ParseQuery(maskTemplateExpressions(source), sqlparser.WithStructuralValidation())
	if err != nil || query == nil || len(query.List) != 1 || len(query.Joins) != 0 ||
		query.IsNested() || len(query.WithSelects) != 0 || query.Union != nil || strings.TrimSpace(query.Kind) != "" {
		return false
	}
	switch projection := query.List[0].Expr.(type) {
	case *expr.Star:
		return len(projection.Except) == 0
	case *expr.Selector:
		star, ok := projection.X.(*expr.Star)
		return ok && len(star.Except) == 0
	default:
		return false
	}
}
