package readerbuilder

import (
	"fmt"
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	sqltext "github.com/viant/sqlparser/source"
)

// validateSimpleGroupedMain applies the deliberately narrow initial cube
// authoring contract. It validates the database SQL inside the first/root
// wrapped view; outer DQL metadata is compiled separately by Service.Apply.
func validateSimpleGroupedMain(source string) error {
	views, _, err := inspectViewSources(source)
	if err != nil {
		return err
	}
	if len(views) == 0 {
		return fmt.Errorf("cube activation requires a wrapped main view")
	}
	main := views[0]
	inner := maskTemplateExpressions(source[main.SourceSpan.Start:main.SourceSpan.End])
	parsed, err := sqlparser.ParseQuery(inner, sqlparser.WithStructuralValidation())
	if err != nil {
		return fmt.Errorf("cube main view %q SQL: %w", main.Name, err)
	}
	if parsed == nil || len(parsed.Joins) > 0 || parsed.Union != nil || parsed.Having != nil || parsed.Window != nil || len(parsed.WithSelects) > 0 || !strings.EqualFold(strings.TrimSpace(parsed.Kind), "") {
		return fmt.Errorf("cube main view %q must use one simple grouped SELECT without joins, CTEs, HAVING, windows, DISTINCT or set operations", main.Name)
	}
	if len(parsed.GroupBy) == 0 {
		return fmt.Errorf("cube main view %q requires GROUP BY", main.Name)
	}
	groups := map[string]bool{}
	for _, item := range parsed.GroupBy {
		if item != nil && item.Expr != nil {
			groups[normalizeGroupedExpression(sqlparser.Stringify(item.Expr))] = true
		}
	}
	dimensions, measures := 0, 0
	for _, item := range parsed.List {
		if item == nil || item.Expr == nil {
			return fmt.Errorf("cube main view %q contains an empty projection", main.Name)
		}
		if call, ok := item.Expr.(*expr.Call); ok && cubeAggregate(call) {
			if strings.TrimSpace(item.Alias) == "" {
				return fmt.Errorf("cube aggregate %q requires a unique output alias", sqlparser.Stringify(item.Expr))
			}
			measures++
			continue
		}
		if _, star := item.Expr.(*expr.Star); star {
			return fmt.Errorf("cube main view %q cannot use SELECT *", main.Name)
		}
		expression := normalizeGroupedExpression(sqlparser.Stringify(item.Expr))
		alias := normalizeGroupedExpression(item.Alias)
		if !groups[expression] && (alias == "" || !groups[alias]) {
			return fmt.Errorf("cube dimension %q is not present in GROUP BY", sqlparser.Stringify(item.Expr))
		}
		dimensions++
	}
	if dimensions == 0 || measures == 0 {
		return fmt.Errorf("cube main view %q requires at least one dimension and one aggregate measure", main.Name)
	}
	return nil
}

func cubeAggregate(call *expr.Call) bool {
	if call == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(sqlparser.Stringify(call.X))) {
	case "sum", "count", "min", "max", "avg":
		return true
	default:
		return false
	}
}

func normalizeGroupedExpression(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
}

func maskTemplateExpressions(source string) string {
	result := []byte(source)
	for offset := 0; offset < len(source); {
		index := strings.Index(source[offset:], "${")
		if index < 0 {
			break
		}
		index += offset
		group, end, ok := sqltext.ReadGroupString(source, index+1, '{', '}')
		if !ok {
			break
		}
		_ = group
		for i := index; i < end; i++ {
			if result[i] != '\n' && result[i] != '\r' {
				result[i] = ' '
			}
		}
		offset = end
	}
	return string(result)
}
