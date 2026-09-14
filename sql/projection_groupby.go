package sql

import (
	"strconv"
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

func rewriteGroupedProjectionSQL(selectStmt *query.Select, selectedItems query.List) string {
	if selectStmt == nil || len(selectedItems) == 0 {
		return ""
	}
	selectStmt.List = selectedItems
	selectStmt.GroupBy = groupedProjectionGroupBy(selectedItems)
	selectStmt.OrderBy = filterGroupedOrderBy(selectStmt.OrderBy, selectedItems)
	return (sqlparser.Stringifier{PreserveWindow: true}).String(selectStmt)
}

func groupedProjectionNeedsRewrite(items query.List) bool {
	if len(items) == 0 {
		return false
	}
	hasAggregate := false
	hasNonAggregate := false
	for _, item := range items {
		if isAggregateSelectItem(item) {
			hasAggregate = true
			continue
		}
		hasNonAggregate = true
	}
	return hasAggregate && hasNonAggregate
}

func groupedProjectionGroupBy(items query.List) query.List {
	result := make(query.List, 0, len(items))
	for i, item := range items {
		if isAggregateSelectItem(item) {
			continue
		}
		result = append(result, query.NewItem(expr.NewIntLiteral(strconv.Itoa(i+1))))
	}
	return result
}

func filterGroupedOrderBy(orderBy query.List, items query.List) query.List {
	if len(orderBy) == 0 || len(items) == 0 {
		return orderBy
	}
	allowed := map[string]bool{}
	for _, item := range items {
		if item == nil {
			continue
		}
		if item.Expr != nil {
			allowed[normalizeExpression(sqlparser.Stringify(item.Expr))] = true
		}
		if item.Alias != "" {
			allowed[normalizeExpression(item.Alias)] = true
		}
	}
	result := make(query.List, 0, len(orderBy))
	for _, item := range orderBy {
		if item == nil || item.Expr == nil {
			continue
		}
		if allowed[normalizeExpression(sqlparser.Stringify(item.Expr))] {
			result = append(result, item)
		}
	}
	return result
}

func normalizeExpression(value string) string {
	return strings.ToUpper(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
}

func isAggregateSelectItem(item *query.Item) bool {
	if item == nil || item.Expr == nil {
		return false
	}
	return containsAggregateNode(item.Expr)
}

func containsAggregateNode(n node.Node) bool {
	switch actual := n.(type) {
	case nil:
		return false
	case *expr.Call:
		if actual.X != nil {
			switch ident := actual.X.(type) {
			case *expr.Ident:
				if isAggregateFunction(ident.Name) {
					return true
				}
			case *expr.Selector:
				if isAggregateFunction(ident.Name) {
					return true
				}
			}
			if containsAggregateNode(actual.X) {
				return true
			}
		}
		for _, arg := range actual.Args {
			if containsAggregateNode(arg) {
				return true
			}
		}
		return false
	case *expr.Parenthesis:
		return containsAggregateNode(actual.X)
	case *expr.Unary:
		return containsAggregateNode(actual.X)
	case *expr.Binary:
		return containsAggregateNode(actual.X) || containsAggregateNode(actual.Y)
	case *expr.Raw:
		return containsAggregateNode(actual.X)
	case *expr.Switch:
		for _, item := range actual.Cases {
			if item == nil {
				continue
			}
			if containsAggregateNode(item.X) || containsAggregateNode(item.Y) {
				return true
			}
		}
		return false
	case *expr.Qualify:
		return containsAggregateNode(actual.X)
	}
	return false
}

func isAggregateFunction(name string) bool {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "SUM", "COUNT", "AVG", "MIN", "MAX", "ARRAY_AGG", "STRING_AGG", "ANY_VALUE", "APPROX_COUNT_DISTINCT":
		return true
	default:
		return false
	}
}
