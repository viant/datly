package column

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

func discoveryQuery(source *spec.ViewSource) (string, error) {
	if source == nil {
		return "", nil
	}
	text := strings.TrimSpace(source.SQL)
	if text == "" && strings.TrimSpace(source.Table) != "" {
		text = "SELECT * FROM " + strings.TrimSpace(source.Table)
	}
	if text == "" {
		return "", nil
	}
	parsed, err := sqlparser.ParseQuery(text)
	if err != nil {
		return "", fmt.Errorf("parse discovery SQL: %w", err)
	}
	if err := falsifySelect(parsed); err != nil {
		return "", err
	}
	parsed.Limit = nil
	parsed.Offset = nil
	result := strings.TrimSpace(sqlparser.Stringify(parsed))
	if result == "" {
		return "", fmt.Errorf("discovery SQL is empty after parsing")
	}
	return result, nil
}

func falsifySelect(selectNode *query.Select) error {
	if selectNode == nil {
		return nil
	}
	falsePredicate := &expr.Binary{X: expr.NewIntLiteral("1"), Op: "=", Y: expr.NewIntLiteral("0")}
	if selectNode.Qualify == nil || selectNode.Qualify.X == nil {
		selectNode.Qualify = &expr.Qualify{X: falsePredicate}
	} else {
		selectNode.Qualify = &expr.Qualify{X: &expr.Binary{X: falsePredicate, Op: "AND", Y: selectNode.Qualify.X}}
	}
	for _, item := range selectNode.WithSelects {
		if item == nil || item.X == nil {
			continue
		}
		if err := falsifySelect(item.X); err != nil {
			return err
		}
		item.Raw = ""
	}
	if selectNode.Union != nil {
		if err := falsifySelect(selectNode.Union.X); err != nil {
			return err
		}
	}
	if err := falsifySubquery(selectNode.From.X); err != nil {
		return fmt.Errorf("falsify FROM subquery: %w", err)
	}
	for _, join := range selectNode.Joins {
		if join == nil {
			continue
		}
		if err := falsifySubquery(join.With); err != nil {
			return fmt.Errorf("falsify JOIN subquery: %w", err)
		}
	}
	return nil
}

func falsifySubquery(source node.Node) error {
	var raw string
	var update func(string, *query.Select)
	switch actual := source.(type) {
	case *expr.Parenthesis:
		raw = actual.Raw
		update = func(rewritten string, parsed *query.Select) {
			actual.Raw = "(" + rewritten + ")"
			actual.X = parsed
		}
	case *expr.Raw:
		raw = actual.Raw
		update = func(rewritten string, parsed *query.Select) {
			actual.Raw = "(" + rewritten + ")"
			actual.X = parsed
		}
	default:
		return nil
	}
	raw = trimParentheses(raw)
	if raw == "" {
		return nil
	}
	parsed, err := sqlparser.ParseQuery(raw)
	if err != nil {
		return err
	}
	if err = falsifySelect(parsed); err != nil {
		return err
	}
	update(strings.TrimSpace(sqlparser.Stringify(parsed)), parsed)
	return nil
}

func trimParentheses(source string) string {
	result := strings.TrimSpace(source)
	if len(result) >= 2 && result[0] == '(' && result[len(result)-1] == ')' {
		return strings.TrimSpace(result[1 : len(result)-1])
	}
	return result
}
