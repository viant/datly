package column

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
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
	if err != nil || parsed == nil || len(parsed.List) == 0 {
		// Inner dialect SQL is database-owned. Preserve it as an opaque source
		// and request zero-row metadata rather than guessing or sampling types.
		return "SELECT * FROM (" + strings.TrimSuffix(text, ";") + ") datly_discovery WHERE 1=0", nil
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
	if selectNode.Union != nil {
		if err := falsifySelect(selectNode.Union.X); err != nil {
			return err
		}
	}
	return nil
}

func trimParentheses(source string) string {
	result := strings.TrimSpace(source)
	if len(result) >= 2 && result[0] == '(' && result[len(result)-1] == ')' {
		return strings.TrimSpace(result[1 : len(result)-1])
	}
	return result
}
