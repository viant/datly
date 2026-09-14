package sql

import (
	"fmt"
	"strings"

	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	sqltext "github.com/viant/sqlparser/source"
)

func applySetProjection(sqlText string, items query.List, sourceItems []string) (string, error) {
	if len(items) == 0 || len(items) != len(sourceItems) {
		return sqlText, nil
	}
	columns := make([]string, 0, len(items))
	for i, item := range items {
		name, err := setProjectionOutputName(item, sourceItems[i])
		if err != nil {
			return "", err
		}
		columns = append(columns, name)
	}
	source := strings.TrimSpace(sqlText)
	source = strings.TrimSuffix(source, ";")
	return "SELECT " + strings.Join(columns, ", ") + " FROM (" + source + ") AS datly_set", nil
}

func setProjectionOutputName(item *query.Item, source string) (string, error) {
	if item == nil || item.Expr == nil {
		return "", fmt.Errorf("set query projection item is empty")
	}
	if alias := strings.TrimSpace(item.Alias); alias != "" {
		return alias, nil
	}
	if _, alias := sqltext.SplitTopLevelAlias(source); alias != "" {
		return alias, nil
	}
	if name := terminalProjectionName(item.Expr); name != "" {
		return name, nil
	}
	return "", fmt.Errorf("set query projection expression %q requires an alias", strings.TrimSpace(source))
}

func terminalProjectionName(candidate node.Node) string {
	switch actual := candidate.(type) {
	case *expr.Ident:
		return strings.TrimSpace(actual.Name)
	case *expr.Selector:
		if name := terminalProjectionName(actual.X); name != "" {
			return name
		}
		return strings.TrimSpace(actual.Name)
	default:
		return ""
	}
}
