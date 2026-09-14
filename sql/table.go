package sql

import (
	"fmt"
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

// ReplaceTable replaces actual table-reference AST nodes, including CTE and
// union branches. It never performs textual replacement inside expressions,
// identifiers, literals, or comments.
func ReplaceTable(sqlText, source, target string) (string, error) {
	source = strings.TrimSpace(source)
	target = strings.TrimSpace(target)
	if source == "" || target == "" || equalTableName(source, target) {
		return sqlText, nil
	}
	parsed, err := sqlparser.ParseQuery(sqlText)
	if err != nil || parsed == nil {
		return "", fmt.Errorf("parse SQL for partition table replacement: %w", err)
	}
	targetNode, err := parseTableNode(target)
	if err != nil {
		return "", err
	}
	replaced := replaceSelectTables(parsed, source, targetNode)
	if replaced == 0 {
		return "", fmt.Errorf("partition source table %q was not found", source)
	}
	return sqlparser.Stringify(parsed), nil
}

func parseTableNode(tableName string) (node.Node, error) {
	parsed, err := sqlparser.ParseQuery("SELECT 1 FROM " + tableName)
	if err != nil || parsed == nil || parsed.From.X == nil || parsed.From.Alias != "" || strings.TrimSpace(parsed.From.Unparsed) != "" {
		if err == nil {
			err = fmt.Errorf("invalid table reference")
		}
		return nil, fmt.Errorf("invalid partition table %q: %w", tableName, err)
	}
	return parsed.From.X, nil
}

func replaceSelectTables(selectStmt *query.Select, source string, target node.Node) int {
	if selectStmt == nil {
		return 0
	}
	replaced := replaceFromTable(&selectStmt.From, source, target)
	for _, join := range selectStmt.Joins {
		if join == nil {
			continue
		}
		if equalTableNode(join.With, source) {
			join.With = cloneTableNode(target)
			replaced++
		} else {
			replaced += replaceNestedTableNode(join.With, source, target)
		}
	}
	for _, with := range selectStmt.WithSelects {
		if with != nil {
			withReplaced := replaceSelectTables(with.X, source, target)
			if withReplaced > 0 {
				with.Raw = "(" + sqlparser.Stringify(with.X) + ")"
			}
			replaced += withReplaced
		}
	}
	if selectStmt.Union != nil {
		replaced += replaceSelectTables(selectStmt.Union.X, source, target)
	}
	return replaced
}

func replaceFromTable(from *query.From, source string, target node.Node) int {
	if from == nil || from.X == nil {
		return 0
	}
	if equalTableNode(from.X, source) {
		from.X = cloneTableNode(target)
		return 1
	}
	return replaceNestedTableNode(from.X, source, target)
}

func replaceNestedTableNode(candidate node.Node, source string, target node.Node) int {
	raw, ok := candidate.(*expr.Raw)
	if !ok || raw == nil {
		return 0
	}
	nested, ok := raw.X.(*query.Select)
	if !ok {
		return 0
	}
	replaced := replaceSelectTables(nested, source, target)
	if replaced > 0 {
		raw.Raw = "(" + sqlparser.Stringify(nested) + ")"
		raw.Unparsed = ""
	}
	return replaced
}

func equalTableNode(candidate node.Node, expected string) bool {
	switch candidate.(type) {
	case *expr.Ident, *expr.Selector:
		return equalTableName(sqlparser.Stringify(candidate), expected)
	default:
		return false
	}
}

func equalTableName(left, right string) bool {
	normalize := func(value string) string {
		value = strings.TrimSpace(strings.ToLower(value))
		return strings.Trim(value, "`")
	}
	return normalize(left) == normalize(right)
}

func cloneTableNode(candidate node.Node) node.Node {
	return expr.NewSelector(sqlparser.Stringify(candidate))
}
