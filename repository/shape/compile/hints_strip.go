package compile

import (
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
)

var projectionHintCalls = map[string]bool{
	"useconnector": true,
	"allownulls":   true,
	"setlimit":     true,
	"setcache":     true,
	"cardinality":  true,
	"selfref":      true,
	"dest":         true,
	"type":         true,
}

// stripProjectionHintCalls removes hint-only projection functions (e.g. self_ref, dest)
// from executable SQL while preserving metadata extraction from original DQL.
func stripProjectionHintCalls(sqlText string) string {
	sqlText = strings.TrimSpace(sqlText)
	if sqlText == "" {
		return sqlText
	}
	queryNode, err := sqlparser.ParseQuery(sqlText)
	if err != nil || queryNode == nil {
		return sqlText
	}
	if !stripHintCallsFromSelect(queryNode) {
		return sqlText
	}
	return strings.TrimSpace(sqlparser.Stringify(queryNode))
}

func stripHintCallsFromSelect(node *query.Select) bool {
	if node == nil || len(node.List) == 0 {
		return false
	}
	filtered := make(query.List, 0, len(node.List))
	changed := false
	for _, item := range node.List {
		if item == nil {
			continue
		}
		if isHintProjectionItem(item) {
			changed = true
			continue
		}
		filtered = append(filtered, item)
	}
	// Keep original list if stripping would produce invalid SELECT list.
	if changed && len(filtered) > 0 {
		node.List = filtered
		return true
	}
	return false
}

func isHintProjectionItem(item *query.Item) bool {
	if item == nil || item.Expr == nil {
		return false
	}
	call, ok := item.Expr.(*expr.Call)
	if !ok || call.X == nil {
		return false
	}
	name := normalizeHintCallName(sqlparser.Stringify(call.X))
	return projectionHintCalls[name]
}

func normalizeHintCallName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.Trim(name, "`\"'")
	name = strings.ReplaceAll(name, "_", "")
	return name
}
