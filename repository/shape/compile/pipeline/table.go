package pipeline

import "strings"

// InferTableFromSQL infers root table from SQL text using parser-first strategy.
func InferTableFromSQL(sqlText string) string {
	sqlText = strings.TrimSpace(sqlText)
	if sqlText == "" {
		return ""
	}
	normalized := normalizeParserSQL(sqlText)
	queryNode, _, err := ParseSelectWithDiagnostic(normalized)
	if err != nil || queryNode == nil {
		return ""
	}
	_, table, err := InferRoot(queryNode, "")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(strings.Trim(table, "`\""))
}
