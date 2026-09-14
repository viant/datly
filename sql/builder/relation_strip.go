package builder

import (
	"strings"

	sqltext "github.com/viant/sqlparser/source"
)

func stripRelationFilterTokens(sqlText string) string {
	sqlText = stripColumnInTokens(sqlText)
	sqlText = stripRelationCriteriaTokens(sqlText)
	return normalizeRelationTokenSpacing(cleanupDanglingWhere(sqlText))
}

func stripRelationCriteriaTokens(sqlText string) string {
	sqlText = sqltext.Token(whereCriteriaToken).ReplaceAll(sqlText, "")
	sqlText = sqltext.Token(andCriteriaToken).ReplaceAll(sqlText, "")
	sqlText = sqltext.Token(orCriteriaToken).ReplaceAll(sqlText, "")
	return cleanupDanglingWhere(sqlText)
}

func cleanupDanglingWhere(sqlText string) string {
	lower := strings.ToLower(sqlText)
	whereIndex := sqltext.FindTopLevelKeyword(lower, "where", 0)
	if whereIndex == -1 {
		return sqlText
	}
	afterWhere := whereIndex + len("where")
	insertAt := len(sqlText)
	for _, keyword := range []string{"group by", "having", "order by", "limit", "offset"} {
		if idx := sqltext.FindTopLevelKeyword(lower, keyword, afterWhere); idx != -1 && idx < insertAt {
			insertAt = idx
		}
	}
	if strings.TrimSpace(sqlText[afterWhere:insertAt]) != "" {
		return sqlText
	}
	result := strings.TrimRight(sqlText[:whereIndex], " \t\r\n")
	if insertAt < len(sqlText) {
		if result != "" {
			result += " "
		}
		result += strings.TrimLeft(sqlText[insertAt:], " \t\r\n")
	}
	return result
}
