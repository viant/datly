package builder

import sqltext "github.com/viant/sqlparser/source"

const (
	whereCriteriaToken = "$WHERE_CRITERIA"
	andCriteriaToken   = "$AND_CRITERIA"
	orCriteriaToken    = "$OR_CRITERIA"
)

func (f relationFilter) applyCriteriaTokens(sqlText string) string {
	if !containsRelationCriteriaToken(sqlText) {
		return sqlText
	}
	criteria, hasCriteria := f.expression()
	if !hasCriteria {
		sqlText = sqltext.Token(whereCriteriaToken).ReplaceAll(sqlText, "")
		sqlText = sqltext.Token(andCriteriaToken).ReplaceAll(sqlText, "")
		sqlText = sqltext.Token(orCriteriaToken).ReplaceAll(sqlText, "")
		return normalizeRelationTokenSpacing(sqlText)
	}
	sqlText = sqltext.Token(whereCriteriaToken).ReplaceAll(sqlText, " WHERE "+criteria)
	sqlText = sqltext.Token(andCriteriaToken).ReplaceAll(sqlText, " AND ("+criteria+")")
	sqlText = sqltext.Token(orCriteriaToken).ReplaceAll(sqlText, " OR ("+criteria+")")
	return normalizeRelationTokenSpacing(sqlText)
}

func containsRelationCriteriaToken(sqlText string) bool {
	return sqltext.Token(whereCriteriaToken).Contains(sqlText) ||
		sqltext.Token(andCriteriaToken).Contains(sqlText) ||
		sqltext.Token(orCriteriaToken).Contains(sqlText)
}

func normalizeRelationTokenSpacing(sqlText string) string {
	for _, fragment := range []string{"WHERE", "AND", "OR", "ORDER BY", "GROUP BY", "HAVING", "LIMIT", "OFFSET"} {
		sqlText = sqltext.Token("  "+fragment).ReplaceAll(sqlText, " "+fragment)
	}
	return sqlText
}
