package sql

import (
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	sqltext "github.com/viant/sqlparser/source"
)

func PrepareExecutableSQL(sqlText string, controls *spec.ViewControls) string {
	normalized := NormalizeAuthoredSQL(sqlText)
	hadPaginationToken := paginationToken.Contains(normalized)
	normalized = replacePaginationToken(normalized, controls)
	return applyViewControls(normalized, controls, hadPaginationToken)
}

var paginationToken = sqltext.Token("$PAGINATION")

func applyViewControls(sqlText string, controls *spec.ViewControls, skipPagination bool) string {
	if strings.TrimSpace(sqlText) == "" || controls == nil {
		return strings.TrimSpace(sqlText)
	}
	applied := strings.TrimSpace(sqlText)
	if !sqltext.HasTopLevelClause(applied, "order by") {
		applied = appendOrderBy(applied, controls.OrderBy)
	}
	if !skipPagination && !sqltext.HasTopLevelClause(applied, "limit") {
		applied = appendLimit(applied, controls.Limit)
	}
	if !skipPagination && !sqltext.HasTopLevelClause(applied, "offset") {
		applied = appendOffset(applied, controls.Offset)
	}
	return applied
}

func replacePaginationToken(sqlText string, controls *spec.ViewControls) string {
	if !paginationToken.Contains(sqlText) {
		return sqlText
	}
	replacement := ""
	if controls != nil {
		if controls.Limit != nil {
			replacement += " LIMIT " + strconv.Itoa(*controls.Limit)
		}
		if controls.Offset != nil {
			replacement += " OFFSET " + strconv.Itoa(*controls.Offset)
		}
	}
	return paginationToken.ReplaceAll(sqlText, replacement)
}

func appendOrderBy(sqlText string, orderBy string) string {
	orderBy = strings.TrimSpace(orderBy)
	if orderBy == "" {
		return sqlText
	}
	return insertClause(sqlText, " ORDER BY "+orderBy, "limit", "offset")
}

func appendLimit(sqlText string, limit *int) string {
	if limit == nil {
		return sqlText
	}
	return insertClause(sqlText, " LIMIT "+strconv.Itoa(*limit), "offset")
}

func appendOffset(sqlText string, offset *int) string {
	if offset == nil {
		return sqlText
	}
	return sqlText + " OFFSET " + strconv.Itoa(*offset)
}

func insertClause(sqlText string, clause string, beforeClauses ...string) string {
	if len(beforeClauses) == 0 {
		return sqlText + clause
	}
	lower := strings.ToLower(sqlText)
	insertAt := -1
	for _, candidate := range beforeClauses {
		index := sqltext.FindTopLevelKeyword(lower, candidate, 0)
		if index == -1 {
			continue
		}
		if insertAt == -1 || index < insertAt {
			insertAt = index
		}
	}
	if insertAt == -1 {
		return sqlText + clause
	}
	return strings.TrimRight(sqlText[:insertAt], " \t\r\n") + clause + " " + strings.TrimLeft(sqlText[insertAt:], " \t\r\n")
}
