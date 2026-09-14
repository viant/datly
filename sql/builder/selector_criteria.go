package builder

import (
	"strings"

	sqltext "github.com/viant/sqlparser/source"
	"github.com/viant/sqlx"
	xstate "github.com/viant/xdatly/state"
)

const (
	selectorCriteriaToken      = "$SELECTOR_CRITERIA"
	whereSelectorCriteriaToken = "$WHERE_SELECTOR_CRITERIA"
	andSelectorCriteriaToken   = "$AND_SELECTOR_CRITERIA"
)

// prepareSelectorCriteriaSQL exposes clause syntax before projection parsing,
// retaining the generic slot for the existing binder to interleave its args.
func prepareSelectorCriteriaSQL(sqlText string, selector *xstate.Selector) string {
	if selector == nil || strings.TrimSpace(selector.Criteria) == "" {
		for _, token := range []string{whereSelectorCriteriaToken, andSelectorCriteriaToken, selectorCriteriaToken} {
			sqlText = sqltext.Token(token).ReplaceAll(sqlText, "")
		}
		return cleanupDanglingWhere(sqlText)
	}
	for _, token := range []string{whereSelectorCriteriaToken, andSelectorCriteriaToken} {
		sqlText = sqltext.Token(token).ReplaceAll(sqlText, selectorCriteriaReplacement(token, selectorCriteriaToken))
	}
	return sqlText
}

func bindSelectorCriteriaSQL(sqlText string, resolver sqlx.ParameterResolver, selector *xstate.Selector, positionalArgs []any) (string, []any, error) {
	criteria := ""
	criteriaArgs := []any(nil)
	if selector != nil {
		criteria = strings.TrimSpace(selector.Criteria)
		criteriaArgs = append(criteriaArgs, selector.Placeholders...)
	}
	binder := sqlx.NewParameterBinder(resolver, positionalArgs...)
	if !containsSelectorCriteriaToken(sqlText) {
		boundSQL, args, err := binder.Bind(sqlText)
		if err != nil {
			return "", nil, err
		}
		if err = binder.Complete(); err != nil {
			return "", nil, err
		}
		return boundSQL, args, nil
	}
	var (
		builder   strings.Builder
		args      []any
		remaining = sqlText
	)
	for {
		token, idx := nextSelectorCriteriaToken(remaining)
		if idx == -1 {
			break
		}
		prefix := remaining[:idx]
		boundPrefix, prefixArgs, err := binder.Bind(prefix)
		if err != nil {
			return "", nil, err
		}
		builder.WriteString(boundPrefix)
		args = append(args, prefixArgs...)
		builder.WriteString(selectorCriteriaReplacement(token, criteria))
		args = append(args, criteriaArgs...)
		remaining = remaining[idx+len(token):]
	}
	boundSuffix, suffixArgs, err := binder.Bind(remaining)
	if err != nil {
		return "", nil, err
	}
	if err = binder.Complete(); err != nil {
		return "", nil, err
	}
	builder.WriteString(boundSuffix)
	args = append(args, suffixArgs...)
	return builder.String(), args, nil
}

func containsSelectorCriteriaToken(sqlText string) bool {
	return sqltext.Token(selectorCriteriaToken).Contains(sqlText) ||
		sqltext.Token(whereSelectorCriteriaToken).Contains(sqlText) ||
		sqltext.Token(andSelectorCriteriaToken).Contains(sqlText)
}

func nextSelectorCriteriaToken(sqlText string) (string, int) {
	bestIdx := -1
	bestToken := ""
	for _, token := range []string{whereSelectorCriteriaToken, andSelectorCriteriaToken, selectorCriteriaToken} {
		idx := sqltext.Token(token).Find(sqlText, 0)
		if idx == -1 {
			continue
		}
		if bestIdx == -1 || idx < bestIdx {
			bestIdx = idx
			bestToken = token
		}
	}
	return bestToken, bestIdx
}

func selectorCriteriaReplacement(token, criteria string) string {
	if criteria == "" {
		return ""
	}
	switch token {
	case whereSelectorCriteriaToken:
		return " WHERE " + criteria
	case andSelectorCriteriaToken:
		return " AND " + criteria
	default:
		return criteria
	}
}

func appendAutoSelectorCriteria(sqlText string, args []any, selector *xstate.Selector, hadExplicitToken bool) (string, []any) {
	if selector == nil || hadExplicitToken {
		return sqlText, args
	}
	criteria := strings.TrimSpace(selector.Criteria)
	if criteria == "" {
		return sqlText, args
	}
	clause := " WHERE " + criteria
	if sqltext.HasTopLevelClause(sqlText, "where") {
		clause = " AND " + criteria
	}
	insertAt := sqltext.CriteriaBoundary(sqlText)
	argAt := sqlx.ParseParameters(sqlText[:insertAt]).Count()
	sqlText = insertSelectorClause(sqlText, clause)
	if len(selector.Placeholders) == 0 {
		return sqlText, args
	}
	result := make([]any, 0, len(args)+len(selector.Placeholders))
	result = append(result, args[:argAt]...)
	result = append(result, selector.Placeholders...)
	result = append(result, args[argAt:]...)
	return sqlText, result
}

func insertSelectorClause(sqlText string, clause string) string {
	insertAt := sqltext.CriteriaBoundary(sqlText)
	if insertAt == len(sqlText) {
		return sqlText + clause
	}
	return strings.TrimRight(sqlText[:insertAt], " \t\r\n") + clause + " " + strings.TrimLeft(sqlText[insertAt:], " \t\r\n")
}
