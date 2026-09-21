package sql

import (
	"fmt"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"strings"

	"github.com/viant/sqlparser"
	sqltext "github.com/viant/sqlparser/source"
)

type selectProjectionSource struct {
	selectIndex   int
	fromIndex     int
	parts         []string
	selectionKind string
}

// Authored v0 SQL resources can enclose the entire SELECT in parentheses.
// Peel only complete enclosures, never a derived table or a UNION operand.
func unwrapProjectionSQL(text string) string {
	for {
		trimmed := strings.TrimSpace(text)
		group, end, ok := sqltext.ReadGroupString(trimmed, 0, '(', ')')
		if !ok || end != len(trimmed) {
			return text
		}
		text = strings.TrimSpace(group[1 : len(group)-1])
	}
}

func unwrapGroupedProjectionWrapper(sqlText string, selected []string) (string, []string) {
	outer, err := sqlparser.ParseQuery(sqlText)
	if err != nil || outer == nil || outer.Union != nil || len(outer.WithSelects) > 0 || len(outer.Joins) > 0 ||
		len(outer.GroupBy) > 0 || outer.Having != nil || len(outer.OrderBy) > 0 || outer.Qualify != nil ||
		realOuterWindow(outer) {
		return sqlText, selected
	}
	if sqltext.HasTopLevelClause(sqlText, "where") {
		return sqlText, selected
	}
	alias := strings.TrimSpace(outer.From.Alias)
	raw, ok := outer.From.X.(*expr.Raw)
	if alias == "" || !ok {
		return sqlText, selected
	}
	innerSQL := unwrapProjectionSQL(raw.Raw)
	inner, _ := raw.X.(*query.Select)
	if inner == nil {
		inner, err = sqlparser.ParseQuery(innerSQL)
		if err != nil {
			return sqlText, selected
		}
	}
	if inner == nil || !groupedWrapperInner(inner) {
		return sqlText, selected
	}
	outputByOuter := map[string]string{}
	for _, item := range outer.List {
		if item == nil || strings.TrimSpace(item.Alias) != "" {
			return sqlText, selected
		}
		itemName := sqlparser.Stringify(item.Expr)
		parts, err := sqlparser.TableIdentifierParts(itemName)
		if err != nil || len(parts) != 2 || !strings.EqualFold(parts[0], alias) {
			return sqlText, selected
		}
		outputByOuter[canonicalProjectionName(itemName)] = parts[1]
		outputByOuter[canonicalProjectionName(parts[1])] = parts[1]
	}
	return groupedWrapperInnerSQLWithControls(innerSQL, outer), normalizeGroupedWrapperSelection(selected, outputByOuter)
}

func groupedWrapperInner(selectStmt *query.Select) bool {
	if selectStmt == nil {
		return false
	}
	if len(selectStmt.GroupBy) > 0 || selectStmt.Having != nil {
		return true
	}
	for _, item := range selectStmt.List {
		if isAggregateSelectItem(item) {
			return true
		}
	}
	return false
}

func realOuterWindow(selectStmt *query.Select) bool {
	return selectStmt != nil && selectStmt.Window != nil && !strings.EqualFold(strings.TrimSpace(selectStmt.Window.Raw), "LIMIT")
}

func normalizeGroupedWrapperSelection(selected []string, outputByOuter map[string]string) []string {
	if len(selected) == 0 || len(outputByOuter) == 0 {
		return selected
	}
	result := make([]string, 0, len(selected))
	for _, item := range selected {
		if output := outputByOuter[canonicalProjectionName(item)]; output != "" {
			result = append(result, output)
			continue
		}
		result = append(result, item)
	}
	return result
}

func groupedWrapperInnerSQLWithControls(innerSQL string, outer *query.Select) string {
	result := strings.TrimSuffix(strings.TrimSpace(innerSQL), ";")
	if outer == nil {
		return result
	}
	if outer.Limit != nil {
		result += " LIMIT " + sqlparser.Stringify(outer.Limit)
	}
	if outer.Offset != nil {
		result += " OFFSET " + sqlparser.Stringify(outer.Offset)
	}
	return result
}

func newSelectProjectionSource(sqlText string) (selectProjectionSource, bool) {
	lower := strings.ToLower(sqlText)
	selectIndex := sqltext.FindTopLevelKeyword(lower, "select", 0)
	if selectIndex < 0 {
		return selectProjectionSource{}, false
	}
	fromIndex := sqltext.FindTopLevelKeyword(lower, "from", selectIndex+len("select"))
	if fromIndex < 0 {
		return selectProjectionSource{}, false
	}
	parts := sqltext.SplitTopLevelCSV(sqlText[selectIndex+len("select") : fromIndex])
	if len(parts) == 0 {
		return selectProjectionSource{}, false
	}
	selectionKind, firstPart := splitSelectionKind(parts[0])
	if selectionKind != "" {
		parts[0] = firstPart
	}
	return selectProjectionSource{
		selectIndex: selectIndex, fromIndex: fromIndex,
		parts: parts, selectionKind: selectionKind,
	}, true
}

// parse delegates identifiers (including delimited parts) to the native
// identifier parser; other SQL expressions remain owned by ParseQuery.
func (s selectProjectionSource) parse() (*query.Select, error) {
	result := &query.Select{}
	for _, raw := range s.parts {
		core, alias := sqltext.SplitTopLevelAlias(raw)
		if _, err := sqlparser.TableIdentifierParts(core); err == nil && !strings.HasPrefix(strings.TrimSpace(core), "'") {
			result.List = append(result.List, &query.Item{Expr: &expr.Ident{Name: core}, Alias: alias})
			continue
		}
		parsed, err := sqlparser.ParseQuery("SELECT " + raw + " FROM projection_source")
		if err != nil {
			return nil, fmt.Errorf("source projection item %q: %w", raw, err)
		}
		if parsed == nil || len(parsed.List) != 1 {
			return nil, fmt.Errorf("source projection is unresolved")
		}
		result.List = append(result.List, parsed.List[0])
	}
	return result, nil
}

func (s selectProjectionSource) render(sqlText string, parts []string) string {
	prefix := sqlText[:s.selectIndex+len("select")]
	if s.selectionKind != "" {
		prefix += " " + s.selectionKind
	}
	return prefix + " " + strings.Join(parts, ", ") + " " + sqlText[s.fromIndex:]
}

func normalizeSelectProjection(sqlText string) string {
	selectStmt, err := sqlparser.ParseQuery(sqlText)
	if err != nil || selectStmt == nil || len(selectStmt.List) == 0 {
		return sqlText
	}
	source, ok := newSelectProjectionSource(sqlText)
	if !ok || len(source.parts) != len(selectStmt.List) {
		return sqlText
	}
	changed := false
	filtered := make([]string, 0, len(selectStmt.List))
	for i, item := range selectStmt.List {
		normalized, keep, itemChanged := normalizeProjectionItem(item, source.parts[i])
		if !keep {
			changed = true
			continue
		}
		changed = changed || itemChanged
		filtered = append(filtered, normalized)
	}
	if len(filtered) == 0 {
		return sqlText
	}
	if !changed {
		return sqlText
	}
	return source.render(sqlText, filtered)
}

func splitSelectionKind(part string) (string, string) {
	trimmed := strings.TrimSpace(part)
	if trimmed == "" {
		return "", part
	}
	for _, candidate := range []string{"DISTINCT", "ALL"} {
		if len(trimmed) <= len(candidate) || !strings.EqualFold(trimmed[:len(candidate)], candidate) {
			continue
		}
		if next := trimmed[len(candidate)]; next != ' ' && next != '\t' && next != '\n' && next != '\r' {
			continue
		}
		rest := strings.TrimSpace(trimmed[len(candidate):])
		return trimmed[:len(candidate)], rest
	}
	return "", part
}
