package sql

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	sqltext "github.com/viant/sqlparser/source"
)

func ApplySelectorProjection(sqlText string, selected []string, view *data.View) (string, error) {
	projected, err := (SelectorProjection{SQL: sqlText, View: view}).Prepare(selected)
	if err != nil {
		return "", err
	}
	return projected.Render(projected.Source), nil
}

// Prepare narrows authored outputs before binding while deferring wrappers
// that hide the source namespace until source predicates have been assembled.
func (p SelectorProjection) Prepare(selected []string) (*Projection, error) {
	p.SQL = unwrapProjectionSQL(p.SQL)
	sqlText, view := p.SQL, p.View
	selected = normalizeProjectionSelection(selected)
	if len(selected) > 0 {
		return p.prepare(selected)
	}
	if _, _, err := (SelectorProjection{SQL: sqlText, View: view}).columns(); err != nil {
		var duplicate *duplicateProjectionError
		if view != nil || errors.As(err, &duplicate) {
			return nil, err
		}
	}
	if projected, handled, err := prepareStarProjection(sqlText, selected, view); handled || err != nil {
		return projected, err
	}
	source, err := applyNullProjection(sqlText, view)
	return &Projection{Source: source}, err
}

func (p SelectorProjection) prepare(selected []string) (*Projection, error) {
	if p.View != nil && p.View.IsGroupable() {
		p.SQL = unwrapGroupedProjectionWrapper(p.SQL)
	}
	columns, pureStar, err := p.columns()
	if err != nil {
		return nil, err
	}
	chosen, err := p.selectColumns(columns, selected, pureStar)
	if err != nil {
		return nil, err
	}
	if pureStar {
		return p.prepareSelectedStar(columns, chosen)
	}
	sqlText := p.SQL
	expanded := make([]string, 0, len(columns))
	hasStar := false
	for _, column := range columns {
		expanded = append(expanded, column.source)
		hasStar = hasStar || column.wildcard
	}
	if hasStar {
		parts, ok := newSelectProjectionSource(sqlText)
		if !ok {
			return nil, fmt.Errorf("source projection is unresolved")
		}
		sqlText = parts.render(sqlText, expanded)
	}
	selected = make([]string, 0, len(chosen))
	for _, column := range chosen {
		selected = append(selected, column.output)
	}
	if needsOuterDependentProjection(sqlText, len(chosen), len(columns), p.View != nil && p.View.IsGroupable()) {
		return p.prepareOuterDependentProjection(sqlText, columns, chosen)
	}

	projected, err := applyFilteredSelectorProjection(sqlText, selected, p.View != nil && p.View.IsGroupable())
	if err != nil {
		return nil, err
	}
	source, err := applyNullProjection(projected, p.View)
	return &Projection{Source: source}, err
}

func applyFilteredSelectorProjection(sqlText string, selected []string, groupable bool) (string, error) {
	source, ok := newSelectProjectionSource(sqlText)
	if !ok {
		return "", fmt.Errorf("source projection is unresolved")
	}
	selectStmt, err := sqlparser.ParseQuery(sqlText)
	if err != nil || selectStmt == nil || len(selectStmt.List) != len(source.parts) {
		// An explicit projection is independently parseable while criteria/macro
		// tokens in the suffix are still awaiting the builder's binding phase.
		// Grouped and set rewrites require the complete statement AST.
		if sqltext.HasTopLevelClause(sqlText, "group by") || sqltext.HasTopLevelClause(sqlText, "union") || sqltext.HasTopLevelClause(sqlText, "having") {
			return "", fmt.Errorf("source projection rewrite is unresolved")
		}
		selectStmt, err = source.parse()
	}
	if err != nil || selectStmt == nil || len(source.parts) != len(selectStmt.List) {
		return "", fmt.Errorf("source projection is unresolved")
	}

	matched := map[string]bool{}
	matchedCanonical := map[string]bool{}
	filtered := make([]string, 0, len(source.parts))
	filteredItems := make(query.List, 0, len(source.parts))
	for i, item := range selectStmt.List {
		part := strings.TrimSpace(source.parts[i])
		selectedItem, matchedNames := projectionItemSelected(item, part, selected)
		for _, name := range matchedNames {
			matched[name] = true
			matchedCanonical[canonicalProjectionName(name)] = true
		}
		if !selectedItem {
			continue
		}
		filtered = append(filtered, part)
		filteredItems = append(filteredItems, item)
	}
	for _, selectedName := range selected {
		if !matched[selectedName] && !matchedCanonical[canonicalProjectionName(selectedName)] {
			return "", fmt.Errorf("not found column %s", selectedName)
		}
	}
	if (selectStmt.Union != nil || sqltext.HasTopLevelClause(sqlText, "union")) && len(filtered) < len(source.parts) {
		return applySetProjection(sqlText, filteredItems, filtered)
	}
	if groupable && (len(selectStmt.GroupBy) > 0 || groupedProjectionNeedsRewrite(filteredItems)) {
		rewritten := rewriteGroupedProjectionSQL(selectStmt, filteredItems)
		if strings.TrimSpace(rewritten) != "" {
			return rewritten, nil
		}
	}
	if len(filtered) == 0 || len(filtered) == len(source.parts) {
		return sqlText, nil
	}
	return source.render(sqlText, filtered), nil
}

func needsOuterDependentProjection(sqlText string, selectedCount, totalCount int, groupable bool) bool {
	return !groupable && selectedCount > 0 && selectedCount < totalCount &&
		(sqltext.HasTopLevelClause(sqlText, "group by") || sqltext.HasTopLevelClause(sqlText, "having"))
}

func (p SelectorProjection) prepareOuterDependentProjection(sqlText string, columns, chosen []ProjectionColumn) (*Projection, error) {
	allowNulls := p.View != nil && p.View.NullsAllowed()
	projection := make([]string, 0, len(chosen))
	for _, column := range chosen {
		output := strings.TrimSpace(column.output)
		if output == "" {
			return nil, fmt.Errorf("source projection is unresolved")
		}
		resolved := column.outputColumn(p.View)
		if resolved == nil {
			resolved = &data.Column{}
		}
		outer := *resolved
		outer.Name = output
		outer.Column = output
		outer.Expression = ""
		expression := strings.TrimSpace(outer.SelectExpression(allowNulls))
		if expression == "" {
			return nil, fmt.Errorf("source projection is unresolved")
		}
		projection = append(projection, expression)
	}
	return &Projection{Source: strings.TrimSuffix(strings.TrimSpace(sqlText), ";"), outer: projection, columns: chosen, orderColumns: columns}, nil
}

func normalizeProjectionSelection(input []string) []string {
	if len(input) == 0 {
		return nil
	}
	seen := map[string]bool{}
	var result []string
	for _, item := range input {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		key := strings.ToLower(item)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, key)
	}
	return result
}

func projectionItemSelected(item *query.Item, original string, selected []string) (bool, []string) {
	var matched []string
	for _, candidate := range projectionItemNames(item, original) {
		key := canonicalProjectionName(candidate)
		for _, selectedName := range selected {
			if key == canonicalProjectionName(selectedName) {
				matched = append(matched, selectedName)
			}
		}
	}
	return len(matched) > 0, matched
}

func projectionItemNames(item *query.Item, original string) []string {
	core, alias := sqltext.SplitTopLevelAlias(strings.TrimSpace(original))
	if alias != "" {
		return []string{alias}
	}
	if item != nil && item.Alias != "" {
		return []string{item.Alias}
	}
	if core == "" {
		return nil
	}
	if item != nil {
		if literal, ok := item.Expr.(*expr.Literal); ok && literal.Kind == "string" && strings.HasPrefix(strings.TrimSpace(core), "'") {
			return []string{`"` + strings.ReplaceAll(core, `"`, `""`) + `"`}
		}
	}
	if name, err := starOutputColumn(&data.Column{Column: core}); err == nil {
		return []string{name}
	}
	// An unaliased expression's complete SQL label remains intact. It does
	// not acquire source-field or punctuation-stripped alternatives.
	return []string{core}
}

func canonicalProjectionName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	parts, err := sqlparser.TableIdentifierParts(value)
	if err != nil {
		// A computed output label is one name, not a qualified identifier. Keep
		// its complete spelling and use the same key as a quoted reference to it.
		parts = []string{value}
	}

	for i := range parts {
		parts[i] = strings.ToLower(parts[i])
	}
	encoded, _ := json.Marshal(parts)
	return "identifier:" + string(encoded)
}
