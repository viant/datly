package sql

import (
	"fmt"
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	sqltext "github.com/viant/sqlparser/source"
)

func applyStarProjection(sqlText string, selected []string, view *data.View) (string, bool, error) {
	if view == nil || len(view.Columns) == 0 {
		return sqlText, false, nil
	}
	source := strings.TrimSuffix(strings.TrimSpace(sqlText), ";")
	selectStmt, err := sqlparser.ParseQuery(source)
	if err != nil || selectStmt == nil || !selectStmt.List.IsStarExpr() {
		return sqlText, false, nil
	}
	allowNulls := view.NullsAllowed()
	columns, err := projectionMetadataColumns(selectStmt.List[0], view)
	if err != nil {
		return "", true, err
	}
	if len(selected) == 0 {
		needsProjection := false
		for _, column := range columns {
			if column != nil && column.Nullable && column.NullFallback != "" && !allowNulls {
				needsProjection = true
				break
			}
		}
		if !needsProjection {
			return sqlText, false, nil
		}
	} else {
		available := columns
		columns = make([]*data.Column, 0, len(selected))
		for _, name := range selected {
			column := projectionMetadataColumn(name, available)
			if column == nil {
				return "", true, fmt.Errorf("not found column %s", name)
			}
			columns = append(columns, column)
		}
	}
	projection := make([]string, 0, len(columns))
	for _, column := range columns {
		if column == nil {
			continue
		}
		resolved := *column
		resolved.Column, err = starOutputColumn(column)
		if err != nil {
			return "", true, err
		}
		resolved.Name = resolved.Column
		resolved.Expression = ""
		expression := strings.TrimSpace(resolved.SelectExpression(allowNulls))
		if expression != "" {
			projection = append(projection, expression)
		}
	}
	if len(projection) == 0 {
		return sqlText, false, nil
	}
	return "SELECT " + strings.Join(projection, ", ") + " FROM (" + source + ") AS datly_view", true, nil
}

// applySelectedStar retains the existing outer projection and null rendering,
// using only the wildcard outputs resolved by the SQL projection owner.
func (p SelectorProjection) applySelectedStar(columns, chosen []ProjectionColumn) (string, error) {
	metadata := make([]*data.Column, 0, len(columns))
	for _, column := range columns {
		resolved := data.Column{Name: column.output, Column: column.output}
		original := column.metadata
		if original == nil && p.View != nil {
			original = projectionMetadataColumn(column.output, p.View.Columns)
		}
		if original != nil {
			resolved = *original
			resolved.Column = column.output
			resolved.Name = column.output
			resolved.Expression = ""
		}
		metadata = append(metadata, &resolved)
	}
	view := data.View{Columns: metadata}
	if p.View != nil {
		view = *p.View
		view.Columns = metadata
	}
	names := make([]string, 0, len(chosen))
	for _, column := range chosen {
		names = append(names, column.output)
	}
	source := p.SQL
	parsed, err := sqlparser.ParseQuery(source)
	if err != nil {
		return "", err
	}
	if len(parsed.List) == 1 && projectionStar(parsed.List[0]) != nil && len(projectionStar(parsed.List[0]).Except) > 0 {
		// EXCEPT is projection metadata; lower it to concrete outputs before
		// wrapping, including on dialects without wildcard EXCEPT syntax.
		expanded, err := p.Expand()
		if err != nil {
			return "", err
		}
		projection := make([]string, 0, len(chosen))
		for _, name := range names {
			column := projectionMetadataColumn(name, metadata)
			projection = append(projection, column.SelectExpression(view.NullsAllowed()))
		}
		return "SELECT " + strings.Join(projection, ", ") + " FROM (" + strings.TrimSuffix(strings.TrimSpace(expanded), ";") + ") AS datly_view", nil
	}
	projected, handled, err := applyStarProjection(source, names, &view)
	if err != nil {
		return "", err
	}
	if !handled {
		return "", fmt.Errorf("wildcard source projection is unresolved")
	}
	return projected, nil
}

func projectionMetadataColumn(name string, columns []*data.Column) *data.Column {
	name = canonicalProjectionName(name)
	for _, column := range columns {
		if column == nil {
			continue
		}
		if name == canonicalProjectionName(column.Name) || name == canonicalProjectionName(column.Column) {
			return column
		}
	}
	return nil
}

func starOutputColumn(column *data.Column) (string, error) {
	name := strings.TrimSpace(column.Column)
	if name == "" {
		name = strings.TrimSpace(column.Name)
	}
	if _, err := sqlparser.TableIdentifierParts(name); err != nil {
		return "", fmt.Errorf("column %q has no selectable wildcard output name: %w", column.Name, err)
	}
	scanner := sqltext.NewCodeScanner(name, 0)
	last := -1
	for at, ok := scanner.Next(); ok; at, ok = scanner.Next() {
		if name[at] == '.' {
			last = at
		}
	}
	return strings.TrimSpace(name[last+1:]), nil
}

func applyNullProjection(sqlText string, view *data.View) (string, error) {
	if view == nil || len(view.Columns) == 0 || view.NullsAllowed() {
		return sqlText, nil
	}
	selectStmt, err := sqlparser.ParseQuery(sqlText)
	if err != nil || selectStmt == nil || len(selectStmt.List) == 0 {
		return sqlText, nil
	}
	source, ok := newSelectProjectionSource(sqlText)
	if !ok || len(source.parts) != len(selectStmt.List) {
		return sqlText, nil
	}
	if selectStmt.Union != nil || sqltext.HasTopLevelClause(sqlText, "union") {
		if !hasNullableProjection(selectStmt.List, source.parts, view.Columns) {
			return sqlText, nil
		}
		wrapped, err := applySetProjection(sqlText, selectStmt.List, source.parts)
		if err != nil || wrapped == sqlText {
			return wrapped, err
		}
		return applyNullProjection(wrapped, view)
	}
	changed := false
	for index, item := range selectStmt.List {
		column := projectionColumn(item, source.parts[index], view.Columns)
		if column == nil || !column.Nullable || strings.TrimSpace(column.NullFallback) == "" || isCoalescedProjection(item) {
			continue
		}
		source.parts[index] = coalescedProjection(source.parts[index], item, column)
		changed = true
	}
	if !changed {
		return sqlText, nil
	}
	return source.render(sqlText, source.parts), nil
}

func isCoalescedProjection(item *query.Item) bool {
	if item == nil {
		return false
	}
	call, ok := item.Expr.(*expr.Call)
	return ok && call.X != nil && strings.EqualFold(strings.TrimSpace(sqlparser.Stringify(call.X)), "COALESCE")
}

func hasNullableProjection(items query.List, parts []string, columns []*data.Column) bool {
	for index, item := range items {
		column := projectionColumn(item, parts[index], columns)
		if column != nil && column.Nullable && strings.TrimSpace(column.NullFallback) != "" {
			return true
		}
	}
	return false
}

func projectionColumn(item *query.Item, source string, columns []*data.Column) *data.Column {
	for _, name := range projectionItemNames(item, source) {
		if column := projectionMetadataColumn(name, columns); column != nil {
			return column
		}
	}
	return nil
}

func coalescedProjection(source string, item *query.Item, column *data.Column) string {
	core, alias := sqltext.SplitTopLevelAlias(strings.TrimSpace(source))
	if alias == "" && item != nil {
		alias = strings.TrimSpace(item.Alias)
	}
	if alias == "" {
		alias, _ = starOutputColumn(&data.Column{Column: core})
		if alias == "" {
			alias = strings.TrimSpace(column.Name)
		}
	}
	result := "COALESCE(" + strings.TrimSpace(core) + ", " + strings.TrimSpace(column.NullFallback) + ")"
	if alias != "" {
		result += " AS " + alias
	}
	return result
}
