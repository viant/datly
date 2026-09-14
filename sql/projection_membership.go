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

// SelectorProjection owns selector membership in the authored SQL projection.
// Prepared columns resolve physical wildcard outputs; explicit SQL always wins.
type SelectorProjection struct {
	SQL  string
	View *data.View
}

type ProjectionColumn struct {
	names      ProjectionNames
	order      string
	source     string
	expression string
	output     string
	metadata   *data.Column
	wildcard   bool
	pureStar   bool
}

// ProjectionNames contains declared output names and explicit configured aliases.
// Case-insensitive identifier comparison is shared by fields and order policy.
// Identifier parts and punctuation remain distinct; alternate spellings need
// authored SQL aliases or canonical view mappings.
type ProjectionNames []string

func (names ProjectionNames) Matches(name string) bool {
	key := canonicalProjectionName(name)
	if key == "" {
		return false
	}
	for _, candidate := range names {
		if canonicalProjectionName(candidate) == key {
			return true
		}
	}
	return false
}

// OutputName is the declared SQL result name, before configured selector aliases.
func (c ProjectionColumn) OutputName() string { return c.output }

func (c ProjectionColumn) Matches(name string) bool { return c.names.Matches(name) }

func (c ProjectionColumn) MatchesOutput(name string) bool {
	return (ProjectionNames{c.output}).Matches(name)
}

func (c ProjectionColumn) OrderExpression() string { return c.order }

// SourceExpression retains SQL scope when a projected alias is selected away.
func (c ProjectionColumn) SourceExpression() string {
	if c.wildcard {
		if c.pureStar {
			return c.output
		}
		return c.order
	}
	return c.expression
}

func (p SelectorProjection) Columns(selected []string) ([]ProjectionColumn, error) {
	columns, wildcard, err := p.columns()
	if err != nil {
		return nil, err
	}
	return p.selectColumns(columns, selected, wildcard)
}

func (p SelectorProjection) selectColumns(columns []ProjectionColumn, selected []string, wildcard bool) ([]ProjectionColumn, error) {
	selected = normalizeProjectionSelection(selected)
	if len(selected) == 0 {
		return columns, nil
	}
	indexes := make(map[int]bool)
	var result []ProjectionColumn
	for _, name := range selected {
		match := -1
		for i, col := range columns {
			if !col.Matches(name) {
				continue
			}
			if match != -1 {
				return nil, fmt.Errorf("ambiguous source projection field %q", name)
			}
			match = i
		}
		if match == -1 {
			return nil, fmt.Errorf("not found column %s", name)
		}
		if !indexes[match] && wildcard {
			column := columns[match]
			column.order = column.output
			result = append(result, column)
		}
		indexes[match] = true
	}
	if !wildcard {
		for i, col := range columns {
			if indexes[i] {
				result = append(result, col)
			}
		}
	}
	return result, nil
}

func (p SelectorProjection) columns() ([]ProjectionColumn, bool, error) {
	if err := sqltext.ValidateStructure(p.SQL); err != nil {
		return nil, false, fmt.Errorf("source projection is unresolved: %w", err)
	}
	parts, ok := newSelectProjectionSource(p.SQL)
	source := p.SQL
	if ok {
		source = "SELECT " + strings.Join(parts.parts, ",") + " FROM projection_source"
	}
	parsed, err := sqlparser.ParseQuery(source)
	if ok {
		parsed, err = parts.parse()
	}
	if err != nil || parsed == nil || len(parsed.List) == 0 {
		return nil, false, fmt.Errorf("source projection is unresolved")
	}
	var columns []ProjectionColumn
	for i, item := range parsed.List {
		if item == nil || item.Expr == nil {
			return nil, false, fmt.Errorf("source projection is unresolved")
		}
		if projectionStar(item) != nil {
			full, err := sqlparser.ParseQuery(strings.TrimSuffix(strings.TrimSpace(p.SQL), ";"))
			if err != nil || full == nil {
				return nil, false, fmt.Errorf("wildcard source projection is unresolved")
			}
			resolved, err := p.wildcardColumns(full, item, 0)
			if err != nil {
				return nil, false, err
			}
			columns = append(columns, resolved...)
			continue
		}
		raw := sqlparser.Stringify(item)
		if ok && i < len(parts.parts) {
			raw = strings.TrimSpace(parts.parts[i])
		}
		_, order := sqltext.SplitTopLevelAlias(raw)
		if order == "" {
			order = item.Alias
		}
		if order == "" {
			order = sqlparser.Stringify(item.Expr)
		}
		core, _ := sqltext.SplitTopLevelAlias(raw)
		if literal, ok := item.Expr.(*expr.Literal); ok && (literal.Kind == "int" || literal.Kind == "float") {
			// Bare numeric expressions in ORDER BY denote ordinals, even if a
			// literal's alias was selected away. Keep it a numeric expression.
			core = "(" + core + " + 0)"
		}
		names := projectionItemNames(item, raw)
		if len(names) == 0 {
			return nil, false, fmt.Errorf("source projection has no output name")
		}
		columns = append(columns, ProjectionColumn{names: names, output: names[0], order: order, source: raw, expression: core})
	}
	seen := make(map[string]bool)
	for _, column := range columns {
		key := canonicalProjectionName(column.output)
		if seen[key] {
			return nil, false, &duplicateProjectionError{name: column.output}
		}
		seen[key] = true
	}
	p.applyMappings(columns)
	pureStar := len(parsed.List) == 1 && projectionStar(parsed.List[0]) != nil
	if pureStar {
		for i := range columns {
			columns[i].pureStar = true
		}
	}
	return columns, pureStar, nil
}

type duplicateProjectionError struct{ name string }

func (e *duplicateProjectionError) Error() string {
	return fmt.Sprintf("duplicate output column %q in source projection; assign distinct SQL aliases", e.name)
}

// applyMappings admits only canonical view mappings with a unique source target.
// Prepared runtime Go field names alone are not mapping authority. Resolve all
// targets before adding aliases so mapping order cannot create transitive access.
func (p SelectorProjection) applyMappings(columns []ProjectionColumn) {
	if p.View == nil {
		return
	}
	aliases := make([][]string, len(columns))
	add := func(name, source string) {
		if name == "" || source == "" {
			return
		}
		found := -1
		for i, column := range columns {
			if !column.Matches(source) {
				continue
			}
			if found >= 0 {
				found = -2
				break
			}
			found = i
		}
		if found >= 0 {
			aliases[found] = append(aliases[found], name)
		}
	}
	for _, mapping := range p.View.Spec.Columns {
		if mapping != nil && !mapping.NameInferred {
			add(mapping.Name, mapping.Source)
		}
	}
	for i := range columns {
		columns[i].names = append(columns[i].names, aliases[i]...)
	}
}

// TableColumn keeps physical output names unless the canonical view explicitly
// declares an alias. Inferred Go field names are mapping destinations, not SQL
// aliases. Table construction and native matchers consume the same result.
func (p SelectorProjection) TableColumn(column *data.Column) (*data.Column, error) {
	if column == nil {
		return nil, nil
	}
	result := *column
	if result.Column == "" {
		result.Column = result.Name
	}
	output, err := starOutputColumn(&result)
	if err != nil {
		return nil, err
	}
	result.Name = output
	if p.View != nil {
		for _, mapping := range p.View.Spec.Columns {
			if mapping != nil && !mapping.NameInferred && mapping.Name != "" && mapping.Source != "" && (ProjectionNames{mapping.Source}).Matches(result.Column) && (ProjectionNames{mapping.Name}).Matches(column.Name) {
				result.Name = mapping.Name
			}
		}
	}
	return &result, nil
}

// Expand materializes resolved wildcard columns in projection order. The builder
// uses this for ordering an unnarrowed wildcard, so ordinal policy and executed
// SQL cannot disagree when prepared column order differs from table order.
func (p SelectorProjection) Expand() (string, error) {
	columns, _, err := p.columns()
	if err != nil {
		return "", err
	}
	parts := make([]string, 0, len(columns))
	wildcard := false
	for _, column := range columns {
		parts = append(parts, column.source)
		wildcard = wildcard || column.wildcard
	}
	if !wildcard {
		return p.SQL, nil
	}
	source, ok := newSelectProjectionSource(p.SQL)
	if !ok {
		return "", fmt.Errorf("source projection is unresolved")
	}
	return source.render(p.SQL, parts), nil
}

func projectionStar(item *query.Item) *expr.Star {
	if item == nil {
		return nil
	}
	switch value := item.Expr.(type) {
	case *expr.Star:
		return value
	case *expr.Selector:
		if star, ok := value.X.(*expr.Star); ok {
			return star
		}
	}
	return nil
}
