package sql

import (
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
)

// LiteralKinds supplies optional defaults for aliased string/integer literals.
// Result labels and ordinals are database evidence. Parsing failure, CTEs, set
// sources, and unresolved mappings simply supply no defaults; they never gate
// an explicit application type. No table schemas or row values are consulted.
func (p SelectorProjection) LiteralKinds(labels []string) map[int]string {
	stmt, err := sqlparser.ParseQuery(p.SQL)
	if err != nil {
		return nil
	}
	for depth := 0; depth < 32 && stmt != nil; depth++ {
		if len(stmt.WithSelects) > 0 || stmt.Union != nil {
			return nil
		}
		if len(stmt.List) != 1 || projectionStar(stmt.List[0]) == nil {
			break
		}
		star := projectionStar(stmt.List[0])
		if len(star.Except) > 0 || len(stmt.Joins) > 0 {
			return nil
		}
		src := wildcardSource{alias: stmt.From.Alias, node: stmt.From.X}
		qualifier := ""
		switch x := star.X.(type) {
		case *expr.Selector:
			qualifier = x.Name
		case *expr.Ident:
			qualifier = x.Name
		}
		if qualifier != "" && qualifier != "*" {
			parts, e := sqlparser.TableIdentifierParts(qualifier)
			if e != nil || !src.matchesQualifier(parts) {
				return nil
			}
		}
		nested, _ := src.query(nil)
		if nested == nil {
			return nil
		}
		stmt = nested
	}
	if stmt == nil || stmt.Union != nil || len(stmt.WithSelects) > 0 {
		return nil
	}
	firstStar, lastStar := -1, -1
	for i, item := range stmt.List {
		if projectionStar(item) != nil {
			if firstStar < 0 {
				firstStar = i
			}
			lastStar = i
		}
	}
	if firstStar < 0 && len(stmt.List) != len(labels) {
		return nil
	}
	result := map[int]string{}
	for i, item := range stmt.List {
		if item == nil || item.Alias == "" {
			continue
		}
		value, ok := item.Expr.(*expr.Literal)
		if !ok || (value.Kind != "string" && value.Kind != "int") {
			continue
		}
		parts, e := sqlparser.TableIdentifierParts(item.Alias)
		if e != nil || len(parts) != 1 {
			continue
		}
		name := parts[0]
		index := i
		if firstStar >= 0 && i > firstStar {
			if i < lastStar {
				continue
			}
			index = len(labels) - (len(stmt.List) - i)
		}
		if index < 0 || index >= len(labels) || !strings.EqualFold(labels[index], name) {
			continue
		}
		matches := 0
		for _, label := range labels {
			if strings.EqualFold(label, name) {
				matches++
			}
		}
		if matches != 1 {
			continue
		}
		result[index] = value.Kind
	}
	return result
}
