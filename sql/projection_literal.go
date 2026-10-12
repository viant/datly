package sql

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlx"
)

// LiteralKinds supplies optional defaults for aliased string/integer literals.
// Result labels and ordinals are database evidence. Parsing failure, CTEs, set
// sources, and unresolved mappings simply supply no defaults; they never gate
// an explicit application type. No table schemas or row values are consulted.
func (p SelectorProjection) LiteralKinds(labels []string) map[int]string {
	return projectionValueHints(p.SQL, labels, func(value node.Node) string {
		literal, ok := value.(*expr.Literal)
		if ok && (literal.Kind == "string" || literal.Kind == "int") {
			return literal.Kind
		}
		return ""
	})
}

// BoundTypes resolves only direct projected placeholders from already bound
// arguments. SQLX owns placeholder ordinals and protected SQL regions. Values
// never enter SQL, and expressions, ambiguous labels and opaque sources do not
// obtain type authority from their arguments.
func (p SelectorProjection) BoundTypes(labels []string, args []any) map[int]reflect.Type {
	if len(args) == 0 {
		return nil
	}
	parameters := sqlx.ParseParameters(p.SQL)
	if parameters.NamedCount() != 0 || parameters.PositionalCount() != len(args) {
		return nil
	}
	prefix := "__datly_bound_projection_"
	for strings.Contains(p.SQL, prefix) {
		prefix += "_"
	}
	types := make(map[string]reflect.Type, len(args))
	analysis := parameters.RewritePositional(func(index int) string {
		name := fmt.Sprintf("%s%d", prefix, index)
		types[name] = reflect.TypeOf(args[index])
		return ":" + name
	})
	return projectionValueHints(analysis, labels, func(value node.Node) reflect.Type {
		placeholder, ok := value.(*expr.Placeholder)
		if !ok {
			return nil
		}
		return types[strings.TrimPrefix(placeholder.Name, ":")]
	})
}

func projectionValueHints[T comparable](SQL string, labels []string, resolve func(node.Node) T) map[int]T {
	stmt, err := parseProjectionQuery(SQL)
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
	result := map[int]T{}
	var zero T
	for i, item := range stmt.List {
		if item == nil || item.Alias == "" {
			continue
		}
		value := resolve(item.Expr)
		if value == zero {
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
		result[index] = value
	}
	return result
}
