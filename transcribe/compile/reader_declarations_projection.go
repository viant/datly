package compile

import (
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

// validateInvariantProjections uses native output identities for explicit
// projections. Wildcard output existence is checked by SQLX discovery.
func validateInvariantProjections(parsed *query.Select, root *spec.View) error {
	views := canonicalViews(root)
	for _, view := range views {
		var names []string
		wildcard := false
		for _, item := range parsed.List {
			if star, ok := projectionStar(item); ok {
				namespace := starNamespace(star)
				if strings.EqualFold(namespace, view.Namespace) || ((namespace == "" || namespace == "*") && view == root) {
					wildcard = true
					break
				}
				continue
			}
			output := sqlparser.NewColumn(item)
			if output.Namespace != "" && !strings.EqualFold(output.Namespace, view.Namespace) {
				continue
			}
			if output.Namespace == "" && view != root {
				continue
			}
			names = append(names, output.Identity())
		}
		if !wildcard {
			if err := column.ValidateInvariants(view, names); err != nil {
				return err
			}
		}
	}
	if err := validateInvariantSourceProjection(parsed.From.X, root, parsed.WithSelects); err != nil {
		return err
	}
	for _, join := range parsed.Joins {
		if join == nil {
			continue
		}
		namespace := strings.TrimSpace(join.Alias)
		if namespace == "" {
			namespace = terminalName(join.With)
		}
		if view := views[strings.ToLower(namespace)]; view != nil {
			if err := validateInvariantSourceProjection(join.With, view, parsed.WithSelects); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateInvariantSourceProjection(source node.Node, view *spec.View, withs query.WithSelects) error {
	var parsed *query.Select
	var raw string
	switch actual := source.(type) {
	case *query.Select:
		parsed = actual
	case *expr.Ident:
		for _, with := range withs {
			if with != nil && strings.EqualFold(with.Alias, actual.Name) {
				parsed, raw = with.X, with.Raw
				break
			}
		}
	case *expr.Raw:
		raw = actual.Raw
	case *expr.Parenthesis:
		raw = actual.Raw
	}
	if parsed == nil && raw != "" {
		if table, _, err := sqlparser.SourceTable(source); err != nil {
			return err
		} else if table != "" {
			return nil
		}
		prepared, err := prepareSubquery(raw)
		if err != nil {
			return err
		}
		if prepared != nil {
			parsed = prepared.query
		}
	}
	if parsed == nil {
		return nil
	}
	var names []string
	for _, item := range parsed.List {
		if _, ok := projectionStar(item); ok {
			return nil
		}
		names = append(names, sqlparser.NewColumn(item).Identity())
	}
	return column.ValidateInvariants(view, names)
}
