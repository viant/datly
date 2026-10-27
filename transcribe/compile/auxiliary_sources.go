package compile

import (
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	"strings"
)

// lowerAuxiliarySources runs after relation metadata has captured read-only
// intent. Only complete source-position table enclosures are lowered; derived
// SELECT parentheses and ordinary expression parentheses remain SQL syntax.
func lowerAuxiliarySources(selected *query.Select) (bool, error) {
	if selected == nil {
		return false, nil
	}
	changed := false
	lower := func(source node.Node) (node.Node, bool, error) { return lowerAuxiliarySource(source) }
	source, rewritten, err := lower(selected.From.X)
	if err != nil {
		return false, err
	}
	selected.From.X = source
	changed = changed || rewritten
	for _, join := range selected.Joins {
		if join != nil {
			source, rewritten, err = lower(join.With)
			if err != nil {
				return false, err
			}
			join.With = source
			changed = changed || rewritten
		}
	}
	for _, with := range selected.WithSelects {
		if with == nil {
			continue
		}
		if with.X != nil {
			rewritten, err = lowerAuxiliarySources(with.X)
			if err != nil {
				return false, err
			}
			if rewritten {
				with.Raw = "(" + (sqlparser.Stringifier{PreserveWindow: true}).String(with.X) + ")"
				changed = true
			}
		} else if strings.TrimSpace(with.Raw) != "" {
			source, rewritten, err = lower(&expr.Raw{Raw: with.Raw})
			if err != nil {
				return false, err
			}
			if rewritten {
				with.Raw = sqlparser.Stringify(source)
				changed = true
			}
		}
	}
	if selected.Union != nil && selected.Union.X != nil {
		rewritten, err = lowerAuxiliarySources(selected.Union.X)
		if err != nil {
			return false, err
		}
		if rewritten {
			changed = true
		}
	}
	return changed, nil
}
func lowerAuxiliarySource(source node.Node) (node.Node, bool, error) {
	if source == nil {
		return source, false, nil
	}
	table, auxiliary, err := sqlparser.SourceTable(source)
	if err != nil {
		return nil, false, err
	}
	if table != "" {
		if auxiliary {
			return expr.NewSelector(table), true, nil
		}
		return source, false, nil
	}
	var raw string
	switch actual := source.(type) {
	case *query.Select:
		changed, err := lowerAuxiliarySources(actual)
		return actual, changed, err
	case *expr.Raw:
		raw = actual.Raw
	case *expr.Parenthesis:
		raw = actual.Raw
	default:
		return source, false, nil
	}
	prepared, err := prepareSubquery(raw)
	if err != nil || prepared == nil || len(prepared.embeds) != 0 {
		return source, false, nil
	}
	changed, err := lowerAuxiliarySources(prepared.query)
	if err != nil || !changed {
		return source, changed, err
	}
	rendered := (sqlparser.Stringifier{PreserveWindow: true}).String(prepared.query)
	if strings.HasPrefix(strings.TrimSpace(raw), "(") {
		rendered = "(" + rendered + ")"
	}
	switch actual := source.(type) {
	case *expr.Raw:
		actual.Raw = rendered
		actual.X = prepared.query
	case *expr.Parenthesis:
		actual.Raw = rendered
		actual.X = prepared.query
	}
	return source, true, nil
}
