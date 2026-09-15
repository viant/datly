package column

import (
	"fmt"
	"sort"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
)

// retainNamedIdentity retains only table identities already exposed by the
// named SQL source. It cannot recover keys omitted by the inner source, change
// its predicates, or turn a grouped/set projection into a writable table.
func retainNamedIdentity(view *spec.View, source *spec.ViewSource, constraints map[string]tableConstraint) error {
	if source.SQL == "" || source.Table == "" || view.Auxiliary {
		return nil
	}
	parsed, err := sqlparser.ParseQuery(source.SQL)
	if err != nil {
		// Template programs retain their existing discovery path.
		return nil
	}
	if parsed.From.Alias == "" || len(parsed.Joins) != 0 || parsed.Union != nil || parsed.Kind != "" || len(parsed.GroupBy) > 0 || parsed.Having != nil {
		return nil
	}
	if table, _, err := sqlparser.SourceTable(parsed.From.X); err != nil {
		return err
	} else if table != "" {
		cte := false
		for _, with := range parsed.WithSelects {
			if with != nil && strings.EqualFold(with.Alias, table) {
				cte = true
				break
			}
		}
		if !cte {
			return nil
		}
	}
	resolver := projectionLineageResolver{table: source.Table, withs: parsed.WithSelects}
	inner, err := resolver.source(parsed.From.X)
	if err != nil {
		return err
	}
	selected, err := resolver.query(parsed)
	if err != nil {
		return err
	}
	keys := []string{}
	for key, constraint := range constraints {
		if constraint.primaryKey {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	changed := false
	for _, key := range keys {
		found := selected.wildcard && !selected.blocked[key]
		for _, physical := range selected.direct {
			found = found || physical == key
		}
		if found {
			continue
		}
		name := ""
		for output, physical := range inner.direct {
			if physical != key {
				continue
			}
			if name != "" {
				return fmt.Errorf("view %s has ambiguous identity projection for %s", view.Name, key)
			}
			name = inner.names[output]
		}
		if name == "" && inner.wildcard && !inner.blocked[key] {
			name = constraints[key].name
		}
		if name == "" {
			continue
		}
		// An unrelated projected expression must never acquire key authority
		// merely because it uses the physical key's spelling.
		for _, item := range parsed.List {
			if normalizedName(sqlparser.NewColumn(item).Identity()) == normalizedName(name) {
				return fmt.Errorf("view %s output %s conflicts with its backing identity", view.Name, name)
			}
		}
		parts, err := sqlparser.TableIdentifierParts(name)
		if err != nil || len(parts) != 1 || parts[0] != name {
			return fmt.Errorf("view %s backing identity %q requires a supported source identifier", view.Name, name)
		}
		identifier := name
		parsed.List = append(parsed.List, query.NewItem(&expr.Selector{Name: parsed.From.Alias, X: &expr.Ident{Name: identifier}}))
		view.Columns = append(view.Columns, &spec.Column{Name: name, Source: name, Tag: `internal:"true"`})
		changed = true
	}
	if changed {
		source.SQL = strings.TrimSpace((sqlparser.Stringifier{PreserveWindow: true}).String(parsed))
		view.Source.SQL = source.SQL
	}
	return nil
}
