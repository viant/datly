package compile

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
)

// planWildcardRenameMetadata resolves every direct field rename before any
// canonical name changes. A Go alias may coincide with another SQL source,
// so original source ownership must take precedence over sequential name
// lookup. Closed projections retain their separate declaration policy.
func planWildcardRenameMetadata(parsed *query.Select, root, view *spec.View) (map[*query.Item]*spec.Column, error) {
	type rename struct {
		item          *query.Item
		source, alias string
	}
	var renames []rename
	wildcard := false
	for _, item := range parsed.List {
		if item == nil {
			continue
		}
		column := sqlparser.NewColumn(item)
		namespace := column.Namespace
		if namespace == "" || namespace == "*" {
			namespace = root.Namespace
		}
		if strings.EqualFold(namespace, view.Namespace) {
			if _, star := projectionStar(item); star {
				wildcard = true
				break
			}
		}
	}
	if !wildcard {
		return nil, nil
	}
	sources := map[string]string{}
	outputs := map[string]bool{}
	for _, item := range parsed.List {
		if item == nil {
			continue
		}
		column := sqlparser.NewColumn(item)
		namespace := column.Namespace
		if namespace == "" || namespace == "*" {
			namespace = root.Namespace
		}
		if !strings.EqualFold(namespace, view.Namespace) {
			continue
		}
		if _, star := projectionStar(item); star {
			continue
		}
		output := wildcardRenameKey(column.Identity())
		if outputs[output] {
			return nil, fmt.Errorf("view %s has duplicate projected output %q", view.Namespace, column.Identity())
		}
		outputs[output] = true
		if item.Alias == "" || column.Expression != "" {
			continue
		}
		source, alias := wildcardRenameKey(column.Name), wildcardRenameKey(item.Alias)
		renames = append(renames, rename{item: item, source: source, alias: alias})
	}
	for _, rename := range renames {
		if previous, found := sources[rename.source]; found {
			return nil, fmt.Errorf("view %s has ambiguous wildcard rename source %s for %s and %s", view.Namespace, rename.source, previous, rename.item.Alias)
		}
		sources[rename.source] = rename.item.Alias
	}
	result := make(map[*query.Item]*spec.Column, len(renames))
	claimed := map[*spec.Column]string{}
	for _, rename := range renames {
		var physical, alias *spec.Column
		for _, existing := range view.Columns {
			if existing == nil {
				continue
			}
			source := wildcardRenameKey(existing.Source)
			if source == "" {
				source = wildcardRenameKey(existing.Name)
			}
			if source == rename.source {
				if physical != nil {
					return nil, fmt.Errorf("view %s wildcard rename source %s matches multiple canonical columns", view.Namespace, rename.source)
				}
				physical = existing
			}
			if wildcardRenameKey(existing.Name) != rename.alias {
				continue
			}
			if source != rename.source {
				// A physical annotation belongs to its own pending rename even
				// when its SQL name also spells this rename's Go alias.
				if _, renamed := sources[source]; renamed {
					continue
				}
				// Standalone alias annotations start with Source == Name.
				// A different explicit Source already owns another SQL result.
				if source != rename.alias {
					return nil, fmt.Errorf("view %s wildcard rename alias %s is owned by unrelated source %s", view.Namespace, rename.item.Alias, existing.Source)
				}
			}
			if alias != nil {
				return nil, fmt.Errorf("view %s wildcard rename alias %s matches multiple canonical columns", view.Namespace, rename.item.Alias)
			}
			alias = existing
		}
		metadata := physical
		if alias != nil {
			if physical != nil && physical != alias {
				return nil, fmt.Errorf("view %s wildcard rename source %s and alias %s have competing canonical columns", view.Namespace, rename.source, rename.item.Alias)
			}
			metadata = alias
		}
		if metadata != nil {
			if previous, found := claimed[metadata]; found {
				return nil, fmt.Errorf("view %s wildcard rename alias %s shares canonical metadata with %s", view.Namespace, rename.item.Alias, previous)
			}
			claimed[metadata] = rename.item.Alias
		}
		result[rename.item] = metadata
	}
	return result, nil
}

func wildcardRenameKey(name string) string {
	return strings.ToLower(projectionColumnName(name))
}
