package compile

import (
	"fmt"
	"reflect"
	"strings"

	sqlio "github.com/viant/sqlx/io"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"github.com/viant/tagly/tags"
)

// readViewProjection partitions the authored outer list by canonical namespace.
// Outer scalar aliases rename Go fields; executable projections keep the source
// column label. Expressions and aliases inside the source SQL remain intact.
// It retains source AST expressions; it never rewrites an inner
// query or substitutes its table for SQL. Missing matching columns are internal
// backing fields, using the same contract as explicit star exclusions.
func readViewProjection(parsed *query.Select, root, view *spec.View) (query.List, error) {
	if view == nil {
		return nil, fmt.Errorf("projection requires a canonical view")
	}
	if err := validateRelationSourceOutputs(parsed, root, view); err != nil {
		return nil, err
	}
	renames, err := planWildcardRenameMetadata(parsed, root, view)
	if err != nil {
		return nil, err
	}
	var result query.List
	wildcard := false
	outputs := map[string]bool{}
	direct := map[string][]string{}
	views := canonicalViews(root)
	for _, item := range parsed.List {
		if item == nil {
			continue
		}
		column := sqlparser.NewColumn(item)
		namespace := column.Namespace
		if namespace == "" || namespace == "*" {
			namespace = root.Namespace
		}
		if views[strings.ToLower(namespace)] == nil {
			return nil, fmt.Errorf("projection namespace %q has no canonical view", namespace)
		}
		if !strings.EqualFold(namespace, view.Namespace) {
			continue
		}
		if _, star := projectionStar(item); star {
			wildcard = true
			result = append(result, item)
			continue
		}
		name := strings.ToLower(projectionColumnName(column.Identity()))
		if name == "" {
			return nil, fmt.Errorf("view %s projection requires an output name", view.Namespace)
		}
		if outputs[name] {
			return nil, fmt.Errorf("view %s has duplicate projected output %q", view.Namespace, column.Identity())
		}
		outputs[name] = true
		if column.Expression == "" {
			direct[strings.ToLower(projectionColumnName(column.Name))] = append(direct[strings.ToLower(projectionColumnName(column.Name))], column.Identity())
		}
		projected := item
		if item.Alias != "" && column.Expression == "" {
			// This SELECT describes the component shape, not a vendor SQL alias.
			// Record the Go name against the independently executed source output.
			var metadata *spec.Column
			if renames != nil {
				metadata = renames[item]
			} else {
				for _, existing := range view.Columns {
					if existing != nil && strings.EqualFold(existing.Name, item.Alias) {
						metadata = existing
						break
					}
				}
			}
			if metadata == nil {
				metadata = &spec.Column{Name: item.Alias}
				view.Columns = append(view.Columns, metadata)
			}
			if renames != nil {
				metadata.Name = item.Alias
			}
			metadata.Source = column.Name
			if reflect.StructTag(metadata.Tag).Get(sqlio.TagSqlx) == "" {
				parsedTags := tags.NewTags(metadata.Tag)
				parsedTags.Set(sqlio.TagSqlx, column.Name)
				metadata.Tag = parsedTags.Stringify()
			}
			copy := *item
			copy.Alias = ""
			copy.Raw = ""
			projected = &copy
		}
		result = append(result, projected)
	}
	if wildcard {
		// A field rename over a wildcard must not select the same SQL column twice.
		var filtered query.List
		for _, item := range result {
			if _, star := projectionStar(item); star {
				filtered = append(filtered, item)
				continue
			}
			column := sqlparser.NewColumn(item)
			if column.Expression == "" {
				continue
			}
			filtered = append(filtered, item)
		}
		result = filtered

		if len(result) == 1 {
			return nil, nil
		}
		return result, nil
	}
	// A table-root read declares each unlisted relation's own row contract.
	// Only an outer projection over a named root source narrows an unlisted
	// SQL relation to its internal matching fields. Explicit child selections
	// still apply in either form.
	if len(result) == 0 {
		if view.Source != nil && view.Source.SQL == "" {
			return nil, nil
		}
		table, _, err := sqlparser.SourceTable(parsed.From.X)
		if err != nil {
			return nil, err
		}
		if view != root && table != "" && !referencesCTE(parsed.From.X, parsed.WithSelects) {
			return nil, nil
		}
	}
	var backing func(*spec.View) error
	add := func(name string) (string, error) {
		key := strings.ToLower(projectionColumnName(name))
		for _, output := range direct[key] {
			if strings.EqualFold(projectionColumnName(output), projectionColumnName(name)) {
				return output, nil
			}
		}
		if len(direct[key]) == 1 {
			return direct[key][0], nil
		}
		if len(direct[key]) > 1 {
			return "", fmt.Errorf("view %s has ambiguous projected relation key %s", view.Namespace, name)
		}
		if outputs[key] {
			return "", fmt.Errorf("view %s projected output %s conflicts with its relation key", view.Namespace, name)
		}
		outputs[key] = true
		result = append(result, query.NewItem(expr.NewSelector(view.Namespace+"."+name)))
		markInternalColumn(view, name)
		direct[key] = []string{name}
		return name, nil
	}
	backing = func(parent *spec.View) error {
		for _, relation := range parent.Relations {
			if relation == nil || relation.View == nil {
				continue
			}
			for _, link := range relation.On {
				if link == nil {
					continue
				}
				var err error
				if parent == view {
					link.ParentColumn, err = add(link.ParentColumn)
				}
				if err != nil {
					return err
				}
				if relation.View == view {
					link.ChildColumn, err = add(link.ChildColumn)
				}
				if err != nil {
					return err
				}
			}
			if err := backing(relation.View); err != nil {
				return err
			}
		}
		return nil
	}
	if err := backing(root); err != nil {
		return nil, err
	}
	return result, nil
}

// Declarations refer to public outputs, before internal matching fields are
// appended. An annotation must not resurrect a removed or renamed output.
func validateDeclaredProjectionTargets(parsed *query.Select, root *spec.View, targets map[*spec.View][]string) error {
	for view, names := range targets {
		outputs := map[string]bool{}
		wildcard := false
		for _, item := range parsed.List {
			column := sqlparser.NewColumn(item)
			namespace := column.Namespace
			if namespace == "" || namespace == "*" {
				namespace = root.Namespace
			}
			if !strings.EqualFold(namespace, view.Namespace) {
				continue
			}
			if _, ok := projectionStar(item); ok {
				wildcard = true
				break
			}
			outputs[strings.ToLower(projectionColumnName(column.Identity()))] = true
		}
		if wildcard {
			continue
		}
		for _, name := range names {
			logical := false
			for _, column := range view.Columns {
				if column != nil && sameProjectionColumn(column, name) && column.ExplicitType && sqlio.ParseTag(reflect.StructTag(column.Tag)).Transient {
					logical = true
					break
				}
			}
			if !logical && !outputs[strings.ToLower(projectionColumnName(name))] {
				return fmt.Errorf("annotation target %s.%s is absent from the SQL projection", view.Namespace, name)
			}
		}
	}
	return nil
}
