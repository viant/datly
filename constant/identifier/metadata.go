package identifier

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
	"github.com/viant/tagly/tags"
)

// Metadata resolves a single trusted identifier, never an arbitrary SQL fragment.
func (r Renderer) Metadata(source string) (string, error) {
	if !strings.Contains(source, "$") && !strings.HasPrefix(source, "Unsafe_") {
		return source, nil
	}
	resolved, err := r.Identifier(source)
	if err != nil {
		return "", err
	}
	if strings.Contains(resolved, "$") && !strings.ContainsAny(source, "`\"[]") {
		// A complete scalar reference (e.g. refDb=${schema}) is an identifier
		// here, although the same reference in SQL normally binds a value.
		resolved, err = r.Identifier("`" + source + "`")
		if err != nil {
			return "", err
		}
	}
	if strings.Contains(resolved, "$") {
		return "", fmt.Errorf("unresolved identifier %q: declare an immutable constant", source)
	}
	parts, err := sqlparser.TableIdentifierParts(resolved)
	if err != nil {
		return "", fmt.Errorf("identifier %q: %w", source, err)
	}
	if len(parts) == 1 {
		return parts[0], nil
	}
	return resolved, nil
}

// Tag returns a detached SQLX tag with only identifier-valued options resolved.
func (r Renderer) Tag(source reflect.StructTag) (reflect.StructTag, error) {
	parsed := tags.NewTags(string(source))
	sqlx := parsed.Lookup("sqlx")
	if sqlx == nil || !strings.Contains(string(sqlx.Values), "$") {
		return source, nil
	}
	var options []string
	err := sqlx.Values.Match(func(option string) error {
		key, value, found := strings.Cut(option, "=")
		if found {
			switch strings.ToLower(strings.TrimSpace(key)) {
			case "table", "db", "reftable", "refdb":
				resolved, err := r.Metadata(strings.TrimSpace(value))
				if err != nil {
					return fmt.Errorf("sqlx %s: %w", key, err)
				}
				option = key + "=" + resolved
			}
		}
		options = append(options, option)
		return nil
	})
	if err != nil {
		return "", err
	}
	sqlx.Values = tags.Values(strings.Join(options, ","))
	return reflect.StructTag(parsed.Literal()), nil
}

// Component resolves runtime metadata on a private copy. SQL and resources
// retain their template expressions for the normal SQL compiler.
func (r Renderer) Component(source *spec.Component) (*spec.Component, error) {
	result := source.Clone()
	seen := map[*spec.View]bool{}
	var visit func(*spec.View) error
	visit = func(view *spec.View) error {
		if view == nil || seen[view] {
			return nil
		}
		seen[view] = true
		if view.Source != nil {
			var err error
			view.Source.Table, err = r.Metadata(view.Source.Table)
			if err != nil {
				return fmt.Errorf("view %s table: %w", view.CanonicalName(), err)
			}
		}
		for _, column := range view.Columns {
			if column == nil {
				continue
			}
			tag, err := r.Tag(reflect.StructTag(column.Tag))
			if err != nil {
				return fmt.Errorf("view %s column %s: %w", view.CanonicalName(), column.Name, err)
			}
			column.Tag = string(tag)
		}
		for _, relation := range view.Relations {
			if relation != nil {
				if err := visit(relation.View); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if result != nil {
		if err := visit(result.RootView); err != nil {
			return nil, err
		}
		for _, view := range result.Views {
			if err := visit(view); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}
