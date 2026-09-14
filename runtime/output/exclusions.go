package output

import (
	"fmt"
	"github.com/viant/tagly/format/text"
	xshape "github.com/viant/x/shape"
	"reflect"
	"strings"
)

type exclusions []string

// resolve preserves original Datly's normalized Go-field paths and also accepts
// unambiguous JSON aliases. All encoders receive canonical paths; an unresolved
// path is a registration error rather than a silently ineffective exclusion.
func (e exclusions) resolve(typeOf reflect.Type, rows *rowsPlan) (exclusions, error) {
	var result exclusions
	for _, path := range e {
		var canonical string
		var err error
		qualified := false
		if rows != nil && rows.name != "" {
			first, _, _ := strings.Cut(path, ".")
			qualified = normalizedExclusion(first) == normalizedExclusion(rows.name)
			for _, field := range rows.fields {
				if field.Name == rows.name {
					alias, _, _ := strings.Cut(field.Tag.Get("json"), ",")
					qualified = qualified || alias != "" && normalizedExclusion(first) == normalizedExclusion(alias)
				}
			}
		}
		if rows != nil && rows.name != "" && !qualified {
			canonical, err = e.path(typeOf, rows.name+"."+path)
		} else {
			err = fmt.Errorf("no row envelope")
		}
		if err != nil {
			canonical, err = e.path(typeOf, path)
		}
		if err != nil {
			return nil, fmt.Errorf("output exclusion %q: %w", path, err)
		}
		result = append(result, canonical)
	}
	return result, nil
}

func (e exclusions) path(typeOf reflect.Type, path string) (string, error) {
	if typeOf == nil {
		return "", fmt.Errorf("output type is required")
	}
	current := xshape.Linked(typeOf)
	var names []string
	for _, part := range strings.Split(path, ".") {
		part = strings.TrimSpace(part)
		if part == "" {
			return "", fmt.Errorf("empty field path segment")
		}
		fields, err := current.Fields()
		if err != nil {
			return "", err
		}
		var selected *xshape.Field
		normalized := text.DetectCaseFormat(part).Format(part, text.CaseFormatUpperCamel)
		for i := range fields {
			field := &fields[i]
			if !field.Exported {
				continue
			}
			alias, _, _ := strings.Cut(field.Tag.Get("json"), ",")
			if field.Name == part {
				selected = field
				break
			}
			if strings.EqualFold(field.Name, part) || strings.EqualFold(field.Name, normalized) || alias != "-" && alias != "" && strings.EqualFold(alias, part) {
				if selected != nil {
					return "", fmt.Errorf("ambiguous field %q", part)
				}
				selected = field
			}
		}
		if selected == nil {
			return "", fmt.Errorf("field %q was not found", part)
		}
		// Visible promoted fields carry the complete owner index. Preserve that
		// owner path so typed projections descend through the embedded holder.
		for depth := 1; depth < len(selected.Index); depth++ {
			for _, owner := range fields {
				if reflect.DeepEqual(owner.Index, selected.Index[:depth]) {
					if !owner.Exported {
						return "", fmt.Errorf("promoted field %s has an unexported owner", part)
					}
					names = append(names, owner.Name)
					break
				}
			}
		}
		names = append(names, selected.Name)
		current = xshape.Linked(selected.ReflectedType)
	}
	return strings.Join(names, "."), nil
}

func (e exclusions) Exclude(path, name string) bool {
	full := name
	if path != "" {
		full = path + "." + name
	}
	for _, excluded := range e {
		if normalizedExclusion(excluded) == normalizedExclusion(full) {
			return true
		}
	}
	return false
}

func normalizedExclusion(path string) string {
	parts := strings.Split(path, ".")
	for i, part := range parts {
		parts[i] = strings.ToLower(text.DetectCaseFormat(part).Format(part, text.CaseFormatUpperCamel))
	}
	return strings.Join(parts, ".")
}
