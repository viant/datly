package docs

import (
	"fmt"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	sqlxio "github.com/viant/sqlx/io"
	"reflect"
	"strings"
)

// ForComponent fixes view/column provenance from canonical contracts, without
// walking Go fields or changing the structural view model.
func (s *Snapshot) ForComponent(component *spec.Component, output reflect.Type) (*Snapshot, error) {
	if s == nil || component == nil {
		return nil, fmt.Errorf("documentation snapshot and component required")
	}
	result := *s
	result.views = map[string]*spec.View{}
	if component.RootView == nil {
		return &result, nil
	}
	index, err := tag.NewBindingIndex(output)
	if err != nil {
		return nil, err
	}
	for _, param := range spec.EffectiveParameters(component.Parameters) {
		if param == nil || param.Source.Kind != "output" || param.Source.Name != "view" {
			continue
		}
		field, ok, err := index.Resolve(param)
		if err != nil {
			return nil, err
		}
		if ok {
			result.addView(field.Name, component.RootView, map[*spec.View]bool{})
		}
	}
	return &result, nil
}
func (s *Snapshot) addView(path string, view *spec.View, active map[*spec.View]bool) {
	if view == nil || active[view] {
		return
	}
	active[view] = true
	defer delete(active, view)
	value := *view
	copy := &value
	copy.Source = view.Source.Clone()
	copy.Relations = nil
	copy.Columns = make([]*spec.Column, 0, len(view.Columns))
	for _, column := range view.Columns {
		if column != nil {
			copy.Columns = append(copy.Columns, column.Clone())
		}
	}
	if copy.Source == nil {
		copy.Source = &spec.ViewSource{}
	}
	copy.Source.Table = view.ProjectionTable()
	s.views[path] = copy

	for _, relation := range view.Relations {
		if relation != nil {
			s.addView(path+"."+relation.Holder, relation.View, active)
		}
	}
}

// StructField is called by existing schema projectors with their canonical field
// and path. SQLX and Datly tag owners supply aliases and explicit annotations.
func (s *Snapshot) StructField(path string, field reflect.StructField) Annotation {
	mapping := sqlxio.ParseTag(field.Tag)
	column := mapping.Name()
	if column == "" {
		column = field.Tag.Get("source")
	}
	if column == "" {
		column = field.Name
	}
	f := Field{Path: path, Name: field.Name, Column: column, Authored: Annotation{Description: field.Tag.Get("desc"), Example: field.Tag.Get("example")}}
	if s != nil {
		if view := s.views[path]; view != nil && view.Source != nil {
			f.Table = view.Source.Table
			f.Column = "_"
			return s.Field(f)
		}
		prefix := path
		for {
			cut := strings.LastIndex(prefix, ".")
			if cut < 0 {
				break
			}
			prefix = prefix[:cut]
			if view := s.views[prefix]; view != nil {
				if view.Source != nil {
					f.Table = view.Source.Table
				}
				name := field.Name
				if selector := field.Tag.Get("selectorAlias"); selector != "" {
					name = selector
				} else if _, projected, ok := strings.Cut(mapping.Column, "|"); ok {
					projected, _, _ = strings.Cut(projected, "|")
					name = strings.TrimSpace(projected)
				}
				var matched *spec.Column
				for _, c := range view.Columns {
					if strings.EqualFold(c.Name, name) || strings.EqualFold(c.Name, field.Name) || (field.Tag.Get("sqlOutput") != "" && strings.EqualFold(c.Output, field.Tag.Get("sqlOutput"))) {
						matched = c
						break
					}
				}
				if matched == nil {
					// A physical-source fallback is safe only when it identifies one occurrence column.
					for _, c := range view.Columns {
						if strings.EqualFold(c.Source, f.Column) {
							if matched != nil {
								matched = nil
								break
							}
							matched = c
						}
					}
				}
				if c := matched; c != nil {
					if f.Authored.Description == "" {
						f.Authored.Description = reflect.StructTag(c.Tag).Get("desc")
					}
					if f.Authored.Example == "" {
						f.Authored.Example = reflect.StructTag(c.Tag).Get("example")
					}
					if c.Source != "" {
						f.Column = c.Source
					}
					if table, column, known := view.ProjectionOrigin(c.Name); known {
						f.Table = table
						if column != "" {
							f.Column = column
						}
					}
				}

				break
			}
		}
	}
	// Explicit SQLX physical mappings are canonical and take precedence.
	if mapping.Table != "" {
		f.Table = mapping.Table
		if mapping.Db != "" {
			f.Table = mapping.Db + "." + f.Table
		}
		f.Column = mapping.Name()
	}
	return s.Field(f)
}
