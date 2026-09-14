package docs

import (
	"fmt"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"github.com/viant/sqlparser"
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
	result.lineage = map[string]*sqlparser.ColumnLineage{}
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
	// Retain only annotation identity; the source may be reused by its author.
	copy := &spec.View{Columns: make([]*spec.Column, 0, len(view.Columns))}
	if view.Source != nil {
		copy.Source = &spec.ViewSource{Table: view.Source.Table}
	}
	for _, column := range view.Columns {
		if column != nil {
			copy.Columns = append(copy.Columns, &spec.Column{Name: column.Name, Source: column.Source, Tag: column.Tag})
		}
	}
	s.views[path] = copy
	if view.Source != nil && view.Source.SQL != "" {
		if parsed, err := sqlparser.ParseQuery(view.Source.SQL); err == nil {
			lineage := (sqlparser.Lineage{Query: parsed}).Compile()
			s.lineage[path] = lineage
			if copy.Source == nil {
				copy.Source = &spec.ViewSource{}
			}
			if copy.Source.Table == "" {
				copy.Source.Table = lineage.RootTable()
			}
		}
	}
	for _, relation := range view.Relations {
		if relation != nil {
			s.addView(path+"."+relation.Holder, relation.View, active)
		}
	}
}

// StructField is called by existing schema projectors with their canonical field
// and path. SQLX and Datly tag owners supply aliases and explicit annotations.
func (s *Snapshot) StructField(path string, field reflect.StructField) Annotation {
	column := sqlxio.ParseTag(field.Tag).Name()
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
				for _, c := range view.Columns {
					if strings.EqualFold(c.Name, field.Name) {
						if f.Authored.Description == "" {
							f.Authored.Description = reflect.StructTag(c.Tag).Get("desc")
						}
						if f.Authored.Example == "" {
							f.Authored.Example = reflect.StructTag(c.Tag).Get("example")
						}
						if c.Source != "" {
							f.Column = c.Source
						}
						break
					}
				}
				if origins, ok := s.lineage[prefix]; ok {
					f.Table = ""
					if origin, ok := origins.Lookup(f.Column); ok {
						f.Table = origin.Table
						f.Column = origin.Column
					}
				}
				break
			}
		}
	}
	return s.Field(f)
}
