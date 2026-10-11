package compile

import (
	"fmt"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/typecatalog"
	sqlio "github.com/viant/sqlx/io"
	xshape "github.com/viant/x/shape"
	"reflect"
	"strings"
)

// Linked transient parent keys carry the same hook provenance used by the
// runtime collector. They are logical Go fields, not SQL result columns.
func lowerLinkedHookKeys(root *spec.View, directives []viewDirective, types *typecatalog.Resolver) error {
	if types == nil {
		return nil
	}
	views := canonicalViews(root)
	for _, directive := range directives {
		if directive.name != spec.ViewControlType {
			continue
		}
		view := views[strings.ToLower(directive.target)]
		if view == nil {
			continue
		}
		descriptor, err := types.Descriptor(directive.value)
		// Generated row names need not exist in linked package authority yet.
		if err != nil || descriptor == nil {
			continue
		}
		fields, err := xshape.New(descriptor, types.Descriptor).FieldsAt("")
		if err != nil {
			return err
		}
		for _, relation := range view.Relations {
			if relation == nil {
				continue
			}
			for _, link := range relation.On {
				if link == nil {
					continue
				}
				var matched *xshape.Field
				for i := range fields {
					field := fields[i]
					if !field.Exported || !strings.EqualFold(field.Name, typecatalog.FieldName(link.ParentColumn)) || !hookRelationTag(string(field.Tag)) {
						continue
					}
					if matched != nil {
						return fmt.Errorf("ambiguous linked hook key %s.%s", directive.target, link.ParentColumn)
					}
					matched = &fields[i]
				}
				if matched != nil {
					field := *matched
					link.ParentField = field.Name
					found := false
					for _, column := range view.Columns {
						if column != nil && sameProjectionColumn(column, link.ParentColumn) {
							found = true
							break
						}
					}
					if !found {
						identity, err := field.CanonicalType()
						if err != nil {
							return err
						}
						typeRef, err := dql.ColumnType(identity, nil)
						if err != nil {
							return err
						}
						view.Columns = append(view.Columns, &spec.Column{Name: field.Name, Source: link.ParentColumn, Type: typeRef, ExplicitType: true, Tag: string(field.Tag)})
					}
				}
			}
		}
	}
	return nil
}

func hookRelationTag(raw string) bool {
	tag := reflect.StructTag(raw)
	mapped := sqlio.ParseTag(tag)
	return mapped != nil && mapped.Transient && tag.Get("relationKey") == "hook"
}

func hookRelationColumn(view *spec.View, name string) bool {
	for _, column := range view.Columns {
		if column != nil && column.ExplicitType && sameProjectionColumn(column, name) && hookRelationTag(column.Tag) {
			return true
		}
	}
	return false
}
