package column

import (
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	sqlio "github.com/viant/sqlx/io"
	"reflect"
	"strings"
)

// Application-computed maps and hook parent keys retain Go authority without
// declaring SQL result columns. Physical relation keys keep SQL authority.
func sqlResultView(view *spec.View) *spec.View {
	result := *view
	result.Columns = make([]*spec.Column, 0, len(view.Columns))
	for _, column := range view.Columns {
		if !computedMapColumn(view, column) && !hookParentColumn(view, column) {
			result.Columns = append(result.Columns, column)
		}
	}
	return &result
}

func hookParentColumn(view *spec.View, column *spec.Column) bool {
	if column == nil || !column.ExplicitType || column.Type.IsZero() || column.DeleteMarker || column.ConcurrencyToken {
		return false
	}
	tags := reflect.StructTag(column.Tag)
	if _, ok := tags.Lookup(tag.InvariantName); ok {
		return false
	}
	mapped := sqlio.ParseTag(tags)
	if mapped == nil || !mapped.Transient || tags.Get("relationKey") != "hook" {
		return false
	}
	for _, relation := range view.Relations {
		if relation == nil {
			continue
		}
		for _, link := range relation.On {
			if link == nil || (link.ParentNamespace != "" && !strings.EqualFold(link.ParentNamespace, view.Namespace)) {
				continue
			}
			if strings.EqualFold(link.ParentField, column.Name) && strings.EqualFold(link.ParentColumn, column.Source) {
				return true
			}
		}
	}
	return false
}

// computedMapColumn recognizes an explicitly excluded Go map, which has no
// scalar SQL result identity. Scalar CASTs retain their projection authority.
// A field used by a relation still needs physical projection unless
// hookParentColumn proves
// the established logical parent-key contract.
func computedMapColumn(view *spec.View, column *spec.Column) bool {
	if column == nil || !column.ExplicitType || !strings.HasPrefix(strings.TrimSpace(column.Type.Name), "map[") || column.DeleteMarker || column.ConcurrencyToken {
		return false
	}
	tags := reflect.StructTag(column.Tag)
	if _, ok := tags.Lookup(tag.InvariantName); ok || tags.Get("relationKey") != "" || tags.Get(sqlio.TagSqlx) != "-" {
		return false
	}
	for _, relation := range view.Relations {
		if relation == nil {
			continue
		}
		for _, link := range relation.On {
			if link == nil {
				continue
			}
			if (link.ParentNamespace == "" || strings.EqualFold(link.ParentNamespace, view.Namespace)) &&
				(strings.EqualFold(link.ParentField, column.Name) || column.Source != "" && strings.EqualFold(link.ParentColumn, column.Source)) {
				return false
			}
			if strings.EqualFold(link.ChildNamespace, view.Namespace) &&
				(strings.EqualFold(link.ChildField, column.Name) || column.Source != "" && strings.EqualFold(link.ChildColumn, column.Source)) {
				return false
			}
		}
	}
	return true
}
