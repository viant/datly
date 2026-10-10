package column

import (
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	sqlio "github.com/viant/sqlx/io"
	"reflect"
	"strings"
)

// Hook parent keys retain Go authority but do not declare SQL result columns.
// Only a transient field used by an actual parent link has that provenance.
func sqlResultView(view *spec.View) *spec.View {
	result := *view
	result.Columns = make([]*spec.Column, 0, len(view.Columns))
	for _, column := range view.Columns {
		if !hookParentColumn(view, column) {
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
