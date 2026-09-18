package column

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	sqlio "github.com/viant/sqlx/io"
)

// ApplyWriterMetadata promotes explicitly authored SQLX field metadata
// into the canonical facts required by mutation planning. It is intentionally
// called only by writer generation; readers neither require nor infer identity
// from author tags.
func ApplyWriterMetadata(component *spec.Component) error {
	if component == nil {
		return fmt.Errorf("writer metadata requires a component")
	}
	visited := map[*spec.View]bool{}
	var applyView func(*spec.View) error
	applyView = func(view *spec.View) error {
		if view == nil || visited[view] {
			return nil
		}
		visited[view] = true
		for _, column := range view.Columns {
			if column == nil {
				continue
			}
			structTag := reflect.StructTag(strings.TrimSpace(column.Tag))
			metadata := sqlio.ParseTag(structTag)
			column.PrimaryKey = column.PrimaryKey || metadata.PrimaryKey
			column.AutoIncrement = column.AutoIncrement || metadata.Autoincrement
			column.Unique = column.Unique || metadata.IsUnique
			column.NotNull = column.NotNull || metadata.Required
		}
		for _, relation := range view.Relations {
			if relation != nil {
				if err := applyView(relation.View); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := applyView(component.RootView); err != nil {
		return err
	}
	for _, view := range component.Views {
		if err := applyView(view); err != nil {
			return err
		}
	}
	return nil
}
