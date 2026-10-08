package bootstrap

import (
	"fmt"
	"github.com/viant/datly/spec"
	sqlxio "github.com/viant/sqlx/io"
	"reflect"
	"strings"
)

// ViewProjection consumes the complete linked column contract. SQL validation
// and alias discovery belong to transcription, never component materialization.
type ViewProjection struct{ View *spec.View }
type OutputColumn struct {
	Column   *spec.Column
	Name     string
	Selector string
}

func (p ViewProjection) Columns() ([]OutputColumn, error) {
	if p.View == nil || p.View.Source == nil {
		return nil, fmt.Errorf("source projection is required")
	}
	result := make([]OutputColumn, 0, len(p.View.Columns))
	names := map[string]bool{}
	for _, column := range p.View.Columns {
		if column == nil || strings.TrimSpace(column.Name) == "" {
			continue
		}
		name := strings.TrimSpace(column.Output)
		if name == "" {
			name = strings.TrimSpace(reflect.StructTag(column.Tag).Get("sqlOutput"))
		}
		if name == "" {
			metadata := sqlxio.ParseTag(reflect.StructTag(column.Tag))
			if _, alias, ok := strings.Cut(metadata.Column, "|"); ok {
				name = strings.TrimSpace(alias)
			}
		}
		if name == "" {
			name = strings.TrimSpace(column.Source)
		}
		if name == "" {
			name = strings.TrimSpace(column.Name)
		}
		if column.Output == "" && strings.TrimSpace(p.View.Source.SQL) == "" && p.View.Source.Table != "" && !column.NameInferred {
			name = column.Name
		}
		selector := strings.TrimSpace(column.Selector)
		if selector == "" {
			selector = name
			if !column.NameInferred && !strings.EqualFold(column.Name, name) {
				selector = column.Name
			}
		}
		key := strings.ToLower(name)
		if names[key] {
			return nil, fmt.Errorf("duplicate projected column metadata %q; assign distinct SQL aliases", name)
		}
		names[key] = true
		result = append(result, OutputColumn{Column: column, Name: name, Selector: selector})
	}
	return result, nil
}
