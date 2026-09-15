package column

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata/sink"
)

// detectColumns keeps database result metadata native. An explicit application
// type authorizes missing driver type metadata independently of SQL provenance
// or DML mapping. Literal defaults are optional and never authorize other names.
func (r *Refiner) detectColumns(ctx context.Context, db *sql.DB, view *spec.View, query string, args ...any) ([]*sink.Column, error) {
	detector := io.ColumnDetector{ResolveTypes: func(columns []io.Column) map[int]reflect.Type {
		labels := make([]string, len(columns))
		for i, column := range columns {
			labels[i] = column.Name()
		}
		kinds := (dsql.SelectorProjection{SQL: query}).LiteralKinds(labels)
		result := make(map[int]reflect.Type, len(kinds))
		for i, kind := range kinds {
			switch kind {
			case "string":
				result[i] = reflect.TypeOf("")
			case "int":
				result[i] = reflect.TypeOf(int(0))
			}
		}
		return result
	}}
	for _, column := range view.Columns {
		if column != nil && column.ExplicitType && column.Type.IsZero() {
			return nil, fmt.Errorf("CAST column %s has no Go type", column.Name)
		}
		if column == nil || !column.ExplicitType {
			continue
		}
		if !column.NameInferred {
			detector.DeclaredColumns = append(detector.DeclaredColumns, column.Name)
		}
		if column.Source != "" {
			detector.DeclaredColumns = append(detector.DeclaredColumns, column.Source)
		}
	}
	return detector.Detect(ctx, db, query, args...)
}
