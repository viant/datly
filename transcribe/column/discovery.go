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
	source := &spec.ViewSource{SQL: query, Table: directSourceTable(query)}
	identities, err := resolveResultSources(view.Columns, source)
	if err != nil {
		return nil, err
	}
	return r.detectColumnsWithSources(ctx, db, view, identities, query, args...)
}

func (r *Refiner) detectColumnsWithSources(ctx context.Context, db *sql.DB, view *spec.View, identities resultSourceIdentity, query string, args ...any) ([]*sink.Column, error) {
	declared, err := declaredResultColumns(view.Columns, identities)
	if err != nil {
		return nil, err
	}
	detector := io.ColumnDetector{DeclaredColumns: declared, ResolveTypes: func(columns []io.Column) map[int]reflect.Type {
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
	return detector.Detect(ctx, db, query, args...)
}

func declaredResultColumns(columns []*spec.Column, identities resultSourceIdentity) ([]string, error) {
	var declared []string
	for _, column := range columns {
		if column != nil && column.ExplicitType && column.Type.IsZero() {
			return nil, fmt.Errorf("CAST column %s has no Go type", column.Name)
		}
		if column != nil && (column.ExplicitType || column.NameInferred && !column.Type.IsZero()) && identities[column] != "" {
			declared = append(declared, identities[column])
		}
	}
	return declared, nil
}
