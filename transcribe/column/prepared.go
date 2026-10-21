package column

import (
	"context"
	"fmt"
	"io/fs"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/typecatalog"
	sqlio "github.com/viant/sqlx/io"
)

// PreparedColumnRequest describes one exact SQL result to a trusted compiler
// integration. Source is a detached, resource-expanded copy; changing it cannot
// change executable SQL. The resolver must derive facts from an authoritative
// schema/model, not infer output columns from annotations in the request.
type PreparedColumnRequest struct {
	Component spec.Key
	View      spec.Key
	Name      string
	Namespace string
	Connector string
	Source    *spec.ViewSource
}

// PreparedColumnResult is the complete ordered result projection. Columns use
// vendor result names and canonical types/constraints. Table is optional for
// readers; when supplied, the SQL must prove that same direct physical source.
// This metadata does not attest that any deployed database has these columns.
type PreparedColumnResult struct {
	Columns []*spec.Column
	Table   string
}

type PreparedColumnResolver func(context.Context, PreparedColumnRequest) (*PreparedColumnResult, error)

// RefinePrepared shares ordinary annotation, source-identity and column-merge
// rules with database discovery, while preserving SQL and embedded resources.
// Only the caller's compilation graph is changed; returned metadata is cloned.
func RefinePrepared(ctx context.Context, component *spec.Component, resources fs.FS, resolve PreparedColumnResolver, resolver *typecatalog.Resolver) error {
	if component == nil || resolve == nil {
		return fmt.Errorf("prepared columns require a component and resolver")
	}
	inputs := newAnalysisInputs(component, resolver)
	visited := map[*spec.View]bool{}
	var visit func(*spec.View, string) error
	visit = func(view *spec.View, connector string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if view == nil || visited[view] {
			return nil
		}
		visited[view] = true
		if view.Source != nil {
			source := view.Source.Clone()
			if source.Bindings != nil && strings.TrimSpace(source.Bindings.Connector) != "" {
				connector = source.Bindings.Connector
			}
			if err := dsql.ResolveSource(view.Name, source, resources); err != nil {
				return err
			}
			if source.SQL == "" && source.Table != "" {
				source.SQL = "SELECT * FROM " + source.Table
			}
			if strings.TrimSpace(source.SQL) != "" {
				SQL, err := projectionAnalysisSQL(source.SQL, inputs)
				if err != nil {
					return fmt.Errorf("prepared projection %s: %w", view.Namespace, err)
				}
				metadata, err := resolve(ctx, PreparedColumnRequest{Component: component.Key, View: view.Key, Name: view.CanonicalName(), Namespace: view.Namespace, Connector: connector, Source: source.Clone()})
				if err != nil {
					return err
				}
				if err = ctx.Err(); err != nil {
					return err
				}
				if metadata == nil || len(metadata.Columns) == 0 {
					return fmt.Errorf("prepared projection %s is missing", view.Namespace)
				}
				columns := make([]*spec.Column, len(metadata.Columns))
				names := make([]string, len(columns))
				for i, field := range metadata.Columns {
					if field == nil || field.Type.IsZero() {
						return fmt.Errorf("prepared projection %s has an untyped column", view.Namespace)
					}
					columns[i], names[i] = field.Clone(), field.Name
					// Closed SQL projections remain authoritative even if a resolver
					// accidentally supplies a wider table/model schema.
					found, complete, e := (dsql.SelectorProjection{SQL: SQL}).HasOutput(field.Name)
					if e != nil {
						return e
					}
					if complete && !found {
						return fmt.Errorf("prepared column %s is absent from SQL projection", field.Name)
					}
				}
				if err = validateColumns("prepared", columns); err != nil {
					return err
				}
				if projected, e := (dsql.SelectorProjection{SQL: SQL}).Columns(nil); e == nil {
					if len(projected) != len(columns) {
						return fmt.Errorf("prepared projection %s is incomplete", view.Namespace)
					}
					for i, field := range projected {
						if !strings.EqualFold(field.OutputName(), names[i]) {
							return fmt.Errorf("prepared projection %s has a different column order", view.Namespace)
						}
					}
				}
				if metadata.Table != "" {
					if directSourceTable(SQL) != metadata.Table || source.Table != "" && source.Table != metadata.Table {
						return fmt.Errorf("prepared table %s does not match SQL source", metadata.Table)
					}
					source.Table = metadata.Table
				}
				source.SQL = SQL
				resultView := sqlResultView(view)
				identities, err := resolveResultSources(resultView.Columns, source)
				if err != nil {
					return err
				}
				if err = validateResultAnnotations(resultView, names, identities); err != nil {
					return err
				}
				if err = identities.validateResults(resultView.Columns, names); err != nil {
					return err
				}
				view.Columns = mergeColumnsWithSources(view.Columns, columns, identities)
				// Constraint flags are merged above. SQLX reference tags need the
				// same authored-precedence merge used by live table discovery.
				for _, merged := range view.Columns {
					for _, prepared := range columns {
						if !strings.EqualFold(merged.Source, prepared.Name) && !strings.EqualFold(merged.Name, prepared.Name) {
							continue
						}
						tag := sqlio.ParseTag(reflect.StructTag(prepared.Tag))
						if tag != nil && tag.RefTable != "" && tag.RefColumn != "" {
							applyReferenceConstraint(merged, &tableReference{schema: tag.RefDb, table: tag.RefTable, column: tag.RefColumn}, prepared.Name)
						}
					}
				}
				if metadata.Table != "" && view.Source.Table == "" {
					view.Source.Table, view.Source.TableInferred = metadata.Table, true
				}
			}
		}
		for _, relation := range view.Relations {
			if relation != nil {
				if err := visit(relation.View, connector); err != nil {
					return err
				}
			}
		}
		return nil
	}
	connector := ""
	if component.Settings != nil {
		connector = component.Settings.DefaultConnector
	}
	if err := visit(component.RootView, connector); err != nil {
		return err
	}
	for _, view := range component.Views {
		if err := visit(view, connector); err != nil {
			return err
		}
	}
	return nil
}
