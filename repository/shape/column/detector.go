package column

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/view"
	viewcolumn "github.com/viant/datly/view/column"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlx/io"
)

// Detector resolves columns for shape-generated views.
//
// Rules:
//   - schema field order is canonical order
//   - wildcard SQL always performs DB discovery
//   - newly discovered columns are appended at the end
//   - matched columns keep schema order but refresh metadata from DB
type Detector struct{}

func New() *Detector {
	return &Detector{}
}

func (d *Detector) Resolve(ctx context.Context, resource *view.Resource, aView *view.View) (view.Columns, error) {
	if aView == nil {
		return nil, fmt.Errorf("shape column detector: nil view")
	}

	base := columnsFromSchema(aView)
	if !usesWildcard(aView) {
		return base, nil
	}

	discovered, err := d.detect(ctx, resource, aView)
	if err != nil {
		return nil, err
	}
	if len(base) == 0 {
		return discovered, nil
	}
	return mergePreservingOrder(base, discovered), nil
}

func (d *Detector) detect(ctx context.Context, resource *view.Resource, aView *view.View) (view.Columns, error) {
	connector, err := lookupConnector(ctx, resource, aView)
	if err != nil {
		return nil, err
	}
	db, err := connector.DB()
	if err != nil {
		return nil, fmt.Errorf("shape column detector: failed to open db for view %s: %w", aView.Name, err)
	}
	query := sourceSQL(aView)
	sqlColumns, err := viewcolumn.Discover(ctx, db, aView.Table, query)
	if err != nil {
		return nil, fmt.Errorf("shape column detector: discover failed for view %s: %w", aView.Name, err)
	}
	return view.NewColumns(sqlColumns, aView.ColumnsConfig), nil
}

func lookupConnector(ctx context.Context, resource *view.Resource, aView *view.View) (*view.Connector, error) {
	if resource == nil {
		return nil, fmt.Errorf("shape column detector: missing resource for view %s", aView.Name)
	}
	if aView.Connector == nil {
		return nil, fmt.Errorf("shape column detector: missing connector for wildcard view %s", aView.Name)
	}
	connectors := view.ConnectorSlice(resource.Connectors).Index()
	connector := aView.Connector
	if connector.Ref != "" {
		lookup, err := connectors.Lookup(connector.Ref)
		if err != nil {
			return nil, fmt.Errorf("shape column detector: connector ref %s for view %s: %w", connector.Ref, aView.Name, err)
		}
		connector = lookup
	}
	if err := connector.Init(ctx, connectors); err != nil {
		return nil, fmt.Errorf("shape column detector: connector init for view %s: %w", aView.Name, err)
	}
	return connector, nil
}

func sourceSQL(aView *view.View) string {
	if aView.Template != nil && strings.TrimSpace(aView.Template.Source) != "" {
		return aView.Template.Source
	}
	return aView.Source()
}

func usesWildcard(aView *view.View) bool {
	if aView != nil && aView.Template == nil && strings.TrimSpace(aView.Table) != "" {
		return true
	}
	query := sourceSQL(aView)
	trimmed := strings.TrimSpace(strings.ToLower(query))
	if trimmed == "" {
		return false
	}
	if !strings.Contains(trimmed, "*") {
		return false
	}
	if !strings.HasPrefix(trimmed, "select") && !strings.HasPrefix(trimmed, "with") {
		return true
	}
	parsed, err := sqlparser.ParseQuery(query)
	if err != nil {
		return true
	}
	return sqlparser.NewColumns(parsed.List).IsStarExpr()
}

func columnsFromSchema(aView *view.View) view.Columns {
	if aView == nil || aView.Schema == nil {
		return nil
	}
	rType := aView.Schema.Type()
	if rType == nil {
		return nil
	}
	for rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice {
		rType = rType.Elem()
	}
	if rType.Kind() != reflect.Struct {
		return nil
	}
	result := make(view.Columns, 0, rType.NumField())
	appendSchemaColumns(rType, "", &result)
	return result
}

func appendSchemaColumns(rType reflect.Type, ns string, columns *view.Columns) {
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		if field.PkgPath != "" { // unexported
			continue
		}
		if field.Anonymous {
			inner := field.Type
			for inner.Kind() == reflect.Ptr {
				inner = inner.Elem()
			}
			if inner.Kind() == reflect.Struct {
				appendSchemaColumns(inner, ns, columns)
			}
			continue
		}

		tag := io.ParseTag(field.Tag)
		if tag != nil && tag.Transient {
			continue
		}

		name := field.Name
		if tag != nil && tag.Column != "" {
			name = tag.Column
		}
		if tag != nil && tag.Ns != "" {
			name = tag.Ns + name
		} else if ns != "" {
			name = ns + name
		}

		columnType := field.Type
		nullable := false
		if columnType.Kind() == reflect.Ptr {
			nullable = true
			columnType = columnType.Elem()
		}
		*columns = append(*columns, view.NewColumn(name, columnType.String(), columnType, nullable, view.WithColumnTag(string(field.Tag))))
	}
}

func mergePreservingOrder(base, discovered view.Columns) view.Columns {
	if len(base) == 0 {
		return discovered
	}
	if len(discovered) == 0 {
		return base
	}
	seen := map[string]*view.Column{}
	for _, item := range discovered {
		if item == nil {
			continue
		}
		seen[strings.ToLower(item.Name)] = item
	}
	result := make(view.Columns, 0, len(base)+len(discovered))
	for _, item := range base {
		if item == nil {
			continue
		}
		if fresh, ok := seen[strings.ToLower(item.Name)]; ok {
			delete(seen, strings.ToLower(item.Name))
			// Keep schema name/order but refresh discovered metadata.
			item.DataType = firstNonEmpty(fresh.DataType, item.DataType)
			item.SetColumnType(firstType(fresh.ColumnType(), item.ColumnType()))
			item.Nullable = fresh.Nullable
			if item.DatabaseColumn == "" {
				item.DatabaseColumn = fresh.DatabaseColumn
			}
		}
		result = append(result, item)
	}
	for _, item := range discovered {
		if item == nil {
			continue
		}
		if _, ok := seen[strings.ToLower(item.Name)]; !ok {
			continue
		}
		result = append(result, item)
		delete(seen, strings.ToLower(item.Name))
	}
	return result
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func firstType(values ...reflect.Type) reflect.Type {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}
