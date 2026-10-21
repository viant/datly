package column

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
)

// PreparedColumn is a result column from an authoritative offline schema/model.
// Name is the vendor result name (including SQL aliases), not a Go field name.
// Type must be concrete; SQL annotations alone are not an authoritative schema.
// Value fields and immutable reflect.Type make metadata safe to copy.
type PreparedColumn struct {
	Name     string
	Type     reflect.Type
	Nullable bool
	Tag      string
}

// PreparedRequest identifies the exact result being compiled. SQL has local
// embedded sources and constants expanded; ExecutableSQL is the unchanged source.
// The provider is trusted Go integration code, never a runtime request field.
type PreparedRequest struct {
	Component     string
	View          string
	Connector     string
	SQL           string
	ExecutableSQL string
}

// PreparedResult must describe the complete, ordered result projection. Table is
// optional and accepted only when SQL proves that direct physical source.
type PreparedResult struct {
	Columns []PreparedColumn
	Table   string
}

type PreparedResolver func(context.Context, PreparedRequest) (*PreparedResult, error)

// ResolvePrepared checks the trusted provider's result without opening a DB or
// falling back to discovery. Neither the request nor the result can mutate SQL.
func ResolvePrepared(ctx context.Context, request PreparedRequest, resolve PreparedResolver) ([]PreparedColumn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if resolve == nil {
		return nil, fmt.Errorf("prepared columns: missing resolver")
	}
	parsed, err := sqlparser.ParseQuery(request.SQL)
	if err != nil || parsed == nil {
		return nil, fmt.Errorf("prepared columns %s: cannot parse projection: %v", request.View, err)
	}
	if err := validateStaticProjection(parsed); err != nil {
		return nil, err
	}
	result, err := resolve(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("prepared columns %s: %w", request.View, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if result == nil || len(result.Columns) == 0 {
		return nil, fmt.Errorf("prepared projection %s is missing", request.View)
	}
	columns := append([]PreparedColumn(nil), result.Columns...)
	seen := map[string]bool{}
	for _, col := range columns {
		key := strings.ToLower(strings.TrimSpace(col.Name))
		if key == "" || key != strings.ToLower(col.Name) || seen[key] {
			return nil, fmt.Errorf("prepared projection %s has an empty or duplicate column %q", request.View, col.Name)
		}
		seen[key] = true
		typ := col.Type
		for typ != nil && typ.Kind() == reflect.Ptr {
			typ = typ.Elem()
		}
		if typ == nil || typ.Kind() == reflect.Interface {
			return nil, fmt.Errorf("prepared projection %s has an untyped column %s", request.View, col.Name)
		}
	}
	projected, closed, err := preparedProjection(parsed)
	if err != nil {
		return nil, err
	}
	if closed {
		if len(projected) != len(columns) {
			return nil, fmt.Errorf("prepared projection %s has %d columns; SQL requires %d", request.View, len(columns), len(projected))
		}
		for i, name := range projected {
			if !strings.EqualFold(name, columns[i].Name) {
				return nil, fmt.Errorf("prepared projection %s column %d is %q; SQL requires %q in that order", request.View, i+1, columns[i].Name, name)
			}
		}
	}
	if result.Table != "" {
		table := strings.Trim(sqlparser.Stringify(parsed.From.X), "`\"")
		if parsed.NestedSelect() != nil || len(parsed.Joins) != 0 || len(parsed.WithSelects) != 0 || parsed.Union != nil || table != result.Table {
			return nil, fmt.Errorf("prepared table %q does not match a direct SQL source", result.Table)
		}
	}
	return columns, nil
}

func validateStaticProjection(q *query.Select) error {
	if q == nil {
		return nil
	}
	for _, item := range q.List {
		if item == nil {
			return fmt.Errorf("prepared columns: missing projection item")
		}
		expr := sqlparser.Stringify(item.Expr)
		// Dynamic projection expressions cannot be proven from one prepared shape.
		if strings.ContainsAny(expr, "$#") || strings.ContainsAny(item.Alias, "$#") {
			return fmt.Errorf("prepared columns: dynamic projection is unsupported")
		}
	}
	if err := validateStaticProjection(q.NestedSelect()); err != nil {
		return err
	}
	for _, cte := range q.WithSelects {
		if err := validateStaticProjection(cte.X); err != nil {
			return err
		}
	}
	if q.Union != nil {
		return validateStaticProjection(q.Union.X)
	}
	return nil
}

func preparedProjection(q *query.Select) ([]string, bool, error) {
	columns := sqlparser.NewColumns(q.List)
	if len(columns) == 0 {
		return nil, false, fmt.Errorf("prepared columns: empty SQL projection")
	}
	hasWildcard := false
	for _, item := range q.List {
		if (query.List{item}).IsStarExpr() {
			hasWildcard = true
		}
	}
	if hasWildcard {
		if len(columns) != 1 {
			return nil, false, fmt.Errorf("prepared columns: mixed wildcard projections are unsupported")
		}
		if len(columns[0].Except) > 0 {
			return nil, false, fmt.Errorf("prepared columns: wildcard exclusions are unsupported")
		}
		// A sole wildcard over a closed subquery/CTE is still a closed projection.
		if len(columns) == 1 && len(q.Joins) == 0 {
			if nested := q.NestedSelect(); nested != nil {
				return preparedProjection(nested)
			}
			table := strings.Trim(sqlparser.Stringify(q.From.X), "`\"")
			if cte := q.WithSelects.Select(table); cte != nil {
				return preparedProjection(cte.X)
			}
		}
		return nil, false, nil
	}
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = strings.Trim(col.Identity(), "`\"")
		if names[i] == "" {
			return nil, false, fmt.Errorf("prepared columns: expression requires an explicit result alias")
		}
	}
	return names, true, nil
}

// MaterializePrepared creates fresh runtime columns from validated metadata.
func MaterializePrepared(columns []PreparedColumn) view.Columns {
	result := make(view.Columns, len(columns))
	for i, col := range columns {
		result[i] = view.NewColumn(col.Name, col.Type.String(), col.Type, col.Nullable, view.WithColumnTag(col.Tag))
	}
	return result
}
