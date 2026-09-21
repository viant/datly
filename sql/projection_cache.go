package sql

import (
	"fmt"
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlx/io/read/cache"
)

// CacheProjection supplies native replay metadata from the already-rendered
// SQL. It reuses the selector grouping owner; it does not authorize columns or
// change SQL. Opaque outer expressions use declared dimension metadata, or are
// conservatively dimensions when that authority is absent.
type CacheProjection struct {
	SQL  string
	View *data.View
}

func (p CacheProjection) Fields() ([]cache.ProjectionField, error) {
	statement, err := sqlparser.ParseQuery(p.SQL)
	if err != nil {
		return nil, err
	}
	if statement == nil || len(statement.List) == 0 {
		return nil, fmt.Errorf("cache projection requires an explicit select list")
	}
	hasAggregate, declaredDimensions := false, false
	for _, item := range statement.List {
		hasAggregate = hasAggregate || isAggregateSelectItem(item)
	}
	if p.View != nil {
		for _, column := range p.View.Columns {
			declaredDimensions = declaredDimensions || column != nil && column.Groupable
		}
	}
	fields := make([]cache.ProjectionField, 0, len(statement.List))
	for _, item := range statement.List {
		name, err := setProjectionOutputName(item, sqlparser.Stringify(item))
		if err != nil {
			return nil, err
		}
		if name == "" || strings.Contains(name, "*") {
			return nil, fmt.Errorf("cache projection requires explicit column identities")
		}
		name = strings.Trim(name, "`\"")
		expression := normalizeExpression(sqlparser.Stringify(item.Expr))
		field := cache.ProjectionField{Name: name, ColumnName: name}
		measure := isAggregateSelectItem(item)
		if !hasAggregate && declaredDimensions {
			for _, column := range p.View.Columns {
				if column != nil && (strings.EqualFold(name, column.Name) || strings.EqualFold(name, column.Column)) {
					measure = !column.Groupable
					break
				}
			}
		}
		if measure {
			field.MeasureKey = expression
		} else {
			field.DimensionKey = projectionDimensionKey(name, expression)
		}
		fields = append(fields, field)
	}
	return fields, nil
}

func projectionDimensionKey(name, expression string) string {
	name = strings.TrimSpace(name)
	if name != "" {
		return strings.ToLower(name) + "\x00" + expression
	}
	return expression
}
