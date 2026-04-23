package inference

import (
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"strconv"
	"strings"
)

type ColumnParameterNamer func(column *Field) string

func ExtractColumnConfig(column *sqlparser.Column, groupable bool) (*view.ColumnConfig, error) {
	if column.Comments == "" && !groupable {
		return nil, nil
	}
	columnConfig := &view.ColumnConfig{}
	if column.Comments != "" {
		if err := TryUnmarshalHint(column.Comments, columnConfig); err != nil {
			return nil, fmt.Errorf("invalid column %v settings: %w, %s", column.Name, err, column.Comments)
		}
	}
	if groupable && columnConfig.Groupable == nil {
		columnConfig.Groupable = &groupable
	}
	if columnConfig.DataType != nil {
		column.Type = *columnConfig.DataType
	}
	columnConfig.Name = column.Identity()
	columnConfig.Alias = column.Alias
	return columnConfig, nil
}

func GroupableColumns(aQuery *query.Select, columns sqlparser.Columns) map[string]bool {
	result := make(map[string]bool)
	if aQuery == nil || len(aQuery.GroupBy) == 0 || len(columns) == 0 {
		return result
	}

	index := map[string]*sqlparser.Column{}
	for _, column := range columns {
		if column == nil {
			continue
		}
		for _, key := range columnGroupableKeys(column) {
			index[key] = column
		}
	}

	for _, item := range aQuery.GroupBy {
		for _, column := range groupByColumns(item, columns, index) {
			result[column.Identity()] = true
		}
	}
	return result
}

func groupByColumns(item *query.Item, columns sqlparser.Columns, index map[string]*sqlparser.Column) []*sqlparser.Column {
	if item == nil || item.Expr == nil {
		return nil
	}

	if literal, ok := item.Expr.(*expr.Literal); ok {
		if position, err := strconv.Atoi(strings.TrimSpace(literal.Value)); err == nil && position > 0 && position <= len(columns) {
			return []*sqlparser.Column{columns[position-1]}
		}
	}

	key := normalizedGroupableKey(sqlparser.Stringify(item.Expr))
	if key == "" {
		return nil
	}
	if column, ok := index[key]; ok {
		return []*sqlparser.Column{column}
	}
	return nil
}

func columnGroupableKeys(column *sqlparser.Column) []string {
	result := make([]string, 0, 4)
	appendKey := func(value string) {
		key := normalizedGroupableKey(value)
		if key == "" {
			return
		}
		for _, existing := range result {
			if existing == key {
				return
			}
		}
		result = append(result, key)
	}

	appendKey(column.Identity())
	appendKey(column.Name)
	if column.Namespace != "" && column.Name != "" {
		appendKey(column.Namespace + "." + column.Name)
	}
	appendKey(column.Expression)
	return result
}

func normalizedGroupableKey(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return strings.ToLower(value)
}
