package inference

import (
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
)

type ColumnParameterNamer func(column *Field) string

func ExtractColumnConfig(column *sqlparser.Column) (*view.ColumnConfig, error) {
	if column.Comments == "" {
		return nil, nil
	}
	columnConfig := &view.ColumnConfig{}
	if err := TryUnmarshalHint(column.Comments, columnConfig); err != nil {
		return nil, fmt.Errorf("invalid column %v settings: %w, %s", column.Name, err, column.Comments)
	}
	if columnConfig.DataType != nil {
		column.Type = *columnConfig.DataType
	}
	columnConfig.Name = column.Identity()
	columnConfig.Alias = column.Alias
	return columnConfig, nil
}
