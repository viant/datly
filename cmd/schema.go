package cmd

import (
	"context"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/view"
	"strings"
)

func (s *serverBuilder) BuildSchema(ctx context.Context, schemaName, paramName string, tableParam *option.TableParam, routeOption *option.Route) (*view.Definition, bool) {
	table := tableParam.Table
	s.mergeTypes(routeOption, table)

	if len(table.Inner) > 0 && isMetaTemplate(table.SQL) {
		return s.buildSchemaFromTable(ctx, schemaName, table)
	}

	if paramType, ok := routeOption.Declare[paramName]; ok {
		return s.buildSchemaFromParamType(schemaName, paramType)
	}

	return nil, false
}

func (s *serverBuilder) mergeTypes(routeOption *option.Route, table *option.Table) {
	if len(routeOption.Declare) == 0 {
		return
	}

	if table.ColumnTypes == nil {
		table.ColumnTypes = map[string]string{}
	}

	for k, v := range routeOption.Declare {
		table.ColumnTypes[k] = v
	}
}

func (s *serverBuilder) buildSchemaFromTable(ctx context.Context, schemaName string, table *option.Table) (*view.Definition, bool) {
	var fields = make([]*view.Field, 0)
	s.updateTableColumnTypes(ctx, table)
	for _, column := range table.Inner {
		fieldName := column.Alias
		if fieldName == "" {
			fieldName = column.Name
		}

		if fieldName == "" {
			continue
		}

		dataType := column.DataType
		if dataType == "" {
			dataType = table.ColumnTypes[strings.ToLower(fieldName)]
		}

		if dataType == "" {
			dataType = "string"
		}

		fields = append(fields, &view.Field{
			Name:   fieldName,
			Embed:  false,
			Schema: &view.Schema{DataType: dataType},
		})
	}

	return &view.Definition{
		Name:   schemaName,
		Fields: fields,
		Schema: nil,
	}, true
}
