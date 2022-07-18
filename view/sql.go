package view

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/converter"
	"github.com/viant/datly/reader/metadata"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	rdata "github.com/viant/toolbox/data"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/parser"
	"reflect"
	"strings"
)

func DetectColumns(ctx context.Context, resource *Resource, v *View) ([]*Column, string, error) {
	actualSQL, evaluated, err := evaluateTemplateIfNeeded(ctx, resource, v)
	if err != nil {
		return nil, "", err
	}

	columns, SQL, err := detectColumns(ctx, resource, actualSQL, v, v.UseParamBindingPositions())
	if err != nil {
		if err != nil && !evaluated {
			fmt.Println(fmt.Errorf("failed to detect columns using velocity engine and SQL:  %v  due to the %w\n", SQL, err).Error())
			columns, SQL, err = detectColumns(ctx, resource, v.Source(), v, v.UseParamBindingPositions())
			if err != nil {
				return nil, "", fmt.Errorf("failed also to detect columns using %v due to the %w\n", SQL, err)
			}
		}

		return columns, SQL, err
	}

	if v.From != "" && v.Table != "" {
		tableColumns, tableSQL, errr := detectColumns(ctx, resource, v.Table, v, false)
		if errr != nil {
			return nil, tableSQL, errr
		}

		v.Logger.ColumnsDetection(tableSQL, v.Table)
		if err != nil {
			return nil, tableSQL, err
		}

		Columns(columns).updateTypes(tableColumns, v.Caser)
	}

	return columns, SQL, nil
}

func evaluateTemplateIfNeeded(ctx context.Context, resource *Resource, aView *View) (SQL string, evaluated bool, err error) {
	if aView.Template == nil {
		return aView.Source(), false, nil
	}

	if err := aView.Template.Init(ctx, resource, aView); err != nil {
		return "", false, err
	}

	params := newValue(aView.Template.Schema.Type())
	presence := newValue(aView.Template.PresenceSchema.Type())

	source, err := aView.Template.EvaluateSource(params, presence, aView)
	if err != nil {
		return "", false, err
	}

	source, err = expandWithZeroValues(source, aView.Template)
	if err != nil {
		return "", false, err
	}

	return source, false, err
}

func detectColumns(ctx context.Context, resource *Resource, SQL string, v *View, usePlaceholders bool) ([]*Column, string, error) {
	SQL, err := detectColumnsSQL(SQL, v)
	if err != nil {
		return nil, "", err
	}

	db, err := v.Connector.Db()

	var args []interface{}
	if usePlaceholders && v.Template != nil {
		totalLength := 0
		for _, parameter := range v.Template.Parameters {
			totalLength += len(parameter.Positions)
		}
		args = make([]interface{}, totalLength)
		for _, parameter := range v.Template.Parameters {
			if err = parameter.Init(ctx, v, resource, nil); err != nil {
				return nil, SQL, err
			}
			var value interface{}
			switch parameter.Schema.Type().Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				value = 0
			case reflect.Float32, reflect.Float64:
				value = 0.0
			case reflect.String:
				value = ""
			case reflect.Bool:
				value = false
			default:
				if parameter.Schema.Type() == converter.TimeType {
					value = Now()
				} else {
					value = reflect.New(parameter.Schema.Type()).Elem().Interface()
				}
			}

			for _, position := range parameter.Positions {
				args[position] = value
			}
		}
	}

	if err != nil {
		return nil, SQL, err
	}

	query, err := db.QueryContext(ctx, SQL, args...)
	if err != nil {
		return nil, SQL, err
	}

	types, err := query.ColumnTypes()
	if err != nil {
		return nil, SQL, err
	}

	ioColumns := io.TypesToColumns(types)
	columnsMetadata, err := columnsMetadata(ctx, db, v, ioColumns)
	if err != nil {
		return nil, SQL, err
	}

	columns := convertIoColumnsToColumns(v.exclude(ioColumns), columnsMetadata)
	return columns, SQL, nil
}

func columnsMetadata(ctx context.Context, db *sql.DB, v *View, columns []io.Column) (map[string]bool, error) {
	if v.Source() != v.Table && v.Table != "" {
		return nil, nil
	}

	if len(columns) > 0 {
		if _, ok := columns[0].Nullable(); ok {
			result := map[string]bool{}
			for _, column := range columns {
				result[column.Name()], _ = column.Nullable()
			}
			return result, nil
		}
	}

	session, err := config.Session(ctx, db)
	if err != nil {
		return nil, err
	}

	sinkColumns, err := config.Columns(ctx, session, db, v.Table)
	if err != nil {
		return nil, err
	}

	result := map[string]bool{}
	for _, column := range sinkColumns {
		result[column.Name] = strings.EqualFold(column.Nullable, "YES") || strings.EqualFold(column.Nullable, "1") || strings.EqualFold(column.Nullable, "TRUE")
	}

	return result, nil
}

func detectColumnsSQL(source string, v *View) (string, error) {
	sb := strings.Builder{}
	sb.WriteString("SELECT ")
	if v.Alias != "" {
		sb.WriteString(v.Alias)
		sb.WriteString(".")
	}
	sb.WriteString("* FROM ")
	sb.WriteString(source)
	sb.WriteString(" ")
	sb.WriteString(v.Alias)
	sb.WriteString(" WHERE 1=0")

	SQL := sb.String()
	if source != v.Name && source != v.Table {
		discover := metadata.EnrichWithDiscover(source, false)
		replacement := rdata.Map{}
		replacement.Put(keywords.AndCriteria[1:], "\n\n AND 1=0 ")
		replacement.Put(keywords.WhereCriteria[1:], "\n\n WHERE 1=0 ")
		SQL = replacement.ExpandAsText(discover)
	}

	parse, err := parser.Parse([]byte(SQL))
	if err != nil {
		return "", err
	}

	replacement := rdata.Map{}
	for _, statement := range parse.Stmt {
		switch actual := statement.(type) {
		case *expr.Select:
			replacement.Put(actual.ID, "")
		}
	}

	SQL = replacement.ExpandAsText(SQL)

	return SQL, nil
}

func expandWithZeroValues(SQL string, template *Template) (string, error) {
	expandMap := rdata.Map{}
	for _, parameter := range template.Parameters {
		var value interface{}
		paramType := parameter.Schema.Type()
		switch paramType.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			value = 0
		case reflect.Float32, reflect.Float64:
			value = 0.0
		case reflect.String:
			value = "''"
		case reflect.Bool:
			value = false
		default:
			value = reflect.New(paramType).Elem().Interface()
		}

		expandMap.SetValue(parameter.Name, value)
	}

	return expandMap.ExpandAsText(SQL), nil
}
