package view

import (
	"context"
	"database/sql"
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
	"time"
)

func detectColumns(ctx context.Context, resource *Resource, SQL string, v *View, usePlaceholders bool) ([]*Column, error) {

	db, err := v.Connector.Db()

	var args []interface{}
	if usePlaceholders && v.Template != nil && v.Template.Schema == nil {
		totalLength := 0
		for _, parameter := range v.Template.Parameters {
			totalLength += len(parameter.Positions)
		}
		args = make([]interface{}, totalLength)
		for _, parameter := range v.Template.Parameters {
			if err = parameter.Init(ctx, v, resource, nil); err != nil {
				return nil, err
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
					value = time.Now()
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
		return nil, err
	}

	query, err := db.QueryContext(ctx, SQL, args...)
	if err != nil {
		return nil, err
	}

	types, err := query.ColumnTypes()
	if err != nil {
		return nil, err
	}

	ioColumns := io.TypesToColumns(types)
	columnsMetadata, err := columnsMetadata(ctx, db, v, ioColumns)
	if err != nil {
		return nil, err
	}
	return convertIoColumnsToColumns(v.exclude(ioColumns), columnsMetadata), nil
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
	SQL := "SELECT " + v.Alias + ".* FROM " + source + " " + v.Alias + " WHERE 1=0"
	if source != v.Name && source != v.Table {
		discover := metadata.EnrichWithDiscover(source, false)
		replacement := rdata.Map{}
		replacement.Put(keywords.AndCriteria[1:], " AND 1=0 ")
		replacement.Put(keywords.WhereCriteria[1:], " WHERE 1=0 ")
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
