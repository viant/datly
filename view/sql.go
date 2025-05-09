package view

import (
	"context"
	"database/sql"
	"github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/service/reader/metadata"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	rdata "github.com/viant/toolbox/data"
	"reflect"
	"strings"
)

type (
	TemplateEvaluation struct {
		SQL        string
		Evaluated  bool
		Expander   ExpanderFn
		Args       []interface{}
		Parameters state.Parameters
	}

	ExpanderFn func(placeholders *[]interface{}, SQL string, selector *Statelet, params CriteriaParam, batchData *BatchData, sanitized *expand.DataUnit) (string, error)
)

func detectColumns(ctx context.Context, evaluation *TemplateEvaluation, v *View) ([]*Column, string, error) {
	SQL, args, err := detectColumnsSQL(evaluation, v)
	if err != nil {
		return nil, "", err
	}

	for i, parameter := range evaluation.Parameters {
		if strings.Contains(v.Template.Source, parameter.Name) {
			schema := parameter.Schema
			if i < len(args) {
				rType := schema.Type()
				if rType.Kind() == reflect.Ptr {
					rType = rType.Elem()
				}
				args[i] = reflect.New(rType).Elem().Interface()
			}
		}
	}

	aDb, err := v.Connector.DB()
	if err != nil {
		return nil, SQL, err
	}
	query, err := aDb.QueryContext(ctx, SQL, args...)
	if err != nil {
		v.Logger.LogDatabaseErr(SQL, err, args...)
		return nil, SQL, err
	}
	defer query.Close()

	types, err := query.ColumnTypes()
	if err != nil {
		return nil, SQL, err
	}

	ioColumns := io.TypesToColumns(types)
	columnsMetadata, err := columnsMetadata(ctx, aDb, v, ioColumns)
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

func detectColumnsSQL(evaluation *TemplateEvaluation, v *View) (string, []interface{}, error) {
	SQL := ensureSelectStatement(evaluation, v)

	var placeholders []interface{}
	var err error

	if evaluation.Expander != nil {
		SQL, err = v.Expand(&placeholders, SQL, &Statelet{}, CriteriaParam{}, &BatchData{}, NewMockSanitizer())
		if err != nil {
			return SQL, nil, err
		}
	}

	if len(placeholders) == 0 {
		placeholders = evaluation.Args
	} else {
		for i, arg := range evaluation.Args {
			if i < len(placeholders) {
				placeholders[i] = arg
			}
		}
	}

	return SQL, placeholders, nil
}

func ensureSelectStatement(evaluation *TemplateEvaluation, v *View) string {

	source := evaluation.SQL
	if source != v.Name && source != v.Table {
		if query, _ := sqlparser.ParseQuery(source); query != nil && query.From.X == nil {
			return wrapWithSelect(v, source)
		}

		return source
	}

	SQL := wrapWithSelect(v, source)
	return SQL
}

func wrapWithSelect(v *View, source string) string {
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
	return SQL
}

func NewMockSanitizer() *expand.DataUnit {
	return &expand.DataUnit{
		Mock: true,
	}
}

func ExpandWithFalseCondition(source string) string {
	discover := metadata.EnrichWithDiscover(source, false)
	replacement := rdata.Map{}
	replacement.Put(keywords.AndCriteria[1:], "\n\n AND 1=0 ")
	replacement.Put(keywords.WhereCriteria[1:], "\n\n WHERE 1=0 ")
	SQL := replacement.ExpandAsText(discover)
	return SQL
}

func expandWithZeroValues(SQL string, template *Template) (string, error) {
	expandMap := rdata.Map{}
	for _, parameter := range template.Parameters {
		var value interface{}
		paramType := parameter.OutputType()
		for paramType.Kind() == reflect.Ptr || paramType.Kind() == reflect.Slice {
			paramType = paramType.Elem()
		}

		switch paramType.Kind() {
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
			value = reflect.New(paramType).Elem().Interface()
		}

		expandMap.SetValue(parameter.Name, value)
	}

	return expandMap.ExpandAsText(SQL), nil
}
