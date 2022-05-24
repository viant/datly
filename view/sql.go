package view

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	rdata "github.com/viant/toolbox/data"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/parser"
	"strings"
)

func detectColumns(ctx context.Context, SQL string, v *View) ([]*Column, error) {
	parse, err := parser.Parse([]byte(SQL))
	if err != nil {
		return nil, err
	}

	replacement := rdata.Map{}
	for _, statement := range parse.Stmt {
		switch actual := statement.(type) {
		case *expr.Select:
			replacement.Put(actual.ID, "")
		}
	}

	SQL = replacement.ExpandAsText(SQL)

	db, err := v.Connector.Db()
	if err != nil {
		return nil, err
	}

	query, err := db.QueryContext(ctx, SQL)
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

func detectColumnsSQL(source string, v *View) string {
	return "SELECT " + v.Alias + ".* FROM " + source + " " + v.Alias + " WHERE 1=0"
}
