package inference

import (
	"context"
	"database/sql"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/sink"
	"strings"
)

type ColumnParameterNamer func(column *Field) string

func detectColumns(ctx context.Context, db *sql.DB, SQL, table string, SQLArgs ...interface{}) (sqlparser.Columns, error) {
	SQL = TrimParenthesis(SQL)
	extractedTable, SQL, queryColumns := parseQuery(SQL)
	if SQL == "" {
		return nil, nil
	}
	var byName = map[string]sink.Column{}
	if extractedTable = strings.TrimSpace(extractedTable); extractedTable != "" {
		table = extractedTable
	}
	if table != "" && !strings.Contains(table, " ") {
		if sinkColumns, _ := readSinkColumns(ctx, db, table); len(sinkColumns) > 0 {
			byName = sink.Columns(sinkColumns).By(sink.ColumnName.Key)
		}
	}
	stmt, err := db.PrepareContext(ctx, SQL)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx, SQLArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tableColumns []sink.Column
	if rows != nil {
		columnsTypes, err := rows.ColumnTypes()
		if err != nil {
			return nil, err
		}
		if len(columnsTypes) != 0 {
			columns := io.TypesToColumns(columnsTypes)
			for _, item := range columns {
				sinkColumn := sink.Column{
					Name: item.Name(),
					Type: item.DatabaseTypeName(),
				}
				if match, ok := byName[sink.ColumnName.Key(&sinkColumn)]; ok {
					sinkColumn = match
					tableColumns = append(tableColumns, sinkColumn)
					continue
				}

				if nullable, ok := item.Nullable(); ok && nullable {
					sinkColumn.Nullable = "1"
				}
				if length, ok := item.Length(); ok {
					sinkColumn.Length = &length
				}
				tableColumns = append(tableColumns, sinkColumn)
			}
		}
	}

	if queryColumns.IsStarExpr() {
		return asColumns(tableColumns), nil
	}
	updatedMatchedColumn(queryColumns, tableColumns)
	return queryColumns, nil
}

func updatedMatchedColumn(queryColumns sqlparser.Columns, tableColumns []sink.Column) {
	byName := sink.Columns(tableColumns).By(sink.ColumnName.Key)

	for i, column := range queryColumns {
		queryColumn := queryColumns[i]
		if matched, ok := byName[strings.ToLower(column.Alias)]; ok && column.Alias != "" {
			updateQueryColumn(queryColumn, matched)
			continue
		}
		if matched, ok := byName[strings.ToLower(column.Name)]; ok && column.Name != "" {
			updateQueryColumn(queryColumn, matched)
			continue
		}
		updateQueryColumn(queryColumn, tableColumns[i])
	}
}

func updateQueryColumn(queryColumn *sqlparser.Column, fromColumn sink.Column) {
	queryColumn.Type = fromColumn.Type
	queryColumn.Length = fromColumn.Length
	queryColumn.IsNullable = fromColumn.IsNullable()
	queryColumn.IsAutoincrement = fromColumn.Autoincrement()
	queryColumn.IsUnique = fromColumn.IsUnique()
	queryColumn.Default = fromColumn.Default
}

func readSinkColumns(ctx context.Context, db *sql.DB, table string) ([]sink.Column, error) {
	session, err := config.Session(ctx, db)
	if err != nil {
		return nil, err
	}
	return config.Columns(ctx, session, db, table)
}

func parseQuery(SQL string) (string, string, sqlparser.Columns) {
	sqlQuery, _ := sqlparser.ParseQuery(SQL)
	var table string
	var queryColumn sqlparser.Columns
	if sqlQuery != nil {
		queryColumn = sqlparser.NewColumns(sqlQuery.List)
		table = sqlparser.Stringify(sqlQuery.From.X)
		if sqlQuery.List.IsStarExpr() && !strings.Contains(table, "SELECT") {
			return table, "", nil //use table metadata
		}
		sqlQuery.Limit = nil
		sqlQuery.Offset = nil
		SQL = sqlparser.Stringify(sqlQuery)
		SQL += " LIMIT 1"
	}
	return table, SQL, queryColumn
}

func asColumns(sinkColumns []sink.Column) sqlparser.Columns {
	var result sqlparser.Columns
	for _, column := range sinkColumns {
		result = append(result, asColumn(column))
	}
	return result
}

func asColumn(column sink.Column) *sqlparser.Column {
	ret := &sqlparser.Column{Name: column.Name}
	updateQueryColumn(ret, column)
	return ret
}
