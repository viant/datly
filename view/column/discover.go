package column

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/sink"
	"strings"
)

func Discover(ctx context.Context, db *sql.DB, table, SQL string, SQLArgs ...interface{}) (sqlparser.Columns, error) {
	var columns sqlparser.Columns
	var err error
	if table == SQL && !strings.Contains(strings.ToLower(SQL), "select") {
		SQL = "SELECT * FROM " + table + " WHERE 1 = 0"
	}
	if SQL != "" {
		if columns, err = detectColumns(ctx, db, SQL, table, SQLArgs...); err != nil {
			return columns, err
		}
	}
	if len(columns) == 0 && table != "" { //TODO mere column types
		sinkColumns, err := readSinkColumns(ctx, db, table)
		if err != nil {
			return nil, err
		}
		columns = asColumns(sinkColumns)
	}
	return columns, nil
}

func detectColumns(ctx context.Context, db *sql.DB, SQL, table string, SQLArgs ...interface{}) (sqlparser.Columns, error) {
	var byName = map[string]sink.Column{}
	SQL = shared.TrimPair(SQL, '(', ')')
	if isWithQuery(SQL) {
		sqlColumns, err := inferColumnWithSQL(ctx, db, SQL, SQLArgs, byName)
		if err == nil {
			return asColumns(sqlColumns), nil
		}
	}

	var queryColumns sqlparser.Columns
	var extractedTable string

	extractedTable, SQL, queryColumns = parseQuery(SQL)
	if SQL == "" {
		return nil, nil
	}

	if extractedTable = strings.TrimSpace(extractedTable); extractedTable != "" {
		table = extractedTable
	}
	if table != "" && !strings.Contains(table, " ") {
		if sinkColumns, _ := readSinkColumns(ctx, db, table); len(sinkColumns) > 0 {
			byName = sink.Columns(sinkColumns).By(sink.ColumnName.Key)
		}
	}

	sqlColumns, err := inferColumnWithSQL(ctx, db, SQL, SQLArgs, byName)
	if err != nil {
		return queryColumns, fmt.Errorf("failed to detect column: %w %s %v", err, SQL, SQLArgs)
	}
	if queryColumns.IsStarExpr() {
		return asColumns(sqlColumns), nil
	}
	updatedMatchedColumn(&queryColumns, sqlColumns)
	for _, column := range queryColumns {
		if err := ExtractColumnConfig(column); err != nil {
			return nil, err
		}
	}
	return queryColumns, nil
}

func isWithQuery(SQL string) bool {
	return "with" == strings.ToLower(SQL[:4])
}

type typeInfo struct {
	DataType *string
}

func ExtractColumnConfig(column *sqlparser.Column) error {
	if column.Comments == "" {
		return nil
	}
	dataType := &typeInfo{}
	if err := shared.UnmarshalWithExt([]byte(column.Comments), dataType, ".json"); err != nil {
		return fmt.Errorf("invalid column %v settings: %w, %s", column.Name, err, column.Comments)
	}
	if dataType.DataType != nil {
		column.Type = *dataType.DataType
	}
	return nil
}

func inferColumnWithSQL(ctx context.Context, db *sql.DB, SQL string, SQLArgs []interface{}, byName map[string]sink.Column) ([]sink.Column, error) {
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
	return tableColumns, nil
}

func updatedMatchedColumn(queryColumns *sqlparser.Columns, tableColumns []sink.Column) {
	byName := sink.Columns(tableColumns).By(sink.ColumnName.Key)
	var columns sqlparser.Columns
	hasWildCard := false
	for i, column := range *queryColumns {
		if strings.Contains(column.Expression, "*") {
			hasWildCard = true
			continue
		}
		queryColumn := (*queryColumns)[i]
		columns = append(columns, queryColumn)
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
	if hasWildCard {
		namedColumn := columns.ByLowerCase()
		for _, tableColumn := range tableColumns {
			if _, ok := namedColumn[strings.ToLower(tableColumn.Name)]; ok {
				continue
			}
			columns = append(columns, asColumn(tableColumn))
		}
		*queryColumns = columns
	}
}

func updateQueryColumn(queryColumn *sqlparser.Column, fromColumn sink.Column) {
	queryColumn.Type = fromColumn.Type
	queryColumn.Length = fromColumn.Length
	queryColumn.IsNullable = fromColumn.IsNullable()
	queryColumn.IsAutoincrement = fromColumn.Autoincrement()
	queryColumn.IsUnique = fromColumn.IsUnique()
	queryColumn.Default = fromColumn.Default
	queryColumn.RawType, _ = io.ParseType(fromColumn.Type)
}

func readSinkColumns(ctx context.Context, db *sql.DB, table string) ([]sink.Column, error) {
	session, err := config.Session(ctx, db)
	if err != nil {
		return nil, err
	}
	columns, err := config.Columns(ctx, session, db, table)
	if len(columns) == 0 && table != "" {
		if columns, e := inferColumnWithSQL(ctx, db, "SELECT * FROM "+table+" WHERE 1 = 0", []interface{}{}, map[string]sink.Column{}); e == nil {
			return columns, err
		}
	}
	return columns, err
}

func parseQuery(SQL string) (string, string, sqlparser.Columns) {
	sqlQuery, _ := sqlparser.ParseQuery(SQL)
	var table string
	var queryColumn sqlparser.Columns
	if sqlQuery != nil {
		queryColumn = sqlparser.NewColumns(sqlQuery.List)
		if sqlQuery.From.X != nil {
			table = sqlparser.Stringify(sqlQuery.From.X)
		}
		if sqlQuery.List.IsStarExpr() && !strings.Contains(table, "SELECT") {
			return table, "", nil //use table metadata
		}
		sqlQuery.Limit = nil
		if table != "" {
			if sqlQuery.Qualify == nil || sqlQuery.Qualify.X == nil {
				sqlQuery.Qualify = &expr.Qualify{X: falsePredicate()}
			} else {
				sqlQuery.Qualify = &expr.Qualify{
					X: &expr.Binary{
						X:  falsePredicate(),
						Op: "AND",
						Y:  sqlQuery.Qualify.X,
					}}
			}
		}
		sqlQuery.Offset = nil
		SQL = sqlparser.Stringify(sqlQuery)
		if table != "" {
			SQL += " LIMIT 1"
		}
	}
	return table, SQL, queryColumn
}

func falsePredicate() *expr.Binary {
	return &expr.Binary{X: &expr.Literal{Value: "1"}, Op: "=", Y: &expr.Literal{Value: "0"}}
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

func RewriteWithQueryIfNeeded(SQL string, query *query.Select) (*query.Select, error) {
	var err error
	if strings.HasPrefix(strings.ToLower(SQL[:5]), "with") {
		SQL = sqlparser.Stringify(query)
		query, err = sqlparser.ParseQuery(SQL)
	}
	return query, err
}
