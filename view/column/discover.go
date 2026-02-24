package column

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/types"
	dconfig "github.com/viant/datly/view/extension"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"reflect"

	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/xreflect"
	"strings"
)

func Discover(ctx context.Context, db *sql.DB, table, SQL string, SQLArgs ...interface{}) (sqlparser.Columns, error) {
	SQL = strings.ReplaceAll(SQL, "$AND_CRITERIA", "")
	SQL = strings.ReplaceAll(SQL, "$WHERE_CRITERIA", "")
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
		sinkColumns, e := readSinkColumns(ctx, db, table)
		if e != nil {
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
		return queryColumns, fmt.Errorf("failed to discover/detect column: %w %s %v", err, SQL, SQLArgs)
	}
	if queryColumns.IsStarExpr() {
		return asColumns(sqlColumns), nil
	}
	updatedMatchedColumn(&queryColumns, sqlColumns)
	types := dconfig.Config.Types
	for _, column := range queryColumns {
		if err := ExtractColumnConfig(column, types); err != nil {
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

func ExtractColumnConfig(column *sqlparser.Column, typesRegistry *xreflect.Types) error {
	if column.Comments == "" {
		return nil
	}
	dataType := &typeInfo{}
	if err := shared.UnmarshalWithExt([]byte(column.Comments), dataType, ".json"); err != nil {
		return fmt.Errorf("invalid column %v settings: %w, %s", column.Name, err, column.Comments)
	}
	if dataType.DataType != nil {
		original := column.Type
		column.Type = *dataType.DataType
		if original != column.Type {
			rType, err := types.LookupType(typesRegistry.Lookup, column.Type)
			if err != nil {
				return fmt.Errorf("invalud column %v data type: %s, %w", column.Name, column.Type, err)
			}
			column.RawType = rType
		}
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

				if rType := item.ScanType(); rType != nil {
					sinkColumn.TypeDefinition = rType.String()
				}
				if sinkColumn.Type == "" {
					if itemType := item.ScanType(); itemType != nil {
						if itemType.Kind() == reflect.Pointer {
							itemType = itemType.Elem()
						}
						sinkColumn.Type = itemType.Name()
					}

					if sinkColumn.Type == "" {
						return nil, fmt.Errorf("unable discover column %v type", item.Name())
					}
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
	if strings.ToLower(queryColumn.Type) == "record" {
		queryColumn.Type = fromColumn.TypeDefinition
	}
}

func readSinkColumns(ctx context.Context, db *sql.DB, table string) ([]sink.Column, error) {
	session, err := config.Session(ctx, db)
	if err != nil {
		return nil, err
	}
	columns, vErr := config.Columns(ctx, session, db, table)
	if len(columns) == 0 && table != "" {
		SQL := "SELECT * FROM " + table + " WHERE 1 = 0"
		var args []interface{}
		shared.EnsureArgs(SQL, &args)
		columns, err = inferColumnWithSQL(ctx, db, SQL, args, map[string]sink.Column{})
	}
	if len(columns) == 0 {
		return nil, vErr
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
		// For CTE-backed queries (WITH ...), SELECT * FROM cte_alias must still be
		// resolved via SQL execution; the alias is not a physical table.
		if sqlQuery.List.IsStarExpr() && !strings.Contains(table, "SELECT") && len(sqlQuery.WithSelects) == 0 {
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
		if sqlQuery.From.Alias == "" && sqlQuery.From.X != nil {
			if _, ok := sqlQuery.From.X.(*expr.Ident); !ok {
				sqlQuery.From.Alias = "t"
			}
		}
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
