package data

import (
	"context"
	"github.com/viant/datly/shared"
	"github.com/viant/sqlx/io"
	"strings"
)

func detectColumns(ctx context.Context, SQL string, v *View) ([]*Column, error) {
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

	return convertIoColumnsToColumns(v.exclude(io.TypesToColumns(types))), nil
}

func detectColumnsSQL(source string, v *View) string {
	if strings.Contains(source, string(shared.Criteria)) {
		if v.hasWhereClause {
			source = strings.ReplaceAll(source, string(shared.Criteria), " AND 1 = 0")
		} else {
			source = strings.ReplaceAll(source, string(shared.Criteria), " WHERE 1 = 0")
		}
	}

	if strings.Contains(source, string(shared.ColumnInPosition)) {
		source = strings.ReplaceAll(source, string(shared.ColumnInPosition), " 1 = 0")
	}

	if strings.Contains(source, string(shared.Pagination)) {
		source = strings.ReplaceAll(source, string(shared.Pagination), " ")
	}

	SQL := "SELECT " + v.Alias + ".* FROM " + source + " " + v.Alias + " WHERE 1=0"
	return SQL
}
