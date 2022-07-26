package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io"
	"strings"
)

func (s *serverBuilder) updateTableColumnTypes(ctx context.Context, table *Table) {
	if len(table.ColumnTypes) > 0 {
		return
	}
	//TODO read all column per alias from main and join table
	table.ColumnTypes = map[string]string{}
	connector := s.options.MatchConnector(table.Connector)
	db, err := connector.DB(ctx)
	if err != nil {
		fmt.Printf(err.Error())
	}

	s.updatedColumns(table, "", table.Name, db)
	s.updatedColumns(table, table.InnerAlias, table.Name, db)
	if len(table.Deps) > 0 {
		for k, v := range table.Deps {
			s.updatedColumns(table, k, v, db)
		}
	}
}

func (s *serverBuilder) updatedColumns(table *Table, prefix, tableName string, db *sql.DB) {
	SQL := "SELECT * FROM " + tableName + " WHERE 1 = 0"
	fmt.Printf("checking %v ...\n", tableName)
	query, err := db.QueryContext(context.Background(), SQL)
	if err != nil {
		s.logger.Write([]byte(fmt.Sprintf("error occured while updating table %v columns: %v", tableName, err)))
		return
	}

	if types, err := query.ColumnTypes(); err == nil {
		ioColumns := io.TypesToColumns(types)
		for _, column := range ioColumns {
			columnType := column.ScanType().String()
			if strings.HasPrefix(columnType, "*") {
				columnType = columnType[1:]
			}
			if columnType == "sql.RawBytes" {
				columnType = "string"
			}
			if strings.Contains(columnType, "int") {
				columnType = "int"
			}
			key := prefix
			if key != "" {
				key += "."
			}
			key += column.Name()
			table.ColumnTypes[strings.ToLower(key)] = columnType
		}
	}
}
