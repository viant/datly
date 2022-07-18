package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io"
	"strings"
)

func updateTableColumnTypes(options *Options, table *Table) {
	//TODO read all column per alias from main and join table
	table.ColumnTypes = map[string]string{}
	connector := options.MatchConnector(table.Connector)
	db, err := connector.Db()
	if err != nil {
		fmt.Printf(err.Error())
	}
	updatedColumns(table, "", table.Name, db)
	if len(table.Deps) > 0 {
		for k, v := range table.Deps {
			updatedColumns(table, k, v, db)
		}
	}
}

func updatedColumns(table *Table, prefix, tableName string, db *sql.DB) {
	SQL := "SELECT * FROM " + tableName + " WHERE 1 = 0"
	fmt.Printf("checking %v ...\n", tableName)
	query, err := db.QueryContext(context.Background(), SQL)
	if err == nil {
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

}
