package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io"
	rdata "github.com/viant/toolbox/data"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/parser"
	"strings"
)

func (s *serverBuilder) updateTableColumnTypes(ctx context.Context, table *Table) {
	//TODO read all column per alias from main and join table
	table.ColumnTypes = map[string]string{}
	connector := s.options.MatchConnector(table.Connector)
	db, err := connector.DB()
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
	parse, err := parser.Parse([]byte(tableName))
	var args []interface{}
	expandMap := &rdata.Map{}

	if err == nil {
		if anIndex := strings.Index(tableName, "SELECT"); anIndex != -1 {

			for _, statement := range parse.Stmt {
				switch actual := statement.(type) {
				case *expr.Select:
					expandMap.SetValue(actual.FullName[1:], "?")
					args = append(args, 0)
				}
			}

			tableName = expandMap.ExpandAsText(tableName)
		}
	}

	SQL := "SELECT * FROM " + tableName + " t WHERE 1 = 0"

	fmt.Printf("checking %v ...\n", tableName)
	query, err := db.QueryContext(context.Background(), SQL, args...)
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
