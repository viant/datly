package ast

import (
	"fmt"
	"github.com/viant/sqlx/metadata/ast/expr"
	"github.com/viant/sqlx/metadata/ast/insert"
	"github.com/viant/sqlx/metadata/ast/parser"
	"github.com/viant/sqlx/metadata/ast/update"
	"sort"
	"strings"
)

func buildViewMetaInExecSQLMode(SQL string, view *ViewMeta, variables map[string]bool) error {
	lcSQL := strings.ToLower(SQL)
	boundary := getStatementBoundary(lcSQL)

	SQLExec := ""
	if len(boundary) > 0 {
		offset := boundary[0]
		limit := len(SQL)
		if len(boundary) > 1 {
			limit = boundary[1] - 1
		}
		SQLExec += strings.TrimSpace(SQL[:offset]) + "\n"
		normalizedSQL, err := normalizeSQLExec(SQL[offset], SQL[offset:limit], view, variables)
		if err != nil {
			return err
		}

		SQLExec += normalizedSQL

		for i := 1; i < len(boundary); i++ {
			offset = boundary[i]
			limit = len(SQL)
			if i+1 < len(boundary) {
				limit = boundary[i+1] - 1
			}
			normalizedSQL, err := normalizeSQLExec(SQL[offset], SQL[offset:limit], view, variables)
			if err != nil {
				return err
			}
			SQLExec += normalizedSQL
		}
	}
	view.Source = SQLExec
	return nil
}

func normalizeSQLExec(stmtType byte, SQLStmt string, view *ViewMeta, variables map[string]bool) (string, error) {
	var nonSQLStmt = ""

	hasSemiColon := false
	//TODO replace this with SQL block splitter instead
	if index := strings.LastIndex(SQLStmt, ";"); index != -1 {
		nonSQLStmt = SQLStmt[index:]
		SQLStmt = SQLStmt[:index]
		hasSemiColon = true
	}
	rawSQL, expressions := ExtractCondBlock(SQLStmt)

	switch stmtType | ' ' {
	case 'i':
		stmt, err := parser.ParseInsert(rawSQL)
		if err != nil {
			return "", err
		}
		view.Inserts = append(view.Inserts, parser.Stringify(stmt.Target.X))
		SQLStmt = normalizeAndExtractInsertValues(stmt, view, SQLStmt)

	case 'u':
		stmt, err := parser.ParseUpdate(rawSQL)
		if err != nil {
			return "", err
		}
		view.Updates = append(view.Updates, parser.Stringify(stmt.Target.X))
		SQLStmt = normalizeAndExtractUpdateSet(stmt, view, rawSQL, SQLStmt, variables)
		SQLStmt = normalizeAndExtractUpdateWhere(stmt, view, SQLStmt, variables)
		SQLStmt = normalizeOptionParameters(expressions, view, SQLStmt, variables)
	}
	if !hasSemiColon {
		SQLStmt += ";"
	}
	return SQLStmt + nonSQLStmt, nil
}

func normalizeAndExtractInsertValues(stmt *insert.Statement, view *ViewMeta, SQL string) string {
	for i, value := range stmt.Values {
		selector := ExtractSelector(value.Raw)
		if selector == "" {
			continue
		}
		column := stmt.Columns[i]
		paramName := selector[1:]
		view.addParameter(&Parameter{Id: paramName, Name: paramName, Typer: &ColumnType{ColumnName: column}})
		SQL = strings.Replace(SQL, selector, sanitizeUnsafeExpr(selector), 1)
	}
	return SQL
}

func normalizeOptionParameters(expressions []string, view *ViewMeta, SQLExec string, variables map[string]bool) string {
	for _, anExpr := range expressions {
		anExpr = strings.TrimSpace(anExpr)
		if strings.HasPrefix(anExpr, ",") {
			anExpr = anExpr[1:]
		}
		pair := strings.SplitN(anExpr, "=", 2)
		if len(pair) != 2 {
			continue
		}
		column := strings.TrimSpace(pair[0])
		selector := ExtractSelector(pair[1])
		if selector == "" {
			continue
		}
		paramName := selector[1:]
		view.addParameter(&Parameter{Id: paramName, Name: paramName, Typer: &ColumnType{ColumnName: column}})
		SQLExec = strings.Replace(SQLExec, anExpr, column+" = "+sanitizeUnsafeExpr(selector), 1)
	}
	return SQLExec
}

func normalizeAndExtractUpdateWhere(stmt *update.Statement, view *ViewMeta, SQLExec string, variables map[string]bool) string {
	var criteria []*Criterion
	ExtractCriteriaPlaceholders(stmt.Qualify.X, &criteria)
	for _, criterion := range criteria {
		y := strings.Trim(criterion.Y, "()")
		if !strings.HasPrefix(y, "$") {
			continue
		}

		_, paramName := getHolderName(y)
		if variables[paramName] {
			continue
		}

		switch strings.ToLower(criterion.Op) {
		case "in":
			paramName := y[1:]
			view.addParameter(&Parameter{Id: paramName, Name: paramName, Repeated: true, Typer: &ColumnType{
				ColumnName: criterion.X,
			}})

		case "=":
			paramName := y[1:]
			required := true
			view.addParameter(&Parameter{Id: paramName, Name: paramName, Required: &required, Typer: &ColumnType{
				ColumnName: criterion.X,
			}})
		}
		SQLExec = strings.Replace(SQLExec, y, sanitizeUnsafeExpr(y), 1)
	}
	return SQLExec
}

func normalizeAndExtractUpdateSet(stmt *update.Statement, view *ViewMeta, rawSQL string, SQLStmt string, variables map[string]bool) string {
	for _, item := range stmt.Set {
		placeholder, ok := item.Expr.(*expr.Placeholder)
		if !ok {
			continue
		}
		paramName := parser.Stringify(placeholder)
		if strings.Contains(paramName, ".") {
			continue
		}
		column := getColumnName(item)
		view.addParameter(&Parameter{Id: paramName[1:], Name: paramName[1:], Typer: &ColumnType{
			ColumnName: column,
		}})
		originalExpr := strings.TrimSpace(rawSQL[item.Begin:item.End])
		item.Expr = &expr.Raw{
			Raw: sanitizeUnsafeExpr(paramName),
		}
		enrichedExpr := parser.Stringify(item)
		SQLStmt = strings.Replace(SQLStmt, originalExpr, enrichedExpr, 1)
	}
	return SQLStmt
}

func sanitizeUnsafeExpr(paramName string) string {
	paramName = sanitizeUnsafeParameter(paramName)
	return fmt.Sprintf("$criteria.AppendBinding(%v)", paramName)
}

func sanitizeUnsafeParameter(paramName string) string {
	paramName = strings.Replace(paramName, "$", "$Unsafe.", 1)
	return paramName
}

func getColumnName(item *update.Item) string {
	column := ""
	switch actual := item.Column.(type) {
	case *expr.Ident:
		column = actual.Name
	case *expr.Selector:
		column = actual.Name
	}
	return column
}

func getStatementBoundary(lcSQL string) []int {
	var boundary []int
	var offset = 0
	tempSQL := lcSQL
	for {
		index := getStatementIndex(tempSQL)
		if index == -1 {
			break
		}

		boundary = append(boundary, offset+index)
		offset += index + 1
		tempSQL = tempSQL[index+1:]
	}
	return boundary
}

func getStatementIndex(lcSQL string) int {
	var candidates []int
	for _, keyword := range []string{"insert ", "update ", "delete", "call "} {
		if index := strings.Index(lcSQL, keyword); index != -1 {
			candidates = append(candidates, index)
		}
	}
	if len(candidates) == 0 {
		return -1
	}
	sort.Ints(candidates)
	return candidates[0]
}
