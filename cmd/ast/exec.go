package ast

import (
	"fmt"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/sqlx/metadata/ast/expr"
	"github.com/viant/sqlx/metadata/ast/insert"
	"github.com/viant/sqlx/metadata/ast/parser"
	"github.com/viant/sqlx/metadata/ast/update"
	"sort"
	"strings"
)

func buildViewMetaInExecSQLMode(SQL string, view *option.ViewMeta, variables map[string]bool) error {
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

func normalizeSQLExec(stmtType byte, SQLStmt string, view *option.ViewMeta, variables map[string]bool) (string, error) {
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

func normalizeAndExtractInsertValues(stmt *insert.Statement, view *option.ViewMeta, SQL string) string {
	for i, value := range stmt.Values {
		selector := ExtractSelector(value.Raw)
		if selector == "" {
			continue
		}
		column := stmt.Columns[i]
		paramName := selector[1:]
		view.AddParameter(&option.Parameter{Id: paramName, Name: paramName, Typer: &option.ColumnType{ColumnName: column}})
		SQL = strings.Replace(SQL, selector, sanitizeUnsafeExpr(selector), 1)
	}
	return SQL
}

func normalizeOptionParameters(expressions []string, view *option.ViewMeta, SQLExec string, variables map[string]bool) string {
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
		view.AddParameter(&option.Parameter{Id: paramName, Name: paramName, Typer: &option.ColumnType{ColumnName: column}})
		SQLExec = strings.Replace(SQLExec, anExpr, column+" = "+sanitizeUnsafeExpr(selector), 1)
	}
	return SQLExec
}

func normalizeAndExtractUpdateWhere(stmt *update.Statement, view *option.ViewMeta, SQLExec string, variables map[string]bool) string {
	var criteria []*Criterion
	ExtractCriteriaPlaceholders(stmt.Qualify.X, &criteria)
	for _, criterion := range criteria {
		y := strings.Trim(criterion.Y, "()")
		if !strings.HasPrefix(y, "$") {
			continue
		}

		prefix, paramName := getHolderName(y)
		SQLExec = strings.Replace(SQLExec, y, sanitizePlaceholder(prefix, paramName, y, variables), 1)

		switch strings.ToLower(criterion.Op) {
		case "in":
			view.AddParameter(&option.Parameter{Id: paramName, Name: paramName, Repeated: true, Typer: &option.ColumnType{
				ColumnName: criterion.X,
			}})

		case "=":
			required := true
			view.AddParameter(&option.Parameter{Id: paramName, Name: paramName, Required: &required, Typer: &option.ColumnType{
				ColumnName: criterion.X,
			}})
		}
	}
	return SQLExec
}

func normalizeAndExtractUpdateSet(stmt *update.Statement, view *option.ViewMeta, rawSQL string, SQLStmt string, variables map[string]bool) string {
	for _, item := range stmt.Set {
		placeholder, ok := item.Expr.(*expr.Placeholder)
		if !ok {
			continue
		}

		actualParam := parser.Stringify(placeholder)
		prefix, paramName := getHolderName(actualParam)

		item.Expr = &expr.Raw{Raw: sanitizePlaceholder(prefix, paramName, actualParam, variables)}

		originalExpr := strings.TrimSpace(rawSQL[item.Begin:item.End])
		enrichedExpr := parser.Stringify(item)

		SQLStmt = strings.Replace(SQLStmt, originalExpr, enrichedExpr, 1)
		column := getColumnName(item)
		view.AddParameter(&option.Parameter{Id: paramName, Name: paramName, Typer: &option.ColumnType{
			ColumnName: column,
		}})
	}

	return SQLStmt
}

func sanitizePlaceholder(prefix, paramName, raw string, variables map[string]bool) string {

	if variables[paramName] {
		return sanitizeInternalVariable(prefix, raw)
	}

	if prefix == keywords.ParamsKey {
		return sanitizeUnsafeParameter(raw)
	}

	return sanitizeInternalVariable("", raw)
}

func sanitizeInternalVariable(prefix, paramName string) string {
	if prefix == keywords.ParamsKey {
		return strings.Replace(paramName, fmt.Sprintf("$%v", keywords.ParamsKey), "$", 1)
	}

	return fmt.Sprintf("$criteria.AppendBinding(%v)", paramName)
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
