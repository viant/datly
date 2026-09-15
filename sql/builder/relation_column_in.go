package builder

import (
	"strings"

	"github.com/viant/datly/data"
	sqltext "github.com/viant/sqlparser/source"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/metadata/info"
)

const andColumnInToken = "$AND_COLUMN_IN"
const columnInToken = "$COLUMN_IN"

type relationFilter struct {
	relation         *data.Relation
	positionalArgs   []any
	compositeColumns []string
	compositeRows    [][]interface{}
	dialect          *info.Dialect
}

func (f relationFilter) applyColumnIn(sqlText string, disableFallback bool) (string, bool) {
	if f.relation == nil {
		return stripColumnInTokens(sqlText), false
	}
	columnToken := sqltext.Token(columnInToken)
	if columnToken.Contains(sqlText) {
		return columnToken.ReplaceAll(sqlText, f.columnInExpression()), true
	}
	andToken := sqltext.Token(andColumnInToken)
	if andToken.Contains(sqlText) {
		expr := f.columnInExpression()
		if expr == "1 = 0" {
			return andToken.ReplaceAll(sqlText, " AND ( 1 = 0 )"), true
		}
		return andToken.ReplaceAll(sqlText, " AND ( "+expr+")"), true
	}
	if disableFallback {
		return sqlText, false
	}
	if len(f.compositeColumns) > 0 {
		if len(f.compositeRows) == 0 {
			return sqlText, false
		}
		if sqlx.ParseParameters(sqlText).HasPositional() {
			return sqlText, false
		}
		expression, _ := f.expression()
		clause := " WHERE " + expression
		if sqltext.HasTopLevelClause(sqlText, "where") {
			clause = " AND " + expression
		}
		return insertRelationClause(sqlText, clause), true
	}
	columnExpr, ok := relationScalarColumnExpr(f.relation)
	if !ok {
		return stripColumnInTokens(sqlText), false
	}
	if len(f.positionalArgs) == 0 {
		clause := " WHERE 1 = 0"
		if sqltext.HasTopLevelClause(sqlText, "where") {
			clause = " AND 1 = 0"
		}
		return insertRelationClause(sqlText, clause), true
	}
	if sqlx.ParseParameters(sqlText).HasPositional() {
		return sqlText, false
	}
	clause := " WHERE " + columnExpr + " IN (" + strings.Repeat("?,", len(f.positionalArgs)) + ")"
	clause = strings.Replace(clause, ",)", ")", 1)
	if sqltext.HasTopLevelClause(sqlText, "where") {
		clause = " AND " + columnExpr + " IN (" + strings.Repeat("?,", len(f.positionalArgs)) + ")"
		clause = strings.Replace(clause, ",)", ")", 1)
	}
	return insertRelationClause(sqlText, clause), true
}

func stripColumnInTokens(sqlText string) string {
	sqlText = sqltext.Token(andColumnInToken).ReplaceAll(sqlText, "")
	sqlText = sqltext.Token(columnInToken).ReplaceAll(sqlText, "")
	return cleanupDanglingWhere(sqlText)
}

func (f relationFilter) columnInExpression() string {
	expression, ok := f.expression()
	if !ok {
		return "1 = 0"
	}
	return expression
}

func (f relationFilter) expression() (string, bool) {
	if f.relation == nil {
		return "", false
	}
	if len(f.compositeColumns) > 0 {
		if len(f.compositeRows) == 0 {
			return "1 = 0", true
		}
		return renderCompositeIn(f.dialect, f.compositeColumns, len(f.compositeRows)), true
	}
	columnExpr, ok := relationScalarColumnExpr(f.relation)
	if !ok {
		return "", false
	}
	if len(f.positionalArgs) == 0 {
		return "1 = 0", true
	}
	return columnExpr + " IN (" + strings.TrimSuffix(strings.Repeat("?,", len(f.positionalArgs)), ",") + ")", true
}

func relationScalarColumnExpr(relation *data.Relation) (string, bool) {
	if relation == nil || relation.Of == nil || len(relation.Of.On) == 0 {
		return "", false
	}
	link := relation.Of.On[0]
	if link == nil || strings.TrimSpace(link.Column) == "" {
		return "", false
	}
	column := strings.TrimSpace(link.Column)
	if ns := strings.TrimSpace(link.Namespace); ns != "" {
		column = ns + "." + column
	}
	return column, true
}

// insertRelationClause also serves selector/partition conjunctions. Native
// clause scanning keeps each existing disjunction one operand of the added AND.
func insertRelationClause(sqlText string, clause string) string {
	insertAt := sqltext.CriteriaBoundary(sqlText)
	where := sqltext.FindTopLevelKeyword(sqlText, "where", 0)
	if strings.HasPrefix(strings.TrimSpace(clause), "AND ") {
		body := strings.TrimSpace(strings.TrimSpace(clause)[len("AND"):])
		if sqltext.FindTopLevelKeyword(body, "or", 0) >= 0 {
			clause = " AND (" + body + ")"
		}
		if where >= 0 {
			start := where + len("where")
			predicate := strings.TrimSpace(sqlText[start:insertAt])
			if sqltext.FindTopLevelKeyword(predicate, "or", 0) >= 0 {
				sqlText = sqlText[:start] + " (" + predicate + ") " + sqlText[insertAt:]
				insertAt = sqltext.CriteriaBoundary(sqlText)
			}
		}
	}
	if insertAt == len(sqlText) {
		return strings.TrimRight(sqlText, " \t\r\n") + clause
	}
	return strings.TrimRight(sqlText[:insertAt], " \t\r\n") + clause + " " + strings.TrimLeft(sqlText[insertAt:], " \t\r\n")
}
