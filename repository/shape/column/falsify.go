package column

import (
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
)

// falsifyQuery parses an SQL string and injects WHERE 1=0 into every SELECT
// in the query tree (outer query, CTEs, UNIONs). This ensures zero rows are
// scanned while preserving the output schema for column type inference.
//
// Returns the rewritten SQL string and true if successful.
// Returns the original SQL and false if parsing fails.
func falsifyQuery(sql string) (string, bool) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return sql, false
	}
	parsed, err := sqlparser.ParseQuery(sql)
	if err != nil {
		return sql, false
	}
	falsifySelect(parsed)
	// Remove LIMIT/OFFSET from outer query — we want schema only
	parsed.Limit = nil
	parsed.Offset = nil
	result := sqlparser.Stringify(parsed)
	if strings.TrimSpace(result) == "" {
		return sql, false
	}
	return result, true
}

// falsifySelect injects 1=0 into a SELECT and recursively into all nested SELECTs.
func falsifySelect(sel *query.Select) {
	if sel == nil {
		return
	}
	// Inject 1=0 into this SELECT's WHERE clause
	injectFalsePredicate(sel)
	// Process CTE WITH selects
	for _, ws := range sel.WithSelects {
		if ws != nil && ws.X != nil {
			falsifySelect(ws.X)
			ws.Raw = "" // Force Stringify to use modified X instead of original Raw
		}
	}
	// Process UNION branches
	if sel.Union != nil && sel.Union.X != nil {
		falsifySelect(sel.Union.X)
	}
	// Process subquery in FROM (if it's a nested SELECT)
	falsifyFromSubquery(sel)
	// Process JOIN subqueries
	for _, join := range sel.Joins {
		if join != nil {
			falsifyJoinSubquery(join)
		}
	}
}

// injectFalsePredicate adds 1=0 to the SELECT's WHERE clause.
func injectFalsePredicate(sel *query.Select) {
	if sel == nil {
		return
	}
	fp := &expr.Binary{
		X:  &expr.Literal{Value: "1"},
		Op: "=",
		Y:  &expr.Literal{Value: "0"},
	}
	if sel.Qualify == nil || sel.Qualify.X == nil {
		sel.Qualify = &expr.Qualify{X: fp}
	} else {
		sel.Qualify = &expr.Qualify{
			X: &expr.Binary{
				X:  fp,
				Op: "AND",
				Y:  sel.Qualify.X,
			},
		}
	}
}

// falsifyFromSubquery checks if the FROM clause contains a subquery and falsifies it.
func falsifyFromSubquery(sel *query.Select) {
	if sel == nil || sel.From.X == nil {
		return
	}
	switch sub := sel.From.X.(type) {
	case *expr.Parenthesis:
		falsifySubqueryExpr(sub)
	case *expr.Raw:
		falsifyRawSubquery(sub)
	}
}

func falsifyRawSubquery(raw *expr.Raw) {
	if raw == nil {
		return
	}
	text := strings.TrimSpace(raw.Raw)
	if text == "" && raw.Unparsed != "" {
		text = strings.TrimSpace(raw.Unparsed)
	}
	// Strip outer parens if present
	if len(text) >= 2 && text[0] == '(' && text[len(text)-1] == ')' {
		text = text[1 : len(text)-1]
	}
	if !strings.Contains(strings.ToLower(text), "select") {
		return
	}
	subQuery, err := sqlparser.ParseQuery(text)
	if err != nil {
		return
	}
	falsifySelect(subQuery)
	rewritten := sqlparser.Stringify(subQuery)
	raw.Raw = "(" + rewritten + ")"
}

// falsifyJoinSubquery checks if a JOIN's WITH clause contains a subquery and falsifies it.
func falsifyJoinSubquery(join *query.Join) {
	if join == nil || join.With == nil {
		return
	}
	if sub, ok := join.With.(*expr.Parenthesis); ok {
		falsifySubqueryExpr(sub)
	}
}

// falsifySubqueryExpr attempts to parse and falsify a parenthesized subquery expression.
func falsifySubqueryExpr(paren *expr.Parenthesis) {
	if paren == nil || paren.X == nil {
		return
	}
	raw := sqlparser.Stringify(paren.X)
	if !strings.Contains(strings.ToLower(strings.TrimSpace(raw)), "select") {
		return
	}
	subQuery, err := sqlparser.ParseQuery(raw)
	if err != nil {
		return
	}
	falsifySelect(subQuery)
	rewritten := sqlparser.Stringify(subQuery)
	paren.X = expr.NewRaw(rewritten)
}
