package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/viant/datly/cmd/ast"
	"github.com/viant/sqlx/metadata/ast/expr"
	"github.com/viant/sqlx/metadata/ast/node"
	"github.com/viant/sqlx/metadata/ast/parser"
	"github.com/viant/sqlx/metadata/ast/query"
	"strings"
)

type (
	Table struct {
		Ref         string
		StarExpr    bool
		Inner       Columns
		ColumnTypes map[string]string
		InnerAlias  string
		InnerSQL    string
		Deps        map[string]string
		Columns     Columns
		Name        string
		SQL         string
		Joins       Joins
		Alias       string
		TableMeta
		ViewMeta *ast.ViewMeta
	}

	TableMeta struct {
		Connector string
	}
	Column struct {
		Ns     string
		Name   string
		Alias  string
		Except []string
	}

	Columns []*Column

	Join struct {
		Key       string
		KeyAlias  string
		OwnerKey  string
		OwnerNs   string
		Owner     *Table
		Connector string
		Field     string

		ToOne bool
		Table *Table
	}

	Joins []*Join
)

func (c Columns) StarExpr(ns string) *Column {
	for _, item := range c {
		if item.Name == "*" && item.Ns == ns {
			return item
		}
	}
	return nil
}

func (c Columns) ByNs(ns string) map[string]*Column {
	var result = make(map[string]*Column)
	for i, item := range c {
		if item.Name == "*" || item.Ns != ns {
			continue
		}
		alias := item.Alias
		if alias == "" {
			alias = item.Name
		}
		result[alias] = c[i]
	}
	return result
}

func (c Columns) ByAlias() map[string]*Column {
	var result = make(map[string]*Column)
	if c == nil {
		return result
	}
	for i, item := range c {
		if item.Name == "*" {
			continue
		}
		alias := item.Alias
		if alias == "" {
			alias = item.Name
		}
		result[alias] = c[i]
	}
	return result
}

func (j *Joins) Index() map[string]*Join {
	var result = make(map[string]*Join)
	for _, join := range *j {
		result[join.Table.Alias] = join
	}

	return result
}

func ParseSQLx(SQL string) (*Table, error) {
	aQuery, err := parser.ParseQuery(SQL)
	if aQuery == nil {
		return nil, err
	}
	var tables = map[string]*Table{}
	table, err := buildTable(aQuery.From.X)
	if err != nil {
		return nil, err
	}
	table.Alias = aQuery.From.Alias
	table.Columns = selectItemToColumn(aQuery)
	if star := table.Columns.StarExpr(table.Alias); star != nil {
		table.StarExpr = true
	}
	tables[table.Alias] = table
	if len(aQuery.Joins) > 0 {
		for _, join := range aQuery.Joins {
			if err := processJoin(join, tables, table.Columns); err != nil {
				return nil, err
			}
		}
	}
	return table, nil
}

func buildTable(x node.Node) (*Table, error) {
	//var err error
	table := &Table{}
	switch actual := x.(type) {
	case *expr.Raw:
		table.SQL = strings.Trim(actual.Raw, "()")
		innerSQL, paramsExprs := ast.ExtractCondBlock(table.SQL)

		innerQuery, err := parser.ParseQuery(innerSQL)

		if innerQuery != nil && innerQuery.From.X != nil {
			table.Name = strings.Trim(parser.Stringify(innerQuery.From.X), "`")
			table.Inner = selectItemToColumn(innerQuery)

			if len(innerQuery.Joins) > 0 {
				table.Deps = map[string]string{}
				for _, join := range innerQuery.Joins {
					table.Deps[join.Alias] = strings.Trim(parser.Stringify(join.With), "`")
				}
			}
			if innerQuery.Qualify != nil {
				extractCriteriaPairs(innerQuery.Qualify.X, &paramsExprs)
			}
			table.InnerSQL = innerSQL
			table.InnerAlias = innerQuery.From.Alias
		}
		table.ViewMeta, err = ast.Parse(table.SQL)
		if err != nil {
			return nil, err
		}
		table.ViewMeta.Expressions = paramsExprs

	case *expr.Selector, *expr.Ident:
		table.Name = parser.Stringify(actual)

	}
	return table, nil
}

func extractCriteriaPairs(node node.Node, list *[]string) {
	if node == nil {
		return
	}
	switch actual := node.(type) {
	case *expr.Binary:
		op := strings.ToUpper(actual.Op)
		switch op {
		case "AND", "OR":
			extractCriteriaPairs(actual.X, list)
			extractCriteriaPairs(actual.Y, list)
		default:
			if bin, ok := actual.Y.(*expr.Binary); ok {
				extractCriteriaPairs(bin.Y, list)
				switch operand := actual.X.(type) {
				case *expr.Selector, *expr.Ident, *expr.Placeholder:
					appendParamExpr(operand, actual.Op, bin.X, list)
					*list = append(*list, parser.Stringify(operand)+" = "+parser.Stringify(bin.X))
				}
				return
			}
			appendParamExpr(actual.X, actual.Op, actual.Y, list)
		}
	case *expr.Unary:

	}
}

func appendParamExpr(x node.Node, op string, y node.Node, list *[]string) {
	if p, ok := y.(*expr.Parenthesis); ok && strings.EqualFold(op, "IN") {
		*list = append(*list, parser.Stringify(x)+" = "+strings.Trim(p.Raw, "()"))
		return
	}
	switch operand := y.(type) {
	case *expr.Placeholder:
		*list = append(*list, parser.Stringify(x)+" = "+operand.Name)
	}
}

func processJoin(join *query.Join, tables map[string]*Table, outerColumn Columns) error {
	relTable, err := buildTable(join.With)
	if err != nil {
		return err
	}
	if join.Comments != "" {
		comments := join.Comments
		comments = strings.ReplaceAll(comments, "/*", "")
		comments = strings.ReplaceAll(comments, "*/", "")
		_ = json.Unmarshal([]byte(comments), &relTable.TableMeta)
	}

	relTable.Alias = join.Alias
	relTable.Ref = relTable.Name
	tables[relTable.Alias] = relTable
	if star := outerColumn.StarExpr(relTable.Alias); star != nil {
		relTable.StarExpr = true
	}
	relJoin := &Join{
		Table: relTable,
	}
	on := join.On.X
	x := extractSelector(on, true)
	y := extractSelector(on, false)
	if err := updateRelationKey(relTable, y, relJoin, x); err != nil {
		return err
	}
	byAlias := relTable.Inner.ByAlias()
	relJoin.KeyAlias = relTable.InnerAlias
	relJoin.Connector = relTable.Connector
	if len(byAlias) > 0 {
		column, ok := byAlias[relJoin.Key]
		if !ok {
			return fmt.Errorf("key %s is not listed on %v", relJoin.Key, relTable.Name)
		}
		if column.Name != relJoin.Key {
			relJoin.Field = relJoin.Key
			relJoin.Key = column.Name
			if column.Ns != "" {
				relJoin.KeyAlias = column.Ns
			}
		}
	}

	relJoin.ToOne = hasOneCardinalityPredicate(join.On.X)
	owner, ok := tables[relJoin.OwnerNs]
	if !ok {
		return fmt.Errorf("unable to locate owner view: %s", relJoin.OwnerNs)
	}
	relJoin.Owner = owner
	owner.Joins = append(owner.Joins, relJoin)
	return nil
}

func updateRelationKey(relTable *Table, y *expr.Selector, relJoin *Join, x *expr.Selector) error {
	if relTable.Alias == y.Name {
		relJoin.Key = parser.Stringify(y.X)
		relJoin.OwnerKey = parser.Stringify(x.X)
		relJoin.OwnerNs = x.Name
	} else if relTable.Alias == x.Name {
		relJoin.Key = parser.Stringify(x.X)
		relJoin.OwnerKey = parser.Stringify(y.X)
		relJoin.OwnerNs = y.Name
	} else {
		return fmt.Errorf("unknow view alias: %v %v", relTable.Alias, relTable.Name)
	}
	return nil
}

func hasOneCardinalityPredicate(n node.Node) bool {
	predicate := parser.Stringify(n)
	return strings.Contains(predicate, " 1 = 1")
}

func extractSelector(n node.Node, left bool) *expr.Selector {
	binary, ok := n.(*expr.Binary)
	if !ok {
		return nil
	}
	op := binary.X
	if !left {
		op = binary.Y
	}
	switch actual := op.(type) {
	case *expr.Literal:
	case *expr.Binary:
		return extractSelector(actual, !left)
	case *expr.Selector:
		return actual
	}
	return nil
}

func selectItemToColumn(query *query.Select) Columns {
	var result []*Column
	for _, item := range query.List {
		switch actual := item.Expr.(type) {
		case *expr.Ident:
			result = append(result, &Column{Name: actual.Name, Alias: item.Alias})
		case *expr.Selector:
			result = append(result, &Column{Name: parser.Stringify(actual.X), Ns: actual.Name, Alias: item.Alias})
		case *expr.Star:
			switch star := actual.X.(type) {
			case *expr.Ident:
				result = append(result, &Column{Name: star.Name, Except: actual.Except})
			case *expr.Selector:
				result = append(result, &Column{Name: parser.Stringify(star.X), Ns: star.Name, Except: actual.Except})
			}
		}
	}
	return result
}
