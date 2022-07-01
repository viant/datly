package cmd

import (
	"fmt"
	"github.com/viant/datly/cmd/ast"
	"github.com/viant/sqlx/metadata/ast/expr"
	"github.com/viant/sqlx/metadata/ast/node"
	"github.com/viant/sqlx/metadata/ast/parser"
	"github.com/viant/sqlx/metadata/ast/query"
	"github.com/viant/toolbox"
	"strings"
)

type (
	Table struct {
		Ref        string
		StarExpr   bool
		Inner      Columns
		InnerAlias string
		Columns    Columns
		Name       string
		SQL        string
		Joins      Joins
		Alias      string

		ViewMeta *ast.ViewMeta
	}

	Column struct {
		Ns     string
		Name   string
		Alias  string
		Except []string
	}

	Columns []*Column

	Join struct {
		Key      string
		KeyAlias string
		OwnerKey string
		OwnerNs  string
		Owner    *Table
		Field    string

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
	var err error
	table := &Table{}
	switch actual := x.(type) {
	case *expr.Raw:
		table.SQL = actual.Raw[1 : len(actual.Raw)-2]
		innerQuery, _ := parser.ParseQuery(table.SQL)
		if innerQuery != nil && innerQuery.From.X != nil {
			table.Name = parser.Stringify(innerQuery.From.X)
			table.Inner = selectItemToColumn(innerQuery)
			table.InnerAlias = innerQuery.From.Alias
			fmt.Printf("SQL: %v\n", table.SQL)
			toolbox.Dump(table.Inner)
		}
		table.ViewMeta, err = ast.Parse(table.SQL)
		if err != nil {
			return nil, err
		}
	case *expr.Selector, *expr.Ident:
		table.Name = parser.Stringify(actual)
	}
	return table, nil
}

func processJoin(join *query.Join, tables map[string]*Table, outerColumn Columns) error {
	relTable, err := buildTable(join.With)
	if err != nil {
		return err
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
