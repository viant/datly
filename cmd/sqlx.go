package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/viant/datly/cmd/ast"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/metadata/ast/expr"
	"github.com/viant/sqlx/metadata/ast/node"
	"github.com/viant/sqlx/metadata/ast/parser"
	"github.com/viant/sqlx/metadata/ast/query"
	"github.com/viant/toolbox"
	"strings"
)

func ParseSQLx(SQL string, routeOpt *option.Route, hints option.ParameterHints) (*option.Table, map[string]*option.TableParam, error) {
	aQuery, err := parser.ParseQuery(SQL)
	if aQuery == nil {
		return nil, nil, err
	}

	toolbox.Dump(aQuery)
	var tables = map[string]*option.Table{}

	table, err := buildTableFromQuery(aQuery, routeOpt, hints)
	if err != nil {
		return nil, nil, err
	}

	table.Alias = aQuery.From.Alias
	table.Columns = selectItemToColumn(aQuery, routeOpt)
	table.ViewHint = aQuery.From.Comments

	if star := table.Columns.StarExpr(table.Alias); star != nil {
		table.StarExpr = true
	}

	var dataParameters = map[string]*option.TableParam{}
	tables[table.Alias] = table

	if len(aQuery.Joins) > 0 {
		for _, join := range aQuery.Joins {
			if err := processJoin(join, tables, table.Columns, dataParameters, routeOpt, hints); err != nil {
				return nil, nil, err
			}
		}
	}
	return table, dataParameters, nil
}

func buildTableFromQuery(aQuery *query.Select, routeOpt *option.Route, hints option.ParameterHints) (*option.Table, error) {
	table, err := buildTable(aQuery.From.X, routeOpt, hints)
	if err != nil {
		return nil, err
	}
	//
	//for _, item := range aQuery.List {
	//	if item.Comments == "" || item.Alias == "" {
	//		continue
	//	}
	//
	//	aType := &DataTyped{}
	//	hint, _ := sanitizer.SplitHint(item.Comments)
	//
	//	if err := json.Unmarshal([]byte(hint), aType); err != nil {
	//		return nil, err
	//	}
	//
	//	table.ColumnTypes[item.Alias] = aType.DataType
	//}

	return table, nil
}

func buildTable(x node.Node, routeOpt *option.Route, hints option.ParameterHints) (*option.Table, error) {
	//var err error
	table := option.NewTable("")

	switch actual := x.(type) {
	case *expr.Raw:
		SQL := strings.TrimSpace(actual.Raw)
		trimmedParentheses := true
		for len(SQL) >= 2 && trimmedParentheses {
			if SQL[0] == '(' && SQL[len(SQL)-1] == ')' {
				SQL = SQL[1 : len(SQL)-1]
			} else {
				trimmedParentheses = false
			}

			SQL = strings.TrimSpace(SQL)
		}

		table.SQL = SQL
		if err := UpdateTableSettings(table, routeOpt, hints); err != nil {
			return table, err
		}

	case *expr.Selector, *expr.Ident:
		table.Name = parser.Stringify(actual)

	}
	return table, nil
}

func UpdateTableSettings(table *option.Table, routeOpt *option.Route, hints option.ParameterHints) error {
	innerSQL, paramsExprs := ast.ExtractCondBlock(table.SQL)
	innerQuery, err := parser.ParseQuery(innerSQL)
	fmt.Printf("innerSQL %v %v\n", table.SQL, err)

	if innerQuery != nil && innerQuery.From.X != nil {
		table.Name = strings.Trim(parser.Stringify(innerQuery.From.X), "`")
		table.Inner = selectItemToColumn(innerQuery, routeOpt)
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

	table.ViewMeta, err = ast.Parse(table.SQL, routeOpt, hints)
	if err != nil {
		return err
	}

	table.ViewMeta.Expressions = paramsExprs
	return nil
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

func processJoin(join *query.Join, tables map[string]*option.Table, outerColumn option.Columns, dataParameters map[string]*option.TableParam, routeOpt *option.Route, hints option.ParameterHints) error {
	relTable, err := buildTable(join.With, routeOpt, hints)
	if err != nil {
		return err
	}
	if hint := join.Comments; hint != "" {
		err = hintToStruct(hint, &relTable.TableMeta)
		if err != nil {
			fmt.Printf(fmt.Errorf("invalid hint: %s, %w\n", hint, err).Error())
		}
	}
	isParamView := isParamPredicate(parser.Stringify(join.On.X))
	if isParamView {

		paramName := join.Alias
		if relTable.DataViewParameter == nil {
			relTable.DataViewParameter = &view.Parameter{}
		}
		relTable.DataViewParameter.In = &view.Location{Name: paramName, Kind: view.DataViewKind}
		relTable.DataViewParameter.Schema = &view.Schema{Name: strings.Title(paramName)}

		relTable.Alias = paramName
		relTable.DataViewParameter.Name = paramName

		dataParameters[paramName] = &option.TableParam{Table: relTable, Param: relTable.DataViewParameter}
		UpdateAuthToken(relTable)
		return nil
	}

	relTable.Alias = join.Alias
	relTable.Ref = relTable.Name
	tables[relTable.Alias] = relTable
	if star := outerColumn.StarExpr(relTable.Alias); star != nil {
		relTable.StarExpr = true
	}
	relJoin := &option.Join{
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
	relJoin.Cache = relTable.Cache
	relJoin.Warmup = relTable.Warmup

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

func UpdateAuthToken(aTable *option.Table) {
	if aTable.Auth == "" {
		return
	}
	required := true
	aTable.Parameter = &view.Parameter{
		Name:            aTable.Auth,
		In:              &view.Location{Name: "Authorization", Kind: view.HeaderKind},
		ErrorStatusCode: 401,
		Required:        &required,
		Codec:           &view.Codec{Name: "JwtClaim"},
		Schema:          &view.Schema{Name: "JwtTokenInfo"},
	}

}

func hintToStruct(encoded string, aStructPtr interface{}) error {
	encoded = strings.ReplaceAll(encoded, "/*", "")
	encoded = strings.ReplaceAll(encoded, "*/", "")
	return json.Unmarshal([]byte(encoded), aStructPtr)
}

func isParamPredicate(criteria string) bool {
	onCriteria := strings.TrimSpace(criteria)
	if index := strings.Index(criteria, "/*"); index != -1 {
		criteria = criteria[:index]
	}
	isParamView := onCriteria == "1 = 1"
	return isParamView
}

func updateRelationKey(relTable *option.Table, y *expr.Selector, relJoin *option.Join, x *expr.Selector) error {
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

func selectItemToColumn(query *query.Select, route *option.Route) option.Columns {
	var result []*option.Column
	for _, item := range query.List {
		appendItem(item, &result, route)
	}
	return result
}

func appendItem(item *query.Item, result *[]*option.Column, route *option.Route) {
	comments := item.Comments
	if hint := comments; hint != "" {
		column := &view.Column{}
		if err := hintToStruct(hint, &column); err != nil {
		}
		item.DataType = column.DataType
	}

	if actualDataType, ok := route.Declare[item.Alias]; ok {
		item.DataType = actualDataType
	}

	column, err := getColumn(item)
	if err != nil {
		fmt.Printf("error when creating column: %v\n", err.Error())
		return
	}

	if column.Comments == "" {
		column.Comments = item.Comments
	}

	*result = append(*result, column)
}

func getColumn(item *query.Item) (*option.Column, error) {
	switch actual := item.Expr.(type) {
	case *expr.Call:
		call := parser.Stringify(actual)
		lcCall := strings.ToLower(call)
		if item.DataType == "" {
			item.DataType = "string"
		}
		if isCast := strings.HasPrefix(lcCall, "cast"); isCast {
			if index := strings.Index(lcCall, " as "); index != -1 {
				targetType := strings.Trim(call[index+4:], " )")
				item.DataType = castTypeToGoType(targetType)
			}
		}

		return &option.Column{Name: call, Alias: item.Alias, DataType: item.DataType}, nil
	case *expr.Ident:
		return &option.Column{Name: actual.Name, Alias: item.Alias, DataType: item.DataType}, nil
	case *expr.Selector:
		return &option.Column{Name: parser.Stringify(actual.X), Ns: actual.Name, DataType: item.DataType, Alias: item.Alias}, nil
	case *expr.Star:
		switch star := actual.X.(type) {
		case *expr.Ident:
			return &option.Column{Name: star.Name, Except: actual.Except}, nil
		case *expr.Selector:
			return &option.Column{Name: parser.Stringify(star.X), Ns: star.Name, Except: actual.Except, Comments: actual.Comments}, nil
		}
	case *expr.Literal:
		return &option.Column{Name: "", Alias: item.Alias, DataType: actual.Kind}, nil
	case *expr.Binary:
		enExpr := parser.Stringify(actual)
		if item.DataType == "" || (strings.Contains(enExpr, "+") || strings.Contains(enExpr, "-") || strings.Contains(enExpr, "/") || strings.Contains(enExpr, "*")) {
			item.DataType = "float64"
		}
		return &option.Column{Name: enExpr, Alias: item.Alias, DataType: item.DataType}, nil
	case *expr.Parenthesis:
		return &option.Column{Name: parser.Stringify(actual), Alias: item.Alias, DataType: item.DataType}, nil
	}
	return nil, fmt.Errorf("invalid type: %T", item.Expr)
}

func castTypeToGoType(targetType string) string {
	switch strings.ToLower(targetType) {
	case "signed":
		return "int"
	}
	return "string"
}
