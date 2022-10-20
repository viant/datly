package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/view"
	"github.com/viant/parsly"
	"github.com/viant/sqlx/metadata/ast/expr"
	"github.com/viant/sqlx/metadata/ast/node"
	"github.com/viant/sqlx/metadata/ast/parser"
	"github.com/viant/sqlx/metadata/ast/query"
	rdata "github.com/viant/toolbox/data"
	"strings"
)

type (
	key struct {
		Column string
		Field  string
		Alias  string
	}

	relationKey struct {
		owner *key
		child *key
	}
)

func newViewConfig(viewName string, fileName string, parent *query.Join, aTable *option.Table, templateMeta *option.Table, mode view.Mode) *viewConfig {
	result := &viewConfig{
		viewName:       viewName,
		queryJoin:      parent,
		table:          aTable,
		fileName:       fileName,
		viewParams:     map[string]*viewParamConfig{},
		metasBuffer:    map[string]*option.Table{},
		templateMeta:   templateMeta,
		viewType:       mode,
		relationsIndex: map[string]int{},
	}
	return result
}

func buildTableFromQueryWithWarning(aQuery *query.Select, x node.Node, routeOpt *option.Route, comment string) *option.Table {
	aTable := buildTableWithWarning(x, routeOpt, comment)
	aTable.Columns = selectItemToColumn(aQuery, routeOpt)
	return aTable
}

func selectItemToColumn(query *query.Select, route *option.Route) option.Columns {
	var result []*option.Column
	for _, item := range query.List {
		appendItem(item, &result, route)
	}
	return result
}

func buildTableWithWarning(x node.Node, routeOpt *option.Route, comment string) *option.Table {
	aTable, err := buildTable(x, routeOpt)
	if err != nil {
		fmt.Printf("[WARN] couldn't build full table representation %v\n", aTable.Name)
	}

	if err = tryUnmarshalHint(comment, &aTable.TableMeta); err != nil {
		fmt.Printf("[WARN] couldn't parse table hint to option.Table: %v\n", comment)
	}

	return aTable
}

func buildTable(x node.Node, routeOpt *option.Route) (*option.Table, error) {
	table := option.NewTable("")

	switch actual := x.(type) {
	case *expr.Raw:
		_, SQL := extractTableSQL(actual)
		table.SQL = SQL
		if err := UpdateTableSettings(table, routeOpt); err != nil {
			return table, err
		}

	case *expr.Selector, *expr.Ident:
		name, _ := extractTableName(actual)
		table.Name = name
	}

	return table, nil
}

func extractTableName(node node.Node) (name string, SQL string) {
	switch actual := node.(type) {
	case *expr.Selector, *expr.Ident:
		return parser.Stringify(actual), ""
	}

	return "", ""
}

func extractTableSQL(actual *expr.Raw) (name string, SQL string) {
	SQL = strings.TrimSpace(actual.Raw)
	trimmedParentheses := true
	for len(SQL) >= 2 && trimmedParentheses {
		if SQL[0] == '(' && SQL[len(SQL)-1] == ')' {
			SQL = SQL[1 : len(SQL)-1]
		} else {
			trimmedParentheses = false
		}

		SQL = strings.TrimSpace(SQL)
	}

	return "", SQL
}

func UpdateTableSettings(table *option.Table, routeOpt *option.Route) error {
	tableSQL := expandConsts(table.SQL, routeOpt)
	innerSQL, _ := ExtractCondBlock(tableSQL)
	innerQuery, err := parser.ParseQuery(innerSQL)
	fmt.Printf("innerSQL %v %v\n", tableSQL, err)

	if innerQuery != nil && innerQuery.From.X != nil {
		table.Name = strings.Trim(parser.Stringify(innerQuery.From.X), "`")
		table.Inner = selectItemToColumn(innerQuery, routeOpt)
		if len(innerQuery.Joins) > 0 {
			for _, join := range innerQuery.Joins {
				table.Deps[option.Alias(join.Alias)] = option.TableName(strings.Trim(parser.Stringify(join.With), "`"))
			}
		}

		table.InnerAlias = innerQuery.From.Alias
	}

	if err != nil {
		return err
	}

	return nil
}

func expandConsts(SQL string, opt *option.Route) string {
	replacementMap := rdata.Map{}
	for key := range opt.Const {
		replacementMap.SetValue(key, opt.Const[key])
	}

	return replacementMap.ExpandAsText(SQL)
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

func relationKeyOf(parentTable *option.Table, relTable *option.Table, join *query.Join) (*relationKey, error) {
	x := extractSelector(join.On.X, true)
	y := extractSelector(join.On.X, false)

	actualTableName := view.NotEmptyOf(parentTable.OuterAlias, parentTable.Name)
	if actualTableName != y.Name && actualTableName != x.Name {
		return nil, fmt.Errorf("unknow view alias: %v %v", actualTableName, parentTable.Name)
	}

	if actualTableName == y.Name {
		y, x = x, y
	}

	ownerKey, err := newKey(x, parentTable)
	if err != nil {
		return nil, err
	}

	childKey, err := newKey(y, relTable)
	if err != nil {
		return nil, err
	}

	return &relationKey{
		owner: ownerKey,
		child: childKey,
	}, nil
}

func newKey(s *expr.Selector, table *option.Table) (*key, error) {
	alias := table.InnerAlias
	tableName := table.Name
	byAlias := table.Inner.ByAlias()

	colName := parser.Stringify(s.X)
	field := colName

	if len(byAlias) > 0 {
		actualColumn, ok := fieldName(byAlias, colName)
		if !ok && !table.HasStarExpr(alias) {
			return nil, fmt.Errorf("key %s is not listed on %v", colName, tableName)
		}

		if ok {
			colName = view.NotEmptyOf(actualColumn, colName)
		}
	}

	return &key{
		Column: colName,
		Field:  field,
		Alias:  alias,
	}, nil
}

func fieldName(byAlias map[string]*option.Column, alias string) (string, bool) {
	aliasLc := strings.ToLower(alias)
	column, ok := byAlias[aliasLc]
	if !ok {
		//return "", "", fmt.Errorf("key %s is not listed on %v", alias, relTable.Name)
		return "", false
	}

	if column.Name != aliasLc {
		return column.Name, true
	}

	return alias, true
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

func IsSQLExecMode(SQL string) bool {
	lcSQL := strings.ToLower(SQL)
	return strings.Contains(lcSQL, "call") ||
		(strings.Contains(lcSQL, "begin") && strings.Contains(lcSQL, "end")) ||
		isUpdate(lcSQL) ||
		isDelete(lcSQL) ||
		isInsert(lcSQL)
}

func isDelete(lcSQL string) bool {
	return strings.Contains(lcSQL, "delete ") && strings.Contains(lcSQL, "from ")
}

func isUpdate(lcSQL string) bool {
	return strings.Contains(lcSQL, "update ") && strings.Contains(lcSQL, "set ")
}

func isInsert(lcSQL string) bool {
	return strings.Contains(lcSQL, "insert ") && strings.Contains(lcSQL, "into ") && strings.Contains(lcSQL, "values")
}

func ExtractCondBlock(SQL string) (string, []string) {
	builder := new(bytes.Buffer)
	var expressions []string
	cursor := parsly.NewCursor("", []byte(SQL), 0)
outer:
	for i := 0; i < len(cursor.Input); i++ {
		match := cursor.MatchOne(condBlockMatcher)
		switch match.Code {
		case parsly.EOF:
			break outer
		case condBlockToken:

			block := match.Text(cursor)[3:]
			cur := parsly.NewCursor("", []byte(block), 0)
			match = cur.MatchAfterOptional(whitespaceMatcher, exprGroupMatcher)
			if match.Code == exprGroupToken {
				matched := string(cur.Input[cur.Pos:])
				if index := strings.Index(matched, "#"); index != -1 {
					expression := strings.TrimSpace(matched[:index])
					expressions = append(expressions, expression)
					if strings.Contains(expression, "=") {
						builder.WriteString(expression)
					}
				}
			}

		default:
			builder.WriteByte(cursor.Input[cursor.Pos])
			cursor.Pos++
		}
	}
	return builder.String(), expressions
}
