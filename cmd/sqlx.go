package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/view"
	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	rdata "github.com/viant/toolbox/data"
	"regexp"
	"strconv"
	"strings"
)

var whiteSpaces = []byte{' ', '\n', '\t', '\r', '\v', '\f', 0x85, 0xA0}

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

func newViewConfig(viewName string, fileName string, parent *query.Join, aTable *Table, templateMeta *Table, mode view.Mode) *viewConfig {
	var metaConfig *templateMetaConfig
	if templateMeta != nil {
		metaConfig = &templateMetaConfig{table: templateMeta}
	}

	result := &viewConfig{
		viewName:        viewName,
		queryJoin:       parent,
		unexpandedTable: aTable,
		expandedTable:   aTable,
		fileName:        fileName,
		metasBuffer:     map[string]*Table{},
		templateMeta:    metaConfig,
		viewType:        mode,
		relationsIndex:  map[string]int{},
	}
	return result
}

func (c *ViewConfigurer) buildTableFromQueryWithWarning(aQuery *query.Select, x node.Node, routeOpt *option.RouteConfig, comment string) *Table {
	aTable := c.buildTableWithWarning(x, routeOpt, comment)
	aTable.Columns = selectItemToColumn(aQuery, routeOpt)
	return aTable
}

func selectItemToColumn(query *query.Select, route *option.RouteConfig) Columns {
	var result []*Column
	for _, item := range query.List {
		appendItem(item, &result, route)
	}
	return result
}

func (c *ViewConfigurer) buildTableWithWarning(x node.Node, routeOpt *option.RouteConfig, comment string) *Table {
	aTable, err := c.buildTable(x, routeOpt, comment)
	if err != nil {
		fmt.Printf("[WARN] couldn't build full table representation %v\n", aTable.Name)
	}

	if err = tryUnmarshalHint(comment, &aTable.ViewConfig); err != nil {
		fmt.Printf("[WARN] couldn't parse table hint to option.Table: %v\n", comment)
	}

	return aTable
}

func (c *ViewConfigurer) buildTable(x node.Node, routeOpt *option.RouteConfig, comment string) (*Table, error) {
	table := NewTable("")

	if err := tryUnmarshalHint(comment, &table.ViewConfig); err != nil {
		return table, err
	}

	switch actual := x.(type) {
	case *expr.Raw:
		SQLstmt := actual.Raw
		_, SQL := extractTableSQL(SQLstmt)
		SQL = c.runSelectPreprocessor(SQL, table)
		table.SQL = SQL

		if err := UpdateTableSettings(table, routeOpt); err != nil {
			return table, err
		}

	case *expr.Selector, *expr.Ident:
		name, _ := extractTableName(actual)
		table.Name = name
	}

	if c.prepare != nil && table.ViewConfig.ExecKind == "" {
		table.ViewConfig.ExecKind = c.prepare.ExecKind
	}

	return table, nil
}

func (c *ViewConfigurer) runSelectPreprocessor(SQL string, table *Table) string {
	connectorRegex := regexp.MustCompile(`\$DB\[().*\]\.`)
	connectorNameRegex := regexp.MustCompile(`\[().*\]`)

	connectors := connectorRegex.FindAllString(SQL, -1)

	for _, connector := range connectors {
		connName := strings.Trim(connectorNameRegex.FindString(connector), "[]")
		if unqoted, err := strconv.Unquote(connName); err == nil {
			connName = unqoted
		}

		if connName != "" {
			table.Connector = connName
		}

		SQL = strings.Replace(SQL, connector, "", 1)
	}

	return SQL
}

func extractTableName(node node.Node) (name string, SQL string) {
	switch actual := node.(type) {
	case *expr.Selector, *expr.Ident:
		return sqlparser.Stringify(actual), ""
	}

	return "", ""
}

func extractTableSQL(aSQL string) (name string, SQL string) {
	SQL = strings.TrimSpace(aSQL)
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

func UpdateTableSettings(table *Table, routeOpt *option.RouteConfig) error {
	tableSQL := expandConsts(table.SQL, routeOpt)
	innerSQL, _ := ExtractCondBlock(tableSQL)
	innerQuery, err := sqlparser.ParseQuery(innerSQL)
	fmt.Printf("innerSQL %v %v\n", tableSQL, err)

	if innerQuery != nil && innerQuery.From.X != nil {
		table.Name = strings.Trim(sqlparser.Stringify(innerQuery.From.X), "`")
		table.Inner = selectItemToColumn(innerQuery, routeOpt)
		if len(innerQuery.Joins) > 0 {
			for _, join := range innerQuery.Joins {
				table.Deps[Alias(join.Alias)] = TableName(strings.Trim(sqlparser.Stringify(join.With), "`"))
			}
		}

		table.InnerAlias = innerQuery.From.Alias
	}

	if err != nil {
		return err
	}

	return nil
}

func expandConsts(SQL string, opt *option.RouteConfig) string {
	replacementMap := rdata.Map{}
	for key := range opt.Const {
		replacementMap.SetValue(key, opt.Const[key])
	}

	return replacementMap.ExpandAsText(SQL)
}

func hintToStruct(encoded string, aStructPtr interface{}) error {
	encoded = strings.ReplaceAll(encoded, "/*", "")
	encoded = strings.ReplaceAll(encoded, "*/", "")
	encoded = strings.TrimSpace(encoded)
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

func relationKeyOf(parentTable *Table, relTable *Table, join *query.Join) (*relationKey, error) {
	x := extractSelector(join.On.X, true)
	y := extractSelector(join.On.X, false)

	actualTableName := view.FirstNotEmpty(parentTable.HolderName, parentTable.Name)
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

func newKey(s *expr.Selector, table *Table) (*key, error) {
	alias := ""
	tableName := table.Name
	byAlias := table.Inner.ByAlias()

	colName := sqlparser.Stringify(s.X)
	field := colName

	for _, column := range table.Inner {
		if column.Alias == colName {
			alias = column.Ns
			break
		}
	}

	if alias == "" {
		alias = table.InnerAlias
	}

	if len(byAlias) > 0 {
		actualColumn, ok := fieldName(byAlias, colName)
		if !ok && !table.HasStarExpr(alias) {
			return nil, fmt.Errorf("key %s is not listed on %v", colName, tableName)
		}

		if ok {
			colName = view.FirstNotEmpty(actualColumn, colName)
		}
	}

	return &key{
		Column: colName,
		Field:  field,
		Alias:  alias,
	}, nil
}

func fieldName(byAlias map[string]*Column, alias string) (string, bool) {
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
	predicate := sqlparser.Stringify(n)
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

func appendItem(item *query.Item, result *[]*Column, route *option.RouteConfig) {
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

func getColumn(item *query.Item) (*Column, error) {
	switch actual := item.Expr.(type) {
	case *expr.Call:
		call := sqlparser.Stringify(actual)
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

		return &Column{Name: call, Alias: item.Alias, DataType: item.DataType}, nil
	case *expr.Ident:
		return &Column{Name: actual.Name, Alias: item.Alias, DataType: item.DataType}, nil
	case *expr.Selector:
		return &Column{Name: sqlparser.Stringify(actual.X), Ns: actual.Name, DataType: item.DataType, Alias: item.Alias}, nil
	case *expr.Star:
		switch star := actual.X.(type) {
		case *expr.Ident:
			return &Column{Name: star.Name, Except: actual.Except}, nil
		case *expr.Selector:
			return &Column{Name: sqlparser.Stringify(star.X), Ns: star.Name, Except: actual.Except, Comments: actual.Comments}, nil
		}
	case *expr.Literal:
		return &Column{Name: "", Alias: item.Alias, DataType: actual.Kind}, nil
	case *expr.Binary:
		enExpr := sqlparser.Stringify(actual)
		if item.DataType == "" || (strings.Contains(enExpr, "+") || strings.Contains(enExpr, "-") || strings.Contains(enExpr, "/") || strings.Contains(enExpr, "*")) {
			item.DataType = "float64"
		}
		return &Column{Name: enExpr, Alias: item.Alias, DataType: item.DataType}, nil
	case *expr.Parenthesis:
		return &Column{Name: sqlparser.Stringify(actual), Alias: item.Alias, DataType: item.DataType}, nil
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
	statements := GetStatements(SQL)
	for _, statement := range statements {
		if statement.IsExec {
			return true
		}
	}

	return false
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
