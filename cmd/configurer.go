package cmd

import (
	"bytes"
	"fmt"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/router"
	"github.com/viant/datly/template/columns"
	"github.com/viant/datly/template/sanitize"
	"github.com/viant/datly/view"
	"github.com/viant/parsly"
	"github.com/viant/sqlx/metadata/ast/expr"
	"github.com/viant/sqlx/metadata/ast/node"
	"github.com/viant/sqlx/metadata/ast/parser"
	"github.com/viant/sqlx/metadata/ast/query"
	rdata "github.com/viant/toolbox/data"
	"net/http"
	"os"
	"sort"
	"strings"
)

type ViewConfigurer struct {
	tables       map[string]*Table
	aView        *viewConfig
	relations    []*Relation
	mainViewName string
	serviceType  router.ServiceType
	hints        map[string]*sanitize.ParameterHint
	consts       map[string]interface{}
	viewParams   []*viewParamConfig
}

func (c *ViewConfigurer) ViewConfig() *viewConfig {
	return c.aView
}

func (c *ViewConfigurer) ViewParams() []*viewParamConfig {
	return c.viewParams
}

func (c *ViewConfigurer) DefaultHTTPMethod() string {
	switch c.serviceType {
	case router.ExecutorServiceType:
		return http.MethodPost
	}

	return http.MethodGet
}

func NewConfigProviderReader(mainViewName string, SQL string, routeOpt *option.RouteConfig, hints map[string]*sanitize.ParameterHint, serviceType router.ServiceType, consts map[string]interface{}) (*ViewConfigurer, error) {
	result := &ViewConfigurer{
		tables:       map[string]*Table{},
		mainViewName: mainViewName,
		serviceType:  serviceType,
		hints:        hints,
		consts:       consts,
	}

	return result, result.Init(SQL, routeOpt, hints)
}

func (c *ViewConfigurer) OutputConfig() (string, error) {
	startExpr := c.aView.unexpandedTable.Columns.StarExpr(c.aView.unexpandedTable.HolderName)
	if startExpr == nil {
		return "", nil
	}

	return startExpr.Comments, nil
}

func (c *ViewConfigurer) Init(SQL string, opt *option.RouteConfig, hints map[string]*sanitize.ParameterHint) error {
	config, viewParams, err := c.buildViewConfig(c.serviceType, c.mainViewName, SQL, opt, nil)
	if err != nil {
		return err
	}

	hintedViewParams, err := c.extractViewParamsFromHints(hints, opt)
	if err != nil {
		return err
	}

	c.aView = config
	c.viewParams = append(viewParams, hintedViewParams...)
	return nil
}

func (c *ViewConfigurer) registerTable(table *Table) {
	c.registerTableWithKey(table.HolderName, table)
}

func (c *ViewConfigurer) registerTableWithKey(key string, table *Table) {
	c.tables[key] = table
}

func isMetaTable(candidate string) bool {
	return strings.Contains(candidate, "$View.") && strings.Contains(candidate, ".SQL")
}

func tryUnmrashalHintWithWarn(hint string, any interface{}) {
	hint = strings.TrimSpace(hint)
	if hint == "" {
		return
	}

	err := tryUnmarshalHint(hint, any)
	if err != nil {
		fmt.Printf("[WARN] couldn't unmarshal %v into %T due to the %v\n", hint, any, err.Error())
	}
}

func tryUnmarshalHint(hint string, any interface{}) error {
	if hint == "" {
		return nil
	}

	return hintToStruct(hint, any)
}

func getMetaTemplateHolder(name string) string {
	var viewNs = "$View."
	index := strings.Index(name, viewNs)
	name = name[index+len(viewNs):]
	index = strings.Index(name, ".SQL")
	return name[:index]
}

func (c *ViewConfigurer) buildViewConfig(serviceType router.ServiceType, viewName string, SQL string, opt *option.RouteConfig, parent *query.Join) (*viewConfig, []*viewParamConfig, error) {
	config, viewParams, err := c.prepareViewConfig(serviceType, viewName, SQL, opt, parent)

	return config, viewParams, err
}

func (c *ViewConfigurer) prepareViewConfig(serviceType router.ServiceType, viewName string, SQL string, opt *option.RouteConfig, parent *query.Join) (*viewConfig, []*viewParamConfig, error) {
	if serviceType == router.ReaderServiceType {
		return c.buildReaderViewConfig(viewName, SQL, opt, parent)
	}

	config, err := c.buildExecViewConfig(viewName, SQL)
	return config, nil, err
}

func (c *ViewConfigurer) buildExecViewConfig(viewName string, templateSQL string) (*viewConfig, error) {
	table := &Table{
		SQL: templateSQL,
	}
	aConfig := newViewConfig(viewName, viewName, nil, table, nil, view.SQLExecMode)

	lcSQL := strings.ToLower(templateSQL)
	boundary := getStatementBoundary(lcSQL)

	var resultErr error
	for i := 1; i < len(boundary); i++ {
		offset := boundary[i-1]
		limit := len(templateSQL)
		if i+1 < len(boundary) {
			limit = boundary[i+1] - 1
		}

		if err := updateExecViewConfig(templateSQL[offset], templateSQL[offset:limit], aConfig); err != nil {
			resultErr = err
		}
	}

	return aConfig, resultErr
}

func (c *ViewConfigurer) buildReaderViewConfig(viewName string, SQL string, opt *option.RouteConfig, parent *query.Join) (*viewConfig, []*viewParamConfig, error) {
	result, dataViewParams, err := c.prepareUnexpanded(viewName, SQL, opt, parent)
	if err != nil {
		return nil, nil, err
	}

	expandMap, err := buildExpandMap(c.hints, c.consts)
	if err != nil {
		return nil, nil, err
	}

	expandedTable, err := buildExpandedTable(viewName, result.unexpandedTable, expandMap, opt)
	if err != nil {
		return nil, nil, err
	}

	result.expandedTable = expandedTable
	return result, dataViewParams, err
}

func buildExpandedTable(viewName string, table *Table, expandMap rdata.Map, opt *option.RouteConfig) (*Table, error) {
	if len(expandMap) == 0 {
		return table, nil
	}

	aQuery, err := parser.ParseQuery(expandMap.ExpandAsText(table.SQL))
	if err != nil {
		fmt.Printf("[WARN] couldn't parse epanded SQL for %v\n", viewName)
	}
	return buildTableFromQueryWithWarning(aQuery, &expr.Raw{Raw: table.SQL}, opt, aQuery.From.Comments), nil
}

func buildExpandMap(hints map[string]*sanitize.ParameterHint, consts map[string]interface{}) (rdata.Map, error) {
	result := rdata.Map{}
	for _, aHint := range hints {
		actual, _ := sanitize.SplitHint(aHint.Hint)
		aParam := &Parameter{}
		if err := tryUnmarshalHint(actual, aParam); err != nil {
			return nil, err
		}

		if aParam.Kind != string(view.EnvironmentKind) {
			continue
		}

		result.SetValue(aHint.Parameter, os.Getenv(aHint.Parameter))
	}

	for constName := range consts {
		result.SetValue(constName, consts[constName])
	}

	return result, nil
}

func (c *ViewConfigurer) prepareUnexpanded(viewName string, SQL string, opt *option.RouteConfig, parent *query.Join) (*viewConfig, []*viewParamConfig, error) {
	aQuery, err := parser.ParseQuery(SQL)
	if err != nil {
		fmt.Printf("[WARN] couldn't parse properly SQL for %v\n", viewName)
	}

	joins, ok := sqlxJoins(aQuery, opt)
	if !ok {
		aTable := buildTableFromQueryWithWarning(aQuery, expr.NewRaw(SQL), opt, aQuery.From.Comments)
		result := newViewConfig(viewName, viewName, parent, aTable, nil, view.SQLQueryMode)
		var namespaceSource string
		if columns.CanBeTableName(aTable.Name) {
			namespaceSource = aTable.Name
		} else {
			namespaceSource = aQuery.From.Alias
		}

		aTable.NamespaceSource = namespaceSource
		return result, nil, nil
	}

	aTable := buildTableFromQueryWithWarning(aQuery, aQuery.From.X, opt, aQuery.From.Comments)
	aTable.HolderName = view.FirstNotEmpty(aQuery.From.Alias, aTable.HolderName)
	aTable.NamespaceSource = aTable.HolderName

	if columns.CanBeTableName(aTable.Name) {
		aTable.NamespaceSource = aTable.Name //for the relations, it will be adjusted later
	}

	result := newViewConfig(viewName, view.FirstNotEmpty(aQuery.From.Alias, viewName), parent, aTable, nil, view.SQLQueryMode)

	var dataViewParams []*viewParamConfig
	for _, join := range joins {
		innerTable := buildTableWithWarning(join.With, opt, join.Comments)
		relViewConfig, childViewParams, err := c.buildViewConfigWithTable(join, innerTable, opt)
		dataViewParams = append(dataViewParams, childViewParams...)
		if err != nil {
			return nil, nil, err
		}

		if err = tryUnmarshalHint(join.Comments, &relViewConfig.unexpandedTable.ViewConfig); err != nil {
			return nil, nil, err
		}

		relViewConfig.unexpandedTable.HolderName = join.Alias
		relViewConfig.expandedTable.HolderName = join.Alias
		if isMetaTable(relViewConfig.unexpandedTable.Name) {
			holder := getMetaTemplateHolder(relViewConfig.unexpandedTable.Name)
			result.AddMetaTemplate(join.Alias, holder, relViewConfig.unexpandedTable)
		} else if isParamPredicate(parser.Stringify(join.On.X)) || relViewConfig.unexpandedTable.ViewConfig.DataViewParameter != nil {
			paramOption := &Parameter{}
			relViewConfig.fileName = join.Alias

			tryUnmrashalHintWithWarn(join.Comments, paramOption)
			dataViewParams = append(dataViewParams, &viewParamConfig{
				viewConfig: relViewConfig,
				params:     []*Parameter{paramOption},
			})

		} else {
			relViewConfig.aKey, err = relationKeyOf(aTable, innerTable, join)
			if err != nil {
				return nil, nil, err
			}

			result.AddRelation(relViewConfig)
		}
	}

	for _, item := range aQuery.List {
		asStarExpr, ok := item.Expr.(*expr.Star)
		if !ok {
			continue
		}

		holder := c.getAlias(asStarExpr)
		if holder == "" {
			continue
		}

		if config, ok := result.ViewConfig(holder); ok {
			tryUnmrashalHintWithWarn(asStarExpr.Comments, &config.outputConfig)
		}

		if metaConfig, ok := result.metaConfigByName(holder); ok {
			metaConfig.except = asStarExpr.Except
		}
	}

	return result, dataViewParams, nil
}

func (c *ViewConfigurer) getAlias(asStarExpr *expr.Star) string {
	stringify := parser.Stringify(asStarExpr.X)
	if index := strings.Index(stringify, "."); index != -1 {
		return stringify[:index]
	}

	return ""
}

func (c *ViewConfigurer) buildViewConfigWithTable(join *query.Join, innerTable *Table, opt *option.RouteConfig) (*viewConfig, []*viewParamConfig, error) {
	if strings.TrimSpace(innerTable.SQL) == "" {
		return newViewConfig(join.Alias, join.Alias, join, innerTable, nil, view.SQLQueryMode), nil, nil
	}

	config, viewParams, err := c.buildViewConfig(c.serviceType, join.Alias, innerTable.SQL, opt, join)
	if config != nil {
		config.unexpandedTable = innerTable
	}

	return config, viewParams, err
}

func sqlxJoins(aQuery *query.Select, opt *option.RouteConfig) ([]*query.Join, bool) {
	if isSQLXRelation(aQuery.From.X) {
		items := selectItemToColumn(aQuery, opt)

		return aQuery.Joins, items.StarExpr(aQuery.From.Alias) != nil
	}

	var result []*query.Join
	for i, join := range aQuery.Joins {
		if isSQLXRelation(join.With) {
			result = append(result, aQuery.Joins[i])
		}
	}

	return result, len(result) != 0
}

func isSQLXRelation(rel node.Node) bool {
	if rel == nil {
		return false
	}

	candidate := parser.Stringify(rel)
	return columns.ContainsSelect(candidate) || !columns.CanBeTableName(candidate)
}

func (c *ViewConfigurer) extractViewParamsFromHints(hints map[string]*sanitize.ParameterHint, opt *option.RouteConfig) ([]*viewParamConfig, error) {
	var viewParams []*viewParamConfig
	for _, hint := range hints {
		aHint, sql := sanitize.SplitHint(hint.Hint)
		if strings.TrimSpace(sql) == "" {
			continue
		}

		param := &Parameter{}
		tryUnmrashalHintWithWarn(aHint, param)

		aViewConfig, childViewParams, err := c.buildViewConfig(router.ReaderServiceType, hint.Parameter, sql, opt, nil)
		if err != nil {
			return nil, err
		}

		viewParams = append(viewParams, newViewParamConfig(aViewConfig, param))

		viewParams = append(viewParams, childViewParams...)
	}

	return viewParams, nil
}

func newViewParamConfig(aViewConfig *viewConfig, param ...*Parameter) *viewParamConfig {
	return &viewParamConfig{
		viewConfig: aViewConfig,
		params:     param,
	}
}

func updateExecViewConfig(stmtType byte, SQLStmt string, view *viewConfig) error {
	rawSQL := RemoveCondBlocks(SQLStmt)

	switch stmtType | ' ' {
	case 'i':
		stmt, err := parser.ParseInsert(rawSQL)
		if err != nil {
			return err
		}
		inheritFromTarget(stmt.Target.X, view, stmt.Target.Comments)

	case 'u':
		stmt, err := parser.ParseUpdate(rawSQL)
		if err != nil {
			return err
		}

		inheritFromTarget(stmt.Target.X, view, stmt.Target.Comments)

	case 'd':
		stmt, err := parser.ParseDelete(rawSQL)
		if err != nil {
			return err
		}

		inheritFromTarget(stmt.Target.X, view, stmt.Target.Comments)
	}

	return nil
}

func inheritFromTarget(target node.Node, view *viewConfig, tableNameComment string) {
	tableName := parser.Stringify(target)
	view.ensureTableName(tableName)
	view.ensureOuterAlias(tableName)
	view.ensureInnerAlias(tableName)
	view.ensureFileName(tableName)
	view.parseComment(tableNameComment)
}

func (c *viewConfig) parseComment(comment string) {
	hint, _ := sanitize.SplitHint(comment)
	tryUnmrashalHintWithWarn(hint, &c.expandedTable.ViewConfig)
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

	if len(boundary) == 0 {
		boundary = append(boundary, 0)
	}

	if len(boundary) == 1 {
		boundary = append(boundary, len(lcSQL))
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

func RemoveCondBlocks(SQL string) string {
	builder := new(bytes.Buffer)
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
	return builder.String()
}
