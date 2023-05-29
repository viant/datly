package cmd

import (
	"bytes"
	"context"
	"fmt"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/config"
	"github.com/viant/datly/router"
	"github.com/viant/datly/template/columns"
	"github.com/viant/datly/template/sanitize"
	"github.com/viant/datly/view"
	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	rdata "github.com/viant/toolbox/data"
	expr2 "github.com/viant/velty/ast/expr"
	"net/http"
	"os"
	"strings"
)

type ViewConfigurer struct {
	tables       map[string]*Table
	aView        *ViewConfig
	relations    []*Relation
	mainViewName string
	serviceType  router.ServiceType
	viewParams   []*ViewParamConfig
	paramIndex   *ParametersIndex
	prepare      *Prepare
	connectors   *Connector
}

func (c *ViewConfigurer) ViewConfig() *ViewConfig {
	return c.aView
}

func (c *ViewConfigurer) ViewParams() []*ViewParamConfig {
	return c.viewParams
}

func (c *ViewConfigurer) DefaultHTTPMethod() string {
	switch c.serviceType {
	case router.ExecutorServiceType:
		return http.MethodPost
	}

	return http.MethodGet
}

func NewConfigProviderReader(mainViewName string, SQL string, routeOpt *option.RouteConfig, serviceType router.ServiceType, index *ParametersIndex, prepare *Prepare, connectors *Connector, builder *routeBuilder) (*ViewConfigurer, error) {
	result := &ViewConfigurer{
		tables:       map[string]*Table{},
		mainViewName: mainViewName,
		serviceType:  serviceType,
		paramIndex:   index,
		prepare:      prepare,
		connectors:   connectors,
	}

	if err := result.Init(SQL, routeOpt); err != nil {
		return nil, err
	}

	builder.session.setMainViewConfig(result.ViewConfig())
	return result, nil
}

func (c *ViewConfigurer) OutputConfig() (string, error) {
	startExpr := c.aView.unexpandedTable.Columns.StarExpr(c.aView.unexpandedTable.HolderName)
	if startExpr == nil {
		return "", nil
	}

	return startExpr.Comments, nil
}

func (c *ViewConfigurer) Init(SQL string, opt *option.RouteConfig) error {
	aConfig, viewParams, err := c.buildViewConfig(c.serviceType, c.mainViewName, SQL, opt, nil)
	if err != nil {
		return err
	}

	hintedViewParams, err := c.preProcessHints(opt)
	if err != nil {
		return err
	}

	c.inheritMainViewConfig(aConfig, opt)
	c.aView = aConfig
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
	hint = strings.TrimSpace(hint)
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

func (c *ViewConfigurer) buildViewConfig(serviceType router.ServiceType, viewName string, SQL string, opt *option.RouteConfig, parent *query.Join) (*ViewConfig, []*ViewParamConfig, error) {
	aConfig, viewParams, err := c.prepareViewConfig(serviceType, viewName, SQL, opt, parent)

	return aConfig, viewParams, err
}

func (c *ViewConfigurer) prepareViewConfig(serviceType router.ServiceType, viewName string, SQL string, opt *option.RouteConfig, parent *query.Join) (*ViewConfig, []*ViewParamConfig, error) {
	if serviceType == router.ReaderServiceType {
		return c.buildReaderViewConfig(viewName, SQL, opt, parent)
	}

	aConfig, err := c.buildExecViewConfig(viewName, SQL)
	return aConfig, nil, err
}

func (c *ViewConfigurer) buildExecViewConfig(viewName string, templateSQL string) (*ViewConfig, error) {
	table := &Table{
		SQL: templateSQL,
	}

	aConfig := newViewConfig(viewName, viewName, nil, table, nil, view.SQLExecMode)
	statements := GetStatements(templateSQL)
	batchInsertEnabled := map[string]bool{}
	batchInsertDisabled := map[string]bool{}

	var resultErr []error
	for _, statement := range statements {
		tableName, err := c.updateConfigWithExecStmt(statement, aConfig, batchInsertEnabled, templateSQL)
		if err != nil {
			resultErr = append(resultErr, err)
		}

		if statement.Selector != nil && tableName != "" {
			batchInsertEnabled[tableName] = true
		} else {
			tables, err := c.findDependantTables(tableName)
			if err != nil {
				resultErr = append(resultErr, err)
			}

			for _, disabled := range tables {
				batchInsertDisabled[disabled] = true
			}
		}
	}

	for tableName := range batchInsertEnabled {
		if batchInsertDisabled[tableName] {
			delete(batchInsertEnabled, tableName)
		}
	}

	for _, err := range resultErr {
		if resultErr != nil {
			fmt.Printf("[WARN] couldn't create update table ast representation: %v\n", err.Error())
		}
	}

	aConfig.batchEnabled = batchInsertEnabled
	return aConfig, nil
}

func (c *ViewConfigurer) updateConfigWithExecStmt(statement *Statement, aConfig *ViewConfig, batchInsertEnabled map[string]bool, templateSQL string) (string, error) {
	if statement.Selector != nil {
		call, ok := statement.Selector.X.(*expr2.Call)
		if !ok {
			return "", fmt.Errorf("expected tu got func call on %v but got %T", statement.Selector.ID, statement.Selector.X)
		}

		if len(call.Args) < 2 {
			return "", fmt.Errorf("expected to got 2 args for %v but got %v", statement.Selector.ID, len(call.Args))
		}

		asStringLiteral, ok := call.Args[1].(*expr2.Literal)
		if !ok {
			return "", fmt.Errorf("expected tu got %T on Args[1] but got %T", asStringLiteral, call.Args[1])
		}

		inheritFromTableName(aConfig, asStringLiteral.Value)
		if statement.SelectorMethod == "Insert" {
			batchInsertEnabled[asStringLiteral.Value] = true
		}

		return asStringLiteral.Value, nil
	} else {
		return updateExecViewConfig(templateSQL[statement.Start], templateSQL[statement.Start:statement.End], aConfig)
	}
}

func (c *ViewConfigurer) buildReaderViewConfig(viewName string, SQL string, opt *option.RouteConfig, parent *query.Join) (*ViewConfig, []*ViewParamConfig, error) {
	result, dataViewParams, err := c.prepareUnexpanded(viewName, SQL, opt, parent)
	if err != nil {
		return nil, nil, err
	}

	expandMap, err := buildExpandMap(c.paramIndex)
	if err != nil {
		return nil, nil, err
	}

	expandedTable, err := c.buildExpandedTable(viewName, result.unexpandedTable, expandMap, opt)
	if err != nil {
		return nil, nil, err
	}

	expandedTable.ViewConfig = result.unexpandedTable.ViewConfig
	result.expandedTable = expandedTable
	return result, dataViewParams, err
}

func (c *ViewConfigurer) buildExpandedTable(viewName string, table *Table, expandMap rdata.Map, opt *option.RouteConfig) (*Table, error) {
	if len(expandMap) == 0 {
		return table, nil
	}

	aQuery, err := sqlparser.ParseQuery(expandMap.ExpandAsText(table.SQL))
	if err != nil {
		fmt.Printf("[WARN] couldn't parse epanded SQL for %v\n", viewName)
	}
	return c.buildTableFromQueryWithWarning(aQuery, &expr.Raw{Raw: table.SQL}, opt, aQuery.From.Comments), nil
}

func buildExpandMap(paramsIndex *ParametersIndex) (rdata.Map, error) {
	hints := paramsIndex.hints
	consts := paramsIndex.consts

	result := rdata.Map{}
	for paramName, aHint := range hints {
		aParam, err := paramsIndex.ParamsMetaWithHint(paramName, aHint)
		if err != nil {
			return nil, err
		}

		if aParam.Kind != string(view.KindEnvironment) {
			continue
		}

		result.SetValue(aHint.Parameter, os.Getenv(aHint.Parameter))
	}

	for constName := range consts {
		result.SetValue(constName, consts[constName])
	}

	return result, nil
}

func (c *ViewConfigurer) prepareUnexpanded(viewName string, SQL string, opt *option.RouteConfig, parent *query.Join) (*ViewConfig, []*ViewParamConfig, error) {
	boundary := GetStatements(SQL)
	if len(boundary) == 0 {
		return nil, nil, fmt.Errorf("not found select in %v", SQL)
	}

	parsableSQL := SQL[boundary[0].Start:]

	aQuery, err := sqlparser.ParseQuery(parsableSQL)
	if err != nil {
		fmt.Printf("[WARN] couldn't parse properly SQL for %v\n", viewName)
	}

	joins, ok := sqlxJoins(aQuery, opt)
	if !ok {
		aTable := c.buildTableFromQueryWithWarning(aQuery, expr.NewRaw(parsableSQL), opt, aQuery.From.Comments)
		aTable.SQL = SQL
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

	aTable := c.buildTableFromQueryWithWarning(aQuery, aQuery.From.X, opt, aQuery.From.Comments)
	aTable.HolderName = view.FirstNotEmpty(aQuery.From.Alias, aTable.HolderName)
	aTable.NamespaceSource = aTable.HolderName

	if columns.CanBeTableName(aTable.Name) {
		aTable.NamespaceSource = aTable.Name //for the relations, it will be adjusted later
	}

	result := newViewConfig(viewName, view.FirstNotEmpty(aQuery.From.Alias, viewName), parent, aTable, nil, view.SQLQueryMode)

	var dataViewParams []*ViewParamConfig
	tables := map[string]*Table{}

	tables[aTable.HolderName] = aTable
	for _, join := range joins {

		innerTable := c.buildTableWithWarning(join.With, opt, join.Comments)
		relViewConfig, childViewParams, err := c.buildViewConfigWithTable(join, innerTable, opt, join.Comments)
		relViewConfig.parent = result
		dataViewParams = append(dataViewParams, childViewParams...)
		if err != nil {
			return nil, nil, err
		}

		relViewConfig.unexpandedTable.HolderName = join.Alias
		relViewConfig.expandedTable.HolderName = join.Alias
		if isMetaTable(relViewConfig.unexpandedTable.Name) {
			holder := getMetaTemplateHolder(relViewConfig.unexpandedTable.Name)
			result.AddMetaTemplate(join.Alias, holder, relViewConfig.unexpandedTable)
		} else if isParamPredicate(sqlparser.Stringify(join.On.X)) || relViewConfig.unexpandedTable.ViewConfig.DataViewParameter != nil {
			relViewConfig.fileName = join.Alias
			paramOption, err := c.paramIndex.ParamsMetaWithComment(relViewConfig.viewName, join.Comments)
			if err != nil {
				return nil, nil, err
			}

			dataViewParams = append(dataViewParams, &ViewParamConfig{
				viewConfig: relViewConfig,
				params:     []*Parameter{paramOption},
			})

		} else {
			tables[innerTable.HolderName] = innerTable
			relViewConfig.aKey, err = relationKeyOf(aTable, innerTable, join, tables)
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
	stringify := sqlparser.Stringify(asStarExpr.X)
	if index := strings.Index(stringify, "."); index != -1 {
		return stringify[:index]
	}

	return ""
}

func (c *ViewConfigurer) buildViewConfigWithTable(join *query.Join, innerTable *Table, opt *option.RouteConfig, comments string) (*ViewConfig, []*ViewParamConfig, error) {
	if strings.TrimSpace(innerTable.SQL) == "" {
		return newViewConfig(join.Alias, join.Alias, join, innerTable, nil, view.SQLQueryMode), nil, nil
	}

	aConfig, viewParams, err := c.buildViewConfig(router.ReaderServiceType, join.Alias, innerTable.SQL, opt, join)
	if aConfig != nil {
		aConfig.unexpandedTable = innerTable
	}

	return aConfig, viewParams, err
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

	candidate := sqlparser.Stringify(rel)
	return columns.ContainsSelect(candidate) || !columns.CanBeTableName(candidate)
}

func (c *ViewConfigurer) preProcessHints(opt *option.RouteConfig) ([]*ViewParamConfig, error) {
	hints := c.paramIndex.hints

	var viewParams []*ViewParamConfig
	for paramName, hint := range hints {
		param, err := c.paramIndex.ParamsMetaWithHint(paramName, hint)
		if err != nil {
			return nil, err
		}

		if param.SQLCodec || param.SQL == "" {
			continue
		}

		aViewConfig, childViewParams, err := c.buildViewConfig(router.ReaderServiceType, hint.Parameter, param.SQL, opt, nil)
		if err != nil {
			return nil, err
		}

		viewParams = append(viewParams, newViewParamConfig(aViewConfig, param))
		viewParams = append(viewParams, childViewParams...)
	}

	return viewParams, nil
}

func (c *ViewConfigurer) findDependantTables(tableName string) ([]string, error) {
	if len(c.connectors.Connectors()) == 0 {
		return nil, fmt.Errorf("not found any connector")
	}

	db, err := c.connectors.Connectors()[0].DB()
	if err != nil {
		return nil, err
	}

	keys, err := readForeignKeys(context.TODO(), db, tableName)
	if err != nil {
		return nil, err
	}

	var tables []string
	for _, aKey := range keys {
		tables = append(tables, aKey.ReferenceTable)
	}

	return tables, nil
}

func (c *ViewConfigurer) inheritMainViewConfig(aConfig *ViewConfig, opt *option.RouteConfig) {
	if aConfig.outputConfig.Field == "" {
		aConfig.outputConfig.Field = opt.Field
	}
}

func isSQLLikeCodec(codec string) bool {
	switch strings.ToLower(codec) {
	case config.CodecStructql:
		return true
	}

	return false
}

func newViewParamConfig(aViewConfig *ViewConfig, param ...*Parameter) *ViewParamConfig {
	return &ViewParamConfig{
		viewConfig: aViewConfig,
		params:     param,
	}
}

func updateExecViewConfig(stmtType byte, SQLStmt string, view *ViewConfig) (string, error) {
	rawSQL := RemoveCondBlocks(SQLStmt)

	switch stmtType | ' ' {
	case 'i':
		stmt, err := sqlparser.ParseInsert(rawSQL)

		tableName := ""
		if stmt != nil {
			tableName = sqlparser.Stringify(stmt.Target.X)
			inheritFromTarget(stmt.Target.X, view, stmt.Target.Comments)
		}

		return tableName, err

	case 'u':
		stmt, err := sqlparser.ParseUpdate(rawSQL)
		tableName := ""
		if stmt != nil {
			inheritFromTarget(stmt.Target.X, view, stmt.Target.Comments)
			tableName = sqlparser.Stringify(stmt.Target.X)
		}

		return tableName, err
	case 'd':
		stmt, err := sqlparser.ParseDelete(rawSQL)
		tableName := ""
		if stmt != nil {
			tableName = sqlparser.Stringify(stmt.Target.X)
			inheritFromTarget(stmt.Target.X, view, stmt.Target.Comments)
		}

		return tableName, err
	}

	return "", nil
}

func inheritFromTarget(target node.Node, view *ViewConfig, tableNameComment string) {
	tableName := sqlparser.Stringify(target)
	inheritFromTableName(view, tableName)
	view.parseComment(tableNameComment)
}

func inheritFromTableName(view *ViewConfig, tableName string) {
	view.ensureTableName(tableName)
	view.ensureOuterAlias(tableName)
	view.ensureInnerAlias(tableName)
	view.ensureFileName(tableName)
}

func (c *ViewConfig) parseComment(comment string) {
	hint, _ := sanitize.SplitHint(comment)
	tryUnmrashalHintWithWarn(hint, &c.expandedTable.ViewConfig)
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
