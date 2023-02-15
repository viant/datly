package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/plugins"
	"github.com/viant/datly/router"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/template/sanitize"
	"github.com/viant/datly/view"
	"github.com/viant/parsly"
	"github.com/viant/sqlparser/query"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/format"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/parser"
	"github.com/viant/xreflect"
	"go/ast"
	"gopkg.in/yaml.v3"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

type (
	Builder struct {
		tablesMeta      *TableMetaRegistry
		routeBuilder    *routeBuilder
		options         *Options
		config          *standalone.Config
		logger          io.Writer
		fs              afs.Service
		constParameters []*view.Parameter
		fileNames       *uniqueIndex
		viewNames       *uniqueIndex
		types           *uniqueIndex
		plugins         []*pluginGenDeta
		bundles         map[string]string
	}

	routeBuilder struct {
		configProvider *ViewConfigurer
		paramsIndex    *ParametersIndex
		routerResource *router.Resource
		route          *router.Route
		option         *option.RouteConfig
		sqlStmt        string
		views          map[string]*view.View
	}

	viewConfig struct {
		viewName        string
		queryJoin       *query.Join
		unexpandedTable *Table
		outputConfig    option.OutputConfig

		relations      []*viewConfig
		relationsIndex map[string]int
		metasBuffer    map[string]*Table
		templateMeta   *templateMetaConfig
		aKey           *relationKey
		fileName       string
		viewType       view.Mode
		expandedTable  *Table
		batchEnabled   map[string]bool
	}

	templateMetaConfig struct {
		table  *Table
		output *option.OutputConfig
		name   string
		except []string
	}

	viewParamConfig struct {
		viewName string
		viewFile string

		viewConfig *viewConfig
		params     []*Parameter
	}

	uniqueIndex struct {
		taken         map[string]int
		caseSensitive bool
	}

	pluginGenDeta struct {
		URL       string
		filesMeta *xreflect.DirTypes
		Types     []string
		fileURL   string
		mainFile  string
	}
)

func newUniqueIndex(caseSensitive bool) *uniqueIndex {
	return &uniqueIndex{
		taken:         map[string]int{},
		caseSensitive: caseSensitive,
	}
}

func (u *uniqueIndex) unique(value string) string {
	aKey := value
	if !u.caseSensitive {
		aKey = strings.ToLower(aKey)
	}

	counter, ok := u.taken[aKey]
	if !ok {
		u.taken[aKey] = 1
		return value
	}

	u.taken[aKey] = counter + 1
	return value + strconv.Itoa(counter)
}

func (u *uniqueIndex) reserve(value string) error {
	aKey := value
	if !u.caseSensitive {
		aKey = strings.ToLower(aKey)
	}

	_, ok := u.taken[aKey]
	if !ok {
		u.taken[aKey] = 1
		return nil
	}

	return fmt.Errorf("%v is already defined", value)
}

func (b *routeBuilder) AddViews(aView *view.View) {
	b.routerResource.Resource.AddViews(aView)
	if aView.Name != "" {
		b.views[aView.Name] = aView
	}
}

func (c *viewConfig) ensureTableName(tableName string) {
	if c.unexpandedTable.Name != "" {
		return
	}

	c.unexpandedTable.Name = tableName
}

func (c *viewConfig) ensureOuterAlias(alias string) {
	if c.unexpandedTable.HolderName != "" {
		return
	}

	c.unexpandedTable.HolderName = alias
}

func (c *viewConfig) ensureInnerAlias(name string) {
	if c.unexpandedTable.InnerAlias != "" {
		return
	}

	c.unexpandedTable.InnerAlias = name
}

func (c *viewConfig) ensureFileName(name string) {
	if c.fileName != "" {
		return
	}

	c.fileName = name
}

func (c *viewConfig) AddMetaTemplate(metaName string, holder string, config *Table) {
	if c.unexpandedTable.HolderName == holder {
		c.templateMeta = &templateMetaConfig{
			name:  metaName,
			table: config,
		}
		return
	}

	if index, ok := c.relationsIndex[holder]; ok {
		c.relations[index].templateMeta = &templateMetaConfig{
			table: config,
			name:  metaName,
		}

		return
	}

	c.metasBuffer[holder] = config
}

func (c *viewConfig) AddRelation(viewConfig *viewConfig) {
	holderName := viewConfig.unexpandedTable.HolderName

	c.relationsIndex[holderName] = len(c.relations)
	c.relations = append(c.relations, viewConfig)

	if metaConfig, ok := c.metasBuffer[holderName]; ok {
		viewConfig.templateMeta.table = metaConfig
		delete(c.metasBuffer, holderName)
	}
}

func (c *viewConfig) ViewConfig(holder string) (*viewConfig, bool) {
	if holder == c.unexpandedTable.HolderName {
		return c, true
	}

	for _, relation := range c.relations {
		if relation.unexpandedTable.HolderName == holder {
			return relation, true
		}
	}

	return nil, false
}

func (c *viewConfig) metaConfigByName(holder string) (*templateMetaConfig, bool) {
	if c.templateMeta != nil && c.templateMeta.name == holder {
		return c.templateMeta, true
	}

	for _, relation := range c.relations {
		if relation.templateMeta != nil && relation.templateMeta.name == holder {
			return relation.templateMeta, true
		}
	}

	return nil, false
}

func (s *Builder) Build(ctx context.Context) error {
	if err := s.loadAndInitConfig(ctx); err != nil {
		return err
	}

	if err := s.loadSQL(ctx); err != nil {
		return err
	}

	if strings.TrimSpace(s.routeBuilder.sqlStmt) == "" {
		return nil
	}

	if err := s.readArtificialParamHints(); err != nil {
		return err
	}

	if err := s.readRouteSettings(); err != nil {
		return err
	}

	if err := s.initConfigProvider(); err != nil {
		return err
	}

	if err := s.initRoute(); err != nil {
		return err
	}

	if err := s.initRouterResource(); err != nil {
		return err
	}

	if err := s.buildViews(ctx); err != nil {
		return err
	}

	if err := s.moveConstParameters(); err != nil {
		return err
	}

	if err := s.uploadFiles(); err != nil {
		return err
	}

	return nil
}

func (s *Builder) buildViews(ctx context.Context) error {
	params, err := s.buildViewParams()
	if err != nil {
		return err
	}

	config := s.routeBuilder.configProvider.ViewConfig()

	aView, err := s.buildMainView(ctx, config)
	if err != nil {
		return err
	}

outer:
	for _, paramName := range params {
		for _, viewParameter := range aView.Template.Parameters {
			if view.FirstNotEmpty(viewParameter.Ref, viewParameter.Name) == paramName {
				continue outer
			}
		}

		aView.Template.Parameters = append(aView.Template.Parameters, &view.Parameter{Reference: shared.Reference{Ref: paramName}})
	}

	s.setMainView(aView)
	if err = s.indexExcludedColumns(config); err != nil {
		return err
	}

	s.inheritRouteServiceType(aView)
	return nil
}

func (s *Builder) loadAndInitConfig(ctx context.Context) error {
	aConfig, err := s.loadConfig(ctx)
	if err != nil {
		return err
	}

	err = s.initConfig(ctx, aConfig)
	if err != nil {
		return err
	}

	s.config = aConfig
	return nil
}

func (s *Builder) readRouteSettings() error {
	if s.routeBuilder.option.Declare != nil {
		s.routeBuilder.paramsIndex.AddParamTypes(s.routeBuilder.option.Declare)
	}

	if s.routeBuilder.option.Const != nil {
		s.routeBuilder.paramsIndex.AddConsts(s.routeBuilder.option.Const)
	}

	if err := s.loadGoTypes(); err != nil {
		return err
	}

	return nil
}

func extractURIParams(URI string) map[string]bool {
	result := map[string]bool{}

	if URI == "" {
		return result
	}

	uriParams, _ := toolbox.ExtractURIParameters(URI, strings.NewReplacer("{", "", "}", "").Replace(URI))
	for _, param := range uriParams {
		result[param] = true
	}

	return result
}

func (s *Builder) initRoute() error {
	method := s.routeBuilder.configProvider.DefaultHTTPMethod()
	if s.routeBuilder.option.Method != "" {
		method = s.routeBuilder.option.Method
	}

	s.routeBuilder.route = &router.Route{
		Method:      method,
		EnableAudit: true,
		Cors: &router.Cors{
			AllowCredentials: boolPtr(true),
			AllowHeaders:     stringsPtr("*"),
			AllowMethods:     stringsPtr("*"),
			AllowOrigins:     stringsPtr("*"),
			ExposeHeaders:    stringsPtr("*"),
		},
		URI:   s.config.APIPrefix + s.options.RouterURI(s.routeBuilder.option.URI),
		Index: router.Index{Namespace: map[string]string{}},
		Output: router.Output{
			CaseFormat: "lc",
		},
	}

	s.routeBuilder.paramsIndex.AddUriParams(extractURIParams(s.routeBuilder.route.URI))
	return s.buildRouterOutput()
}

func (s *Builder) buildRouterOutput() error {
	if s.routeBuilder.option.DateFormat != "" {
		s.routeBuilder.route.Output.DateFormat = s.routeBuilder.option.DateFormat
	}

	s.routeBuilder.route.Output.CSV = s.routeBuilder.option.CSV
	config, err := s.routeBuilder.configProvider.OutputConfig()
	if err != nil {
		return err
	}

	if err = tryUnmarshalHint(config, &s.routeBuilder.route.Output); err != nil {
		return err
	}

	if s.routeBuilder.route.Output.Cardinality == "" {
		s.routeBuilder.route.Output.Cardinality = view.Many
	}

	s.routeBuilder.route.Output.CaseFormat = view.CaseFormat(view.FirstNotEmpty(s.routeBuilder.option.CaseFormat, "lc"))
	if s.routeBuilder.option.ResponseField != "" {
		s.routeBuilder.route.Style = router.ComprehensiveStyle
		s.routeBuilder.route.ResponseField = s.routeBuilder.option.ResponseField
	}

	if err = s.initRouteRequestBodySchemaIfNeeded(); err != nil {
		return err
	}

	if rBody := s.routeBuilder.option.ResponseBody; rBody != nil {
		s.routeBuilder.route.ResponseBody = &router.BodySelector{
			StateValue: rBody.From,
		}
	}

	return nil
}

func (s *Builder) initRouteRequestBodySchemaIfNeeded() error {
	body := s.routeBuilder.option.RequestBody
	if body == nil {
		return nil
	}

	bodyType := body.DataType
	if bodyType == "" {
		return nil
	}

	s.routeBuilder.route.RequestBodySchema = &view.Schema{DataType: bodyType}
	return nil
}

func (s *Builder) unmarshalRouterOutput(startExpr *Column, output *router.Output) error {
	if startExpr == nil || startExpr.Comments == "" {
		return nil
	}

	_, err := sanitize.UnmarshalHint(startExpr.Comments, output)
	return err
}

func (s *Builder) initConfigProvider() error {
	if s.routeBuilder.sqlStmt == "" {
		return nil
	}

	SQL := s.routeBuilder.sqlStmt
	configProvider, err := s.buildConfigProvider(SQL)
	if err != nil {
		return err
	}

	s.routeBuilder.configProvider = configProvider
	return nil
}

func (s *Builder) buildConfigProvider(SQL string) (*ViewConfigurer, error) {
	serviceType := router.ReaderServiceType

	if IsSQLExecMode(SQL) {
		serviceType = router.ExecutorServiceType
	}

	return NewConfigProviderReader(s.options.Generate.Name, SQL, s.routeBuilder.option, serviceType, s.routeBuilder.paramsIndex, nil, &s.options.Connector)
}

func (s *Builder) loadSQL(ctx context.Context) error {
	if s.options.Location == "" {
		return nil
	}

	sourceURL := normalizeURL(s.options.Location)
	SQLbytes, err := s.fs.DownloadWithURL(context.Background(), sourceURL)
	if err != nil {
		return err
	}

	SQL, err := s.prepareRuleIfNeeded(SQLbytes)
	if err != nil {
		return err
	}

	hint, SQL := s.extractRouteSettings([]byte(SQL))
	hints := sanitize.ExtractParameterHints(SQL)
	SQL = sanitize.RemoveParameterHints(SQL, hints)

	tryUnmrashalHintWithWarn(hint, s.routeBuilder.option)

	for paramName, paramType := range s.routeBuilder.option.Declare {
		actualName, err := s.Type(paramType)
		if err != nil {
			return err
		}

		s.routeBuilder.option.Declare[paramName] = actualName
	}

	s.routeBuilder.sqlStmt = SQL
	s.routeBuilder.paramsIndex.AddHints(hints.Index())
	return nil
}

func (s *Builder) initRouterResource() error {
	var redirect *router.Redirect

	s.routeBuilder.routerResource.Redirect = redirect
	s.routeBuilder.routerResource.Routes = []*router.Route{s.routeBuilder.route}
	s.routeBuilder.routerResource.ColumnsDiscovery = true

	return nil
}

func (s *Builder) uploadFiles() error {
	if err := s.uploadConnectionsDep(); err != nil {
		return err
	}

	if err := s.uploadCacheDep(); err != nil {
		return err
	}

	if err := s.uploadVariablesDep(); err != nil {
		return err
	}

	if err := s.uploadPlugins(); err != nil {
		return err
	}

	return fsAddYAML(s.fs, s.options.RouterURL(), s.routeBuilder.routerResource)
}

func (s *Builder) uploadConnectionsDep() error {
	s.routeBuilder.routerResource.With = append(s.routeBuilder.routerResource.With, "connections")
	dependency := &view.Resource{
		ModTime:    TimeNow(),
		Connectors: s.options.Connectors(),
	}

	s.routeBuilder.routerResource.Resource.Connectors = nil
	depURL := s.options.DepURL("connections")
	return fsAddYAML(s.fs, depURL, dependency)
}

func (s *Builder) uploadCacheDep() error {
	cache := s.routeBuilder.option.Cache
	if cache == nil {
		return nil
	}

	s.routeBuilder.routerResource.With = append(s.routeBuilder.routerResource.With, "cache")
	cacheDependency := &view.Resource{ModTime: TimeNow()}
	cacheURL := s.options.DepURL("cache")
	cacheDependency.CacheProviders = append(cacheDependency.CacheProviders, cache)
	return fsAddYAML(s.fs, cacheURL, cacheDependency)
}

func (s *Builder) uploadVariablesDep() error {
	if len(s.constParameters) == 0 {
		return nil
	}

	fileName := "variables"
	if s.routeBuilder.option.ConstFileURL != "" {
		fileName = s.routeBuilder.option.ConstFileURL
	}

	s.routeBuilder.routerResource.With = append(s.routeBuilder.routerResource.With, fileName)
	variablesDep := &view.Resource{ModTime: TimeNow(), Parameters: s.constParameters}
	variablesURL := s.options.DepURL(fileName)
	return fsAddYAML(s.fs, variablesURL, variablesDep)
}

func fsAddJSON(fs afs.Service, URL string, any interface{}) error {
	data, err := json.MarshalIndent(any, "", "\t")
	if err != nil {
		return err
	}
	return fs.Upload(context.Background(), URL, file.DefaultFileOsMode, bytes.NewReader(data))
}

func fsAddYAML(fs afs.Service, URL string, any interface{}) error {
	aMap := map[string]interface{}{}
	data, _ := json.Marshal(any)
	json.Unmarshal(data, &aMap)
	compacted := map[string]interface{}{}
	toolbox.CopyNonEmptyMapEntries(aMap, compacted)
	data, err := yaml.Marshal(compacted)
	if err != nil {
		return err
	}
	return fs.Upload(context.Background(), URL, file.DefaultFileOsMode, bytes.NewReader(data))
}

func (s *Builder) buildMainView(ctx context.Context, config *viewConfig) (*view.View, error) {
	s.inheritRouteFromMainConfig(config.outputConfig)

	aView, err := s.buildAndAddViewWithLog(ctx, config, &view.Config{
		Limit: 25,
		Constraints: &view.Constraints{
			Filterable: []string{"*"},
			Criteria:   true,
			Limit:      true,
			Offset:     true,
			Projection: true,
		},
	}, true)

	return aView, err
}

func (s *Builder) setMainView(aView *view.View) {
	s.routeBuilder.route.View = &view.View{Reference: shared.Reference{Ref: aView.Name}}
}

func updateAsAuthParamIfNeeded(auth string, param *view.Parameter) {
	if auth == "" {
		return
	}

	param.ErrorStatusCode = 401
	param.Required = boolPtr(true)
}

func (s *Builder) paramByName(name string) *view.Parameter {
	param, ok := s.routeBuilder.paramsIndex.Param(name)
	if !ok {
		s.routeBuilder.routerResource.Resource.AddParameters(param)
	}

	return param
}

func (s *Builder) columnTypes(table *Table) ColumnIndex {
	meta := s.tablesMeta.TableMeta(view.FirstNotEmpty(table.HolderName, table.Name))
	columns := meta.IndexColumns(table.InnerAlias).Merge(meta.IndexColumns(""))

	for alias, tableName := range table.Deps {
		tableMeta := s.tablesMeta.TableMeta(string(tableName))
		columns.Merge(tableMeta.IndexColumns("")).Merge(tableMeta.IndexColumns(string(alias)))
	}

	return columns
}

func (s *Builder) buildCacheWarmup(warmup map[string]interface{}, on *relationKey) *view.Warmup {
	if warmup == nil || on == nil {
		return nil
	}

	warmup = copyWarmup(warmup)

	result := &view.Warmup{
		IndexColumn: view.FirstNotEmpty(on.child.Field, on.child.Column),
	}

	multiSet := &view.CacheParameters{}
	for k, v := range warmup {
		switch actual := v.(type) {
		case []interface{}:
			multiSet.Set = append(multiSet.Set, &view.ParamValue{Name: k, Values: actual})
		default:
			multiSet.Set = append(multiSet.Set, &view.ParamValue{Name: k, Values: []interface{}{actual}})
		}
	}

	result.Cases = append(result.Cases, multiSet)
	return result
}

func copyWarmup(warmup map[string]interface{}) map[string]interface{} {
	result := map[string]interface{}{}
	for aKey := range warmup {
		if aKey == "" {
			continue
		}

		result[aKey] = warmup[aKey]
	}
	return result

}

func (s *Builder) addParameters(params ...*view.Parameter) error {
	for i, aParam := range params {
		if _, ok := s.routeBuilder.paramsIndex.parameters[aParam.Name]; ok {
			continue
		}

		if err := s.updateParamByHint(aParam); err != nil {
			return err
		}

		s.routeBuilder.routerResource.Resource.Parameters = append(s.routeBuilder.routerResource.Resource.Parameters, params[i])
		s.routeBuilder.paramsIndex.AddParameter(params[i])
	}

	return nil
}

func (s *Builder) addTypeDef(schema *view.TypeDefinition) {
	s.routeBuilder.routerResource.Resource.Types = append(s.routeBuilder.routerResource.Resource.Types, schema)
}

func (s *Builder) inheritRouteFromMainConfig(config option.OutputConfig) {
	s.routeBuilder.route.ResponseField = view.FirstNotEmpty(config.ResponseField, s.routeBuilder.route.ResponseField)
	s.routeBuilder.route.Style = router.Style(view.FirstNotEmpty(config.Style, string(s.routeBuilder.route.Style)))
}

func (s *Builder) indexExcludedColumns(config *viewConfig) error {
	err := s.appendExcluded(&s.routeBuilder.route.Exclude, config, "")
	if err != nil {
		return err
	}

	if err := s.appendMetaExcluded(&s.routeBuilder.route.Exclude, config, ""); err != nil {
		return err
	}

	return err
}

func (s *Builder) appendExcluded(excluded *[]string, config *viewConfig, path string) error {
	if err := s.excludeTableColumns(excluded, config.expandedTable, path); err != nil {
		return err
	}

	for _, relation := range config.relations {
		holderName, err := s.normalizeFieldName(relation.unexpandedTable.HolderName)
		if err != nil {
			return err
		}

		if err := s.appendExcluded(excluded, relation, combineSegments(path, holderName)); err != nil {
			return err
		}

		if err := s.appendMetaExcluded(excluded, relation, path); err != nil {
			return err
		}
	}

	return nil
}

func (s *Builder) appendMetaExcluded(excluded *[]string, config *viewConfig, path string) error {
	if config.templateMeta != nil {
		for _, field := range config.templateMeta.except {
			actualFieldName, err := s.normalizeFieldName(field)
			if err != nil {
				return err
			}

			actualName, err := s.normalizeFieldName(config.templateMeta.name)
			if err != nil {
				return err
			}

			*excluded = append(*excluded, combineSegments(path, actualName, actualFieldName))
		}
	}
	return nil
}

func (s *Builder) excludeTableColumns(excluded *[]string, table *Table, path string) error {
	for _, column := range table.Columns {
		for _, except := range column.Except {
			actualFieldName, err := s.normalizeFieldName(except)
			if err != nil {
				return err
			}

			excludedFieldPath := combineSegments(path, actualFieldName)
			*excluded = append(*excluded, excludedFieldPath)
		}
	}
	return nil
}

func (s *Builder) normalizeFieldName(except string) (string, error) {
	colFormat, err := format.NewCase(view.DetectCase(except))
	if err != nil {
		return "", err
	}

	actualFieldName := colFormat.Format(except, format.CaseUpperCamel)
	return actualFieldName, nil
}

func combineSegments(segments ...string) string {
	result := ""
	for _, segment := range segments {
		if result == "" {
			result = segment
		} else {
			result = result + "." + segment
		}
	}

	return result
}

func (s *Builder) buildViewParams() ([]string, error) {
	paramViews := s.routeBuilder.configProvider.ViewParams()
	var utilParams []string

	for _, paramViewConfig := range paramViews {
		externalParams, err := s.prepareExternalParameters(paramViewConfig)
		if err != nil {
			return nil, err
		}

		childViewConfig := paramViewConfig.viewConfig

		aView, err := s.buildAndAddViewWithLog(context.TODO(), paramViewConfig.viewConfig, &view.Config{
			Constraints: &view.Constraints{
				Criteria:   false,
				Limit:      true,
				Offset:     true,
				OrderBy:    false,
				Projection: false,
			},
			Limit: 25,
		}, false, externalParams...)

		if err != nil {
			return nil, err
		}

		paramName := aView.Name
		typeDef, err := s.buildSchemaFromTable(paramName, childViewConfig.unexpandedTable, s.columnTypes(childViewConfig.unexpandedTable))
		if err != nil {
			return nil, err
		}

		s.addTypeDef(typeDef)

		aParam := childViewConfig.unexpandedTable.ViewConfig.DataViewParameter

		if aParam == nil {
			aParam = &view.Parameter{
				Name: paramName,
				In: &view.Location{
					Kind: view.DataViewKind,
					Name: paramName,
				},
				Required: boolPtr(true),
			}
		}
		aParam.Schema = s.NewSchema(typeDef.Name, "")

		aView.Schema = s.NewSchema(typeDef.Name, "")
		updateAsAuthParamIfNeeded(childViewConfig.unexpandedTable.Auth, aParam)
		if err = s.addParameters(aParam); err != nil {
			return nil, err
		}

		if s.isUtilParam(aParam) {
			utilParams = append(utilParams, aParam.Name)
		}
	}

	return utilParams, nil
}

func (s *Builder) prepareExternalParameters(paramViewConfig *viewParamConfig) ([]*view.Parameter, error) {
	var externalParams []*view.Parameter

	for _, parameter := range paramViewConfig.params {
		if parameter.Auth != "" {
			authParam := &view.Parameter{
				Name:            parameter.Auth,
				In:              &view.Location{Name: "Authorization", Kind: view.HeaderKind},
				ErrorStatusCode: 401,
				Required:        boolPtr(true),
				Output:          &view.Codec{Name: "JwtClaim", Schema: &view.Schema{DataType: "JwtClaims"}},
				Schema:          &view.Schema{DataType: "string"},
			}

			if err := s.addParameters(authParam); err != nil {
				return nil, err
			}

			externalParams = append(externalParams, authParam)
		}
		if parameter.Connector != "" {
			paramViewConfig.viewConfig.unexpandedTable.Connector = parameter.Connector
		}
	}

	return externalParams, nil
}

func (s *Builder) moveConstParameters() error {
	newParams := make([]*view.Parameter, 0)
	constParams := make([]*view.Parameter, 0)
	for i := range s.routeBuilder.routerResource.Resource.Parameters {
		parameter := s.routeBuilder.routerResource.Resource.Parameters[i]

		if parameter.In.Kind == view.LiteralKind {
			constParams = append(constParams, parameter)
			continue
		}

		newParams = append(newParams, parameter)
	}

	s.routeBuilder.routerResource.Resource.Parameters = newParams
	s.constParameters = constParams

	return nil
}

func (s *Builder) updateParamByHint(param *view.Parameter) error {
	hint, ok := s.routeBuilder.paramsIndex.hints[param.Name]
	if !ok {
		return nil
	}

	JSONHint, _ := sanitize.SplitHint(hint.Hint)
	JSONHint = strings.TrimSpace(JSONHint)
	if JSONHint == "" {
		return nil
	}

	paramConfig := &option.ParameterConfig{}
	if err := tryUnmarshalHint(JSONHint, paramConfig); err != nil {
		return err
	}

	return s.updateViewParam(param, paramConfig)
}

func (s *Builder) updateViewParam(param *view.Parameter, config *option.ParameterConfig) error {
	if config.Const != nil {
		param.Const = config.Const
	}

	param.Name = view.FirstNotEmpty(config.Name, param.Name)
	if config.Target != nil {
		param.In.Name = *config.Target
	}

	if config.Required != nil {
		param.Required = config.Required
	}

	param.In.Kind = view.Kind(view.FirstNotEmpty(config.Kind, string(param.In.Kind)))
	paramType, err := s.Type(view.FirstNotEmpty(config.DataType, param.Schema.DataType))
	if err != nil {
		return err
	}

	param.Schema.DataType = paramType
	if config.Codec != "" {
		param.Output = &view.Codec{Reference: shared.Reference{Ref: config.Codec}}
	}

	if config.ExpectReturned != nil {
		param.MaxAllowedRecords = config.ExpectReturned
	}

	if config.CodecType != "" && param.Output != nil {
		param.Output.Schema = &view.Schema{DataType: config.CodecType}
	}

	return nil
}

func (s *Builder) isUtilParam(param *view.Parameter) bool {
	return s.routeBuilder.paramsIndex.utilsIndex[param.Name]
}

func (s *Builder) inheritRouteServiceType(aView *view.View) {
	switch aView.Mode {
	case "", view.SQLQueryMode:
		s.routeBuilder.route.Service = router.ReaderServiceType
	case view.SQLExecMode:
		s.routeBuilder.route.Service = router.ExecutorServiceType
	}
}

func (s *Builder) prepareRuleIfNeeded(SQL []byte) (string, error) {
	if s.options.PrepareRule == "" {
		return string(SQL), nil
	}

	switch strings.ToLower(s.options.PrepareRule) {
	case PreparePost:
		return s.preparePostRule(context.Background(), SQL)
	case PreparePatch:
		return s.preparePatchRule(context.Background(), SQL)
	case PreparePut:
		return s.preparePutRule(context.Background(), SQL)
	default:
		return "", fmt.Errorf("unsupported prepare rule type")
	}
}

func (s *Builder) loadGoType(typeSrc *option.TypeSrcConfig) error {
	if typeSrc == nil {
		return nil
	}
	s.normalizeURL(typeSrc)

	dirTypes, err := xreflect.ParseTypes(typeSrc.URL)
	if err != nil {
		return err
	}

	aPluginMeta := &pluginGenDeta{
		URL:       typeSrc.URL,
		filesMeta: dirTypes,
	}

	for _, typeName := range typeSrc.Types {
		actualName, asPtr := typeName, false
		if strings.HasPrefix(typeName, "*") {
			actualName = actualName[1:]
			asPtr = true
		}

		rType, err := dirTypes.Type(actualName)
		if err != nil {
			return err
		}

		if err = s.types.reserve(actualName); err != nil {
			return err
		}

		packageName, err := s.packageName(dirTypes)
		if err != nil {
			return err
		}

		var dataType string
		var ref string
		if !s.shouldGenPlugin(actualName, dirTypes) {
			dataType = rType.String()
		} else {
			dataType = actualName
			ref = actualName
			aPluginMeta.Types = append(aPluginMeta.Types, actualName)
			occurrences := dirTypes.TypesOccurrences(actualName)
			if aPluginMeta.fileURL != "" {
				aPluginMeta.fileURL = aPluginMeta.URL
			} else {
				if len(occurrences) == 1 {
					aPluginMeta.fileURL = occurrences[0]
				}
			}
		}

		if aPluginMeta.fileURL == "" {
			aPluginMeta.fileURL = aPluginMeta.URL
		}

		if err != nil {
			return err
		}

		s.addTypeDef(&view.TypeDefinition{
			Reference: shared.Reference{
				Ref: ref,
			},
			Name:     actualName,
			DataType: dataType,
			Ptr:      asPtr,
			Package:  packageName,
		})
	}

	if len(aPluginMeta.Types) > 0 {
		s.plugins = append(s.plugins, aPluginMeta)
	}

	return nil
}

func (s *Builder) packageName(dirTypes *xreflect.DirTypes) (string, error) {
	packageValue, err := dirTypes.Value(plugins.PackageName)
	if err != nil {
		return "", nil
	}

	lit, ok := packageValue.(*ast.BasicLit)
	if !ok {
		return "", nil
	}

	return strconv.Unquote(lit.Value)
}

func (s *Builder) Type(typeName string) (string, error) {
	index := strings.LastIndex(typeName, ".")
	if index == -1 {
		return typeName, nil
	}

	actualName, asPtr := typeName, false
	if strings.HasPrefix(typeName, "*") {
		actualName = actualName[1:]
		asPtr = true
	}

	sourcePath, actualName := actualName[:index-1], actualName[index:]
	if asPtr {
		actualName = "*" + actualName
	}

	return typeName, s.loadGoType(&option.TypeSrcConfig{
		URL:   sourcePath,
		Types: []string{actualName},
	})
}

func (s *Builder) normalizeURL(typeSrc *option.TypeSrcConfig) {
	goPATH := os.Getenv("GOPATH")
	if goPATH == "" {
		goPATH = path.Join(os.Getenv("HOME"), "go")
	}
	typeSrc.URL = strings.ReplaceAll(typeSrc.URL, "${GOPATH}", goPATH)
	if url.Scheme(typeSrc.URL, "") == "" && !strings.HasPrefix(typeSrc.URL, "/") {
		if s.options.RelativePath != "" {
			typeSrc.URL = path.Join(s.options.RelativePath, typeSrc.URL)
		} else {
			if dir, err := os.Getwd(); err == nil {
				typeSrc.URL = filepath.Join(dir, typeSrc.URL)
			}
		}
	}
}

func (s *Builder) loadGoTypes() error {
	if err := s.loadGoType(s.routeBuilder.option.TypeSrc); err != nil {
		return err
	}

	cursor := parsly.NewCursor("", []byte(s.routeBuilder.sqlStmt), 0)
	defer func() {
		s.routeBuilder.sqlStmt = s.routeBuilder.sqlStmt[cursor.Pos:]
	}()

	matched := cursor.MatchAfterOptional(whitespaceMatcher, importKeywordMatcher)
	if matched.Code != importKeywordToken {
		return nil
	}

	matched = cursor.MatchAfterOptional(whitespaceMatcher, exprGroupMatcher, quotedMatcher)
	switch matched.Code {
	case quotedToken:
		text := matched.Text(cursor)
		typeSrc, err := s.parseTypeSrc(text[1 : len(text)-1])
		if err != nil {
			return err
		}

		return s.loadGoType(typeSrc)
	case exprGroupToken:
		exprContent := matched.Text(cursor)
		exprGroupCursor := parsly.NewCursor("", []byte(exprContent[1:len(exprContent)-1]), 0)

		for {

			matched = exprGroupCursor.MatchAfterOptional(whitespaceMatcher, quotedMatcher)
			switch matched.Code {
			case quotedToken:
				text := matched.Text(exprGroupCursor)
				typeSrc, err := s.parseTypeSrc(text[1 : len(text)-1])
				if err != nil {
					return err
				}

				if err = s.loadGoType(typeSrc); err != nil {
					return err
				}
			case parsly.EOF:
				return nil
			default:
				return cursor.NewError(quotedMatcher)
			}
		}
	}

	return nil
}

func (s *Builder) parseTypeSrc(imported string) (*option.TypeSrcConfig, error) {
	importSegments := strings.Split(imported, ".")
	if len(importSegments) != 2 {
		return nil, fmt.Errorf(`unsupported import format: %v, supported: "[path].[type]"`, imported)
	}

	return &option.TypeSrcConfig{
		URL:   importSegments[0],
		Types: []string{importSegments[1]},
	}, nil
}

func (s *Builder) readArtificialParamHints() error {
	cursor := parsly.NewCursor("", []byte(s.routeBuilder.sqlStmt), 0)
	for {
		matched := cursor.MatchOne(setTerminatedMatcher)
		switch matched.Code {
		case setTerminatedToken:
			setStart := cursor.Pos
			cursor.MatchOne(setMatcher) //to move cursor
			matched = cursor.MatchAfterOptional(whitespaceMatcher, exprGroupMatcher)
			if matched.Code != exprGroupToken {
				continue
			}

			selEnd := cursor.Pos

			content := matched.Text(cursor)
			content = content[1 : len(content)-1]
			contentCursor := parsly.NewCursor("", []byte(content), 0)

			matched = contentCursor.MatchAfterOptional(whitespaceMatcher, artificialMatcher)
			if matched.Code != artificialToken {
				continue
			}

			matched = contentCursor.MatchOne(whitespaceMatcher)
			selector, err := parser.MatchSelector(contentCursor)
			if err != nil {
				continue
			}

			s.buildParamHint(selector, contentCursor)
			s.routeBuilder.sqlStmt = strings.Replace(s.routeBuilder.sqlStmt, string(cursor.Input[setStart:selEnd]), "", 1)

		default:
			return nil
		}
	}
}

func (s *Builder) buildParamHint(selector *expr.Select, cursor *parsly.Cursor) {
	_, holderName := sanitize.GetHolderName(view.FirstNotEmpty(selector.FullName, selector.ID))
	paramHint, _ := s.parseParamHint(cursor)
	if paramHint == "" {
		return
	}

	s.routeBuilder.paramsIndex.hints[holderName] = &sanitize.ParameterHint{
		Parameter: holderName,
		Hint:      paramHint,
	}
}

func (s *Builder) parseParamHint(cursor *parsly.Cursor) (string, error) {
	matched := cursor.MatchAfterOptional(whitespaceMatcher, commentMatcher)
	if matched.Code == commentToken {
		return matched.Text(cursor), nil
	}

	config := &option.ParameterConfig{}
	possibilities := []*parsly.Token{typeMatcher, exprGroupMatcher}
	anyMatched := false
	for len(possibilities) > 0 {
		matched = cursor.MatchAfterOptional(whitespaceMatcher, possibilities...)
		switch matched.Code {
		case typeToken:
			typeContent := matched.Text(cursor)
			typeContent = strings.TrimSpace(typeContent[1 : len(typeContent)-1])

			types := strings.Split(typeContent, ",")
			dataType := types[0]
			if strings.HasPrefix(dataType, "[]") {
				config.Cardinality = view.Many
				dataType = dataType[2:]
			} else {
				config.Cardinality = view.One
			}

			config.DataType = dataType
			if len(types) > 1 {
				config.CodecType = types[1]
			}

			possibilities = []*parsly.Token{exprGroupMatcher}

		case exprGroupToken:
			inContent := matched.Text(cursor)
			inContent = strings.TrimSpace(inContent[1 : len(inContent)-1])

			segments := strings.Split(inContent, "/")
			config.Kind = segments[0]

			target := ""
			if len(segments) > 1 {
				target = strings.Join(segments[1:], ".")
			}

			config.Target = &target

			if err := s.readParamConfigs(config, cursor); err != nil {
				return "", err
			}
			possibilities = []*parsly.Token{}
		default:
			if !anyMatched {
				return "", nil
			}
			possibilities = []*parsly.Token{}
		}
		anyMatched = true
	}
	marshal, err := json.Marshal(config)
	if err != nil {
		return "", err
	}

	return string(marshal), nil
}

func (s *Builder) readParamConfigs(config *option.ParameterConfig, cursor *parsly.Cursor) error {
	matched := cursor.MatchOne(dotMatcher)
	if matched.Code != dotToken {
		return nil
	}

	for cursor.Pos < cursor.InputSize {
		matched = cursor.MatchOne(selectMatcher)
		if matched.Code != selectToken {
			return cursor.NewError(selectMatcher)
		}

		text := matched.Text(cursor)
		matched = cursor.MatchOne(exprGroupMatcher)
		if matched.Code != exprGroupToken {
			return cursor.NewError(exprGroupMatcher)
		}

		content := matched.Text(cursor)
		content = content[1 : len(content)-1]

		switch text {
		case "WithCodec":
			config.Codec = strings.Trim(content, "'")
		}

		cursor.MatchOne(whitespaceMatcher)
	}

	return nil
}
