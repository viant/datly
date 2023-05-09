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
	"github.com/viant/datly/config"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/router"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/template/sanitize"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/view"
	"github.com/viant/parsly"
	"github.com/viant/sqlparser/query"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/format"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/parser"
	"github.com/viant/xreflect"
	"go/ast"
	goFormat "go/format"
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
		tablesMeta *TableMetaRegistry
		options    *Options
		config     *standalone.Config
		logger     io.Writer
		fs         afs.Service
		fileNames  *uniqueIndex
		viewNames  *uniqueIndex
		types      *uniqueIndex
		plugins    []*pluginGenDeta
		bundles    map[string]*bundleMetadata
		logs       []string
	}

	routeBuilder struct {
		configProvider *ViewConfigurer
		paramsIndex    *ParametersIndex
		transforms     []*marshal.Transform
		routerResource *router.Resource
		route          *router.Route
		option         *option.RouteConfig
		sqlStmt        string
		views          map[string]*view.View

		session *session
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

	constFileContent struct {
		URL    string
		params []*view.Parameter
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

	routerResource := &router.Resource{
		Resource: view.NewResource(config.Config.FlattenTypes()),
	}

	paramIndex := NewParametersIndex(nil, nil)
	var viewCaches []*view.Cache
	consts := &constFileContent{}

	fileName, routerRoutes, err := s.readRouterOptionIfNeeded(routerResource)
	if err != nil || (len(routerRoutes) == 1 && routerRoutes[0].sourceURL == "") {
		return err
	}

	var routes []*routeBuilder
	for _, aFile := range routerRoutes {
		builder := s.newRouteBuilder(routerResource, paramIndex, aFile)
		routes = append(routes, builder)
		if err = s.buildRoute(ctx,
			builder,
			consts,
			&viewCaches,
		); err != nil {
			return err
		}
	}

	if err = s.updateParamsByHints(routerResource.Resource, paramIndex); err != nil {
		return err
	}

	if len(routes) == 1 {
		ruleName := routes[0].session.ruleName
		if ruleName != "" {
			fileName = ruleName
		}
	}

	if err = s.uploadFiles(fileName, consts, routerResource, viewCaches); err != nil {
		return err
	}

	return nil
}

func (s *Builder) newRouteBuilder(routerResource *router.Resource, paramIndex *ParametersIndex, aFile *session) *routeBuilder {
	return &routeBuilder{
		session:        aFile,
		views:          map[string]*view.View{},
		routerResource: routerResource,
		paramsIndex:    paramIndex,
		option: &option.RouteConfig{
			Declare: map[string]string{},
			Const:   map[string]interface{}{},
		},
	}
}

func (s *Builder) buildRoute(ctx context.Context, builder *routeBuilder, consts *constFileContent, viewCaches *[]*view.Cache) error {
	if err := s.loadSQL(ctx, builder, builder.session.sourceURL); err != nil {
		return err
	}

	if strings.TrimSpace(builder.sqlStmt) == "" {
		return nil
	}

	if err := s.readRouteSettings(builder); err != nil {
		return err
	}

	if consts.URL == "" {
		consts.URL = builder.option.ConstFileURL
	} else if consts.URL != builder.option.ConstFileURL && builder.option.ConstFileURL != "" {
		return fmt.Errorf("missmatch const destination, %v - %v", consts.URL, builder.option.ConstFileURL)
	}

	if err := s.initConfigProvider(builder); err != nil {
		return err
	}

	if err := s.initRoute(builder); err != nil {
		return err
	}

	if err := s.initRouterResource(builder); err != nil {
		return err
	}

	if err := s.buildViews(ctx, builder); err != nil {
		return err
	}

	if err := s.moveConstParameters(builder, &consts.params); err != nil {
		return err
	}

	if builder.option.Cache != nil {
		*viewCaches = append(*viewCaches, builder.option.Cache)
	}

	return nil
}

func (s *Builder) buildViews(ctx context.Context, builder *routeBuilder) error {
	utilParams, err := s.buildViewParams(builder)
	if err != nil {
		return err
	}

	utilParamsIndex := map[string]bool{}
	for _, param := range utilParams {
		utilParamsIndex[param] = true
	}

	for paramName := range builder.paramsIndex.hints {
		if utilParamsIndex[paramName] {
			continue
		}

		utilParams = append(utilParams, paramName)
	}

	aConfig := builder.configProvider.ViewConfig()
	aView, err := s.buildMainView(ctx, builder, aConfig)
	if err != nil {
		return err
	}

outer:
	for _, paramName := range utilParams {
		for _, viewParameter := range aView.Template.Parameters {
			if view.FirstNotEmpty(viewParameter.Ref, viewParameter.Name) == paramName {
				continue outer
			}
		}

		if _, ok := builder.paramsIndex.hints[paramName]; !ok {
			if err = s.addParameters(builder, &view.Parameter{Name: paramName}); err != nil {
				return err
			}
		}

		aView.Template.Parameters = append(aView.Template.Parameters, &view.Parameter{Reference: shared.Reference{Ref: paramName}})
	}

	s.setMainView(builder, aView)
	if err = s.indexExcludedColumns(builder, aConfig); err != nil {
		return err
	}

	s.inheritRouteServiceType(builder, aView)
	result, _ := json.MarshalIndent(aView, "", "  ")
	s.logs = append(s.logs, fmt.Sprintf("---------- connections: -----------\n\t %s \n", string(result)))
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

func (s *Builder) readRouteSettings(builder *routeBuilder) error {
	if builder.option.Declare != nil {
		builder.paramsIndex.AddParamTypes(builder.option.Declare)
	}

	if s.options.ConstURL != "" {
		builder.option.ConstURL = s.options.ConstURL
	}

	if constURL := builder.option.ConstURL; constURL != "" {
		sourceURL := builder.session.JoinWithSourceURL(constURL)
		content, err := s.fs.DownloadWithURL(context.Background(), sourceURL)
		if err != nil {
			return err
		}

		if err = json.Unmarshal(bytes.TrimSpace(content), &builder.option.Const); err != nil {
			return err
		}
	}

	if builder.option.Const != nil {
		builder.paramsIndex.AddConsts(builder.option.Const)
	}

	if err := s.loadGoTypes(builder); err != nil {
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

func (s *Builder) initRoute(builder *routeBuilder) error {
	method := builder.configProvider.DefaultHTTPMethod()
	if builder.option.Method != "" {
		method = builder.option.Method
	}

	builder.route = &router.Route{
		Method:           method,
		EnableAudit:      true,
		Transforms:       builder.transforms,
		CustomValidation: builder.option.CustomValidation,
		Cors: &router.Cors{
			AllowCredentials: boolPtr(true),
			AllowHeaders:     stringsPtr("*"),
			AllowMethods:     stringsPtr("*"),
			AllowOrigins:     stringsPtr("*"),
			ExposeHeaders:    stringsPtr("*"),
		},
		URI:   combineURLs(s.config.APIPrefix, s.options.RoutePrefix, builder.session.routePrefix, builder.option.URI),
		Index: router.Index{Namespace: map[string]string{}},
		Output: router.Output{
			CaseFormat: "lc",
		},
	}

	builder.paramsIndex.AddUriParams(extractURIParams(builder.route.URI))
	return s.buildRouterOutput(builder)
}

func (s *Builder) buildRouterOutput(builder *routeBuilder) error {
	if builder.option.DateFormat != "" {
		builder.route.Output.DateFormat = builder.option.DateFormat
	}

	builder.route.Output.CSV = builder.option.CSV
	aConfig, err := builder.configProvider.OutputConfig()
	if err != nil {
		return err
	}

	if err = tryUnmarshalHint(aConfig, &builder.route.Output); err != nil {
		return err
	}

	if builder.route.Output.Cardinality == "" {
		builder.route.Output.Cardinality = view.Many
	}

	builder.route.Output.CaseFormat = formatter.CaseFormat(view.FirstNotEmpty(builder.option.CaseFormat, "lc"))
	if builder.option.Field != "" {
		builder.route.Style = router.ComprehensiveStyle
		builder.route.Field = builder.option.Field
	}

	if err = s.initRouteRequestBodySchemaIfNeeded(builder); err != nil {
		return err
	}

	if rBody := builder.option.ResponseBody; rBody != nil {
		builder.route.ResponseBody = &router.BodySelector{
			StateValue: rBody.From,
		}
	}

	return nil
}

func (s *Builder) initRouteRequestBodySchemaIfNeeded(builder *routeBuilder) error {
	body := builder.option.RequestBody
	if body == nil {
		return nil
	}

	bodyType := body.DataType
	if bodyType == "" {
		return nil
	}

	builder.route.RequestBodySchema = &view.Schema{DataType: bodyType}
	return nil
}

func (s *Builder) unmarshalRouterOutput(startExpr *Column, output *router.Output) error {
	if startExpr == nil || startExpr.Comments == "" {
		return nil
	}

	_, err := sanitize.UnmarshalHint(startExpr.Comments, output)
	return err
}

func (s *Builder) initConfigProvider(builder *routeBuilder) error {
	if builder.sqlStmt == "" {
		return nil
	}

	SQL := builder.sqlStmt
	configProvider, err := s.buildConfigProvider(SQL, builder)
	if err != nil {
		return err
	}

	builder.configProvider = configProvider
	return nil
}

func (s *Builder) buildConfigProvider(SQL string, builder *routeBuilder) (*ViewConfigurer, error) {
	serviceType := router.ReaderServiceType

	if IsSQLExecMode(SQL) {
		serviceType = router.ExecutorServiceType
	}

	return NewConfigProviderReader(view.FirstNotEmpty(s.options.Generate.Name, s.fileName(builder.session.sourceURL)), SQL, builder.option, serviceType, builder.paramsIndex, nil, &s.options.Connector, builder)
}

func (s *Builder) loadSQL(ctx context.Context, builder *routeBuilder, location string) error {
	if location == "" {
		return nil
	}

	sourceURL := normalizeURL(location)
	SQLbytes, err := s.fs.DownloadWithURL(context.Background(), sourceURL)
	if err != nil {
		return err
	}

	SQL, err := s.prepareRuleIfNeeded(SQLbytes)
	if err != nil {
		return err
	}

	hint, SQL := s.extractRouteSettings([]byte(SQL))

	if SQL, err = s.readArtificialParamHints(builder, SQL); err != nil {
		return err
	}

	hints := sanitize.ExtractParameterHints(SQL)
	SQL = sanitize.RemoveParameterHints(SQL, hints)

	tryUnmrashalHintWithWarn(hint, builder.option)

	for paramName, paramType := range builder.option.Declare {
		actualName, err := s.Type(builder.routerResource.Resource, paramType)
		if err != nil {
			return err
		}

		builder.option.Declare[paramName] = actualName
	}

	builder.sqlStmt = SQL
	builder.paramsIndex.AddHints(hints.Index())
	return nil
}

func (s *Builder) initRouterResource(builder *routeBuilder) error {
	var redirect *router.Redirect

	builder.routerResource.Redirect = redirect
	builder.routerResource.Routes = append(builder.routerResource.Routes, builder.route)
	builder.routerResource.ColumnsDiscovery = true

	return nil
}

func (s *Builder) uploadFiles(resourceName string, consts *constFileContent, resource *router.Resource, caches []*view.Cache) error {
	if err := s.uploadConnectionsDep(resource); err != nil {
		return err
	}

	if err := s.uploadCacheDep(resource, caches); err != nil {
		return err
	}

	if err := s.uploadVariablesDep(resource, consts); err != nil {
		return err
	}

	if err := s.uploadPlugins(); err != nil {
		return err
	}

	return fsAddYAML(s.fs, s.options.RouterURL(resourceName), resource)
}

func (s *Builder) uploadConnectionsDep(resource *router.Resource) error {
	resource.With = append(resource.With, "connections")
	dependency := &view.Resource{
		ModTime:    TimeNow(),
		Connectors: s.options.Connectors(),
	}

	resource.Resource.Connectors = nil
	depURL := s.options.DepURL("connections")
	if err := fsAddYAML(s.fs, depURL, dependency); err != nil {
		return err
	}

	s.logs = append(s.logs, reportContent("---------- connections: -----------\n\t"+depURL, depURL))
	return nil
}

func (s *Builder) uploadCacheDep(resource *router.Resource, caches []*view.Cache) error {
	if len(caches) == 0 {
		return nil
	}

	resource.With = append(resource.With, "cache")
	cacheDependency := &view.Resource{ModTime: TimeNow()}
	cacheURL := s.options.DepURL("cache")
	cacheDependency.CacheProviders = append(cacheDependency.CacheProviders, caches...)
	return fsAddYAML(s.fs, cacheURL, cacheDependency)
}

func (s *Builder) uploadVariablesDep(resource *router.Resource, consts *constFileContent) error {
	if len(consts.params) == 0 {
		return nil
	}

	fileName := "variables"
	if consts.URL != "" {
		fileName = consts.URL
	}

	resource.With = append(resource.With, fileName)
	variablesDep := &view.Resource{ModTime: TimeNow(), Parameters: consts.params}
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
	_ = json.Unmarshal(data, &aMap)
	compacted := map[string]interface{}{}
	_ = toolbox.CopyNonEmptyMapEntries(aMap, compacted)
	data, err := yaml.Marshal(compacted)
	if err != nil {
		return err
	}
	return fs.Upload(context.Background(), URL, file.DefaultFileOsMode, bytes.NewReader(data))
}

func (s *Builder) buildMainView(ctx context.Context, builder *routeBuilder, config *viewConfig) (*view.View, error) {
	s.inheritRouteFromMainConfig(builder, config.outputConfig)

	aView, err := s.buildAndAddViewWithLog(ctx, builder, config, &view.Config{
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

func (s *Builder) setMainView(builder *routeBuilder, aView *view.View) {
	builder.route.View = &view.View{Reference: shared.Reference{Ref: aView.Name}}
}

func updateAsAuthParamIfNeeded(auth string, param *view.Parameter) {
	if auth == "" {
		return
	}

	param.ErrorStatusCode = 401
	param.Required = boolPtr(true)
}

func (s *Builder) paramByName(builder *routeBuilder, name string) *view.Parameter {
	param, ok := builder.paramsIndex.Param(name)
	if !ok {
		builder.routerResource.Resource.AddParameters(param)
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

func (s *Builder) addParameters(builder *routeBuilder, params ...*view.Parameter) error {
	for i, aParam := range params {
		if _, ok := builder.paramsIndex.parameters[aParam.Name]; ok {
			continue
		}

		builder.routerResource.Resource.Parameters = append(builder.routerResource.Resource.Parameters, params[i])
		builder.paramsIndex.AddParameter(params[i])
	}

	return nil
}

func (s *Builder) addTypeDef(resource *view.Resource, schema *view.TypeDefinition) {
	resource.Types = append(resource.Types, schema)
}

func (s *Builder) inheritRouteFromMainConfig(builder *routeBuilder, config option.OutputConfig) {
	builder.route.Field = view.FirstNotEmpty(config.Field, builder.route.Field)
	builder.route.Style = router.Style(view.FirstNotEmpty(config.Style, string(builder.route.Style)))
}

func (s *Builder) indexExcludedColumns(builder *routeBuilder, config *viewConfig) error {
	err := s.appendExcluded(&builder.route.Exclude, config, "")
	if err != nil {
		return err
	}

	if err := s.appendMetaExcluded(&builder.route.Exclude, config, ""); err != nil {
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
	colFormat, err := format.NewCase(formatter.DetectCase(except))
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

func (s *Builder) buildViewParams(builder *routeBuilder) ([]string, error) {
	paramViews := builder.configProvider.ViewParams()
	var utilParams []string

	for _, paramViewConfig := range paramViews {
		externalParams, err := s.prepareExternalParameters(builder, paramViewConfig)
		if err != nil {
			return nil, err
		}

		childViewConfig := paramViewConfig.viewConfig

		aView, err := s.buildAndAddViewWithLog(context.TODO(), builder, paramViewConfig.viewConfig, &view.Config{
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

		for _, param := range paramViewConfig.params {
			for _, qualifier := range param.Qualifiers {
				aView.Qualifiers = append(aView.Qualifiers, &view.Qualifier{
					Value:  qualifier.Value,
					Column: qualifier.Column,
				})
			}
		}

		paramName := aView.Name
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

		updateAsAuthParamIfNeeded(childViewConfig.unexpandedTable.Auth, aParam)
		if err = s.addParameters(builder, aParam); err != nil {
			return nil, err
		}

		if s.isUtilParam(builder, aParam) {
			utilParams = append(utilParams, aParam.Name)
		}
	}

	return utilParams, nil
}

func (s *Builder) prepareExternalParameters(builder *routeBuilder, paramViewConfig *viewParamConfig) ([]*view.Parameter, error) {
	var externalParams []*view.Parameter

	for _, parameter := range paramViewConfig.params {
		if parameter.Auth != "" {
			authParam := &view.Parameter{
				Name:            parameter.Auth,
				In:              &view.Location{Name: "Authorization", Kind: view.HeaderKind},
				ErrorStatusCode: 401,
				Required:        boolPtr(true),
				Output:          &view.Codec{Name: "JwtClaim", Schema: &view.Schema{DataType: "*JwtClaims"}},
				Schema:          &view.Schema{DataType: "string"},
			}

			if err := s.addParameters(builder, authParam); err != nil {
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

func (s *Builder) moveConstParameters(builder *routeBuilder, dest *[]*view.Parameter) error {
	newParams := make([]*view.Parameter, 0)
	constParams := make([]*view.Parameter, 0)
	for i := range builder.routerResource.Resource.Parameters {
		parameter := builder.routerResource.Resource.Parameters[i]

		if parameter.In != nil && parameter.In.Kind == view.LiteralKind {
			constParams = append(constParams, parameter)
			continue
		}

		newParams = append(newParams, parameter)
	}

	builder.routerResource.Resource.Parameters = newParams
	*dest = append(*dest, constParams...)

	return nil
}

func (s *Builder) updateParamByHint(resource *view.Resource, paramIndex *ParametersIndex, param *view.Parameter) error {
	hint, ok := paramIndex.hints[param.Name]
	if !ok {
		return nil
	}

	JSONHint, SQL := sanitize.SplitHint(hint.Hint)
	JSONHint = strings.TrimSpace(JSONHint)
	if JSONHint == "" {
		return nil
	}

	paramConfig := &option.ParameterConfig{}
	if err := tryUnmarshalHint(JSONHint, paramConfig); err != nil {
		return err
	}

	return s.updateViewParam(resource, param, paramConfig, SQL)
}

func (s *Builder) updateViewParam(resource *view.Resource, param *view.Parameter, config *option.ParameterConfig, SQL string) error {
	if param.In == nil {
		param.In = &view.Location{}
	}

	if param.Schema == nil {
		param.Schema = &view.Schema{}
	}

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
	paramType, err := s.Type(resource, view.FirstNotEmpty(config.DataType, param.Schema.DataType))
	if err != nil {
		return err
	}

	param.Schema.DataType = paramType
	if config.Codec != "" {
		param.Output = &view.Codec{Reference: shared.Reference{Ref: config.Codec}}
	}

	if config.MaxAllowedRecords != nil {
		param.MaxAllowedRecords = config.MaxAllowedRecords
	}

	if config.ExpectReturned != nil {
		param.ExpectedReturned = config.ExpectReturned
	}

	if config.MinAllowedRecords != nil {
		param.MinAllowedRecords = config.MinAllowedRecords
	}

	if config.CodecType != "" && param.Output != nil {
		param.Output.Schema = &view.Schema{DataType: config.CodecType}
	}

	if config.StatusCode != nil {
		param.ErrorStatusCode = *config.StatusCode
	}

	if strings.TrimSpace(config.Codec) != "" && isSQLLikeCodec(config.Codec) {
		if param.Codec == nil {
			param.Codec = &view.Codec{Reference: shared.Reference{config.Codec}}
		}

		param.Codec.Query = SQL
	}

	return nil
}

func (s *Builder) isUtilParam(builder *routeBuilder, param *view.Parameter) bool {
	return builder.paramsIndex.utilsIndex[param.Name]
}

func (s *Builder) inheritRouteServiceType(builder *routeBuilder, aView *view.View) {
	switch aView.Mode {
	case "", view.SQLQueryMode:
		builder.route.Service = router.ReaderServiceType
	case view.SQLExecMode:
		builder.route.Service = router.ExecutorServiceType
	}
}

func (s *Builder) prepareRuleIfNeeded(SQL []byte) (string, error) {
	if s.options.PrepareRule == "" {
		return string(SQL), nil
	}

	goFileOutput := s.options.DSQLOutput
	if output := s.options.GoFileOutput; output != "" {
		if strings.HasPrefix(output, "/") {
			goFileOutput = output
		} else {
			goFileOutput = path.Join(goFileOutput, output)
		}
	}

	preparedRouteBuilder := s.newRouteBuilder(
		&router.Resource{Resource: view.EmptyResource()},
		NewParametersIndex(nil, nil),
		newSession(path.Dir(s.options.Location), s.options.Location, s.options.PluginDst, s.options.DSQLOutput, s.options.DSQLOutput, goFileOutput),
	)

	switch strings.ToLower(s.options.PrepareRule) {
	case PreparePost:
		return s.preparePostRule(context.Background(), preparedRouteBuilder, SQL)
	case PreparePatch:
		return s.preparePatchRule(context.Background(), preparedRouteBuilder, SQL)
	case PreparePut:
		return s.preparePutRule(context.Background(), preparedRouteBuilder, SQL)
	default:
		return "", fmt.Errorf("unsupported prepare rule type")
	}
}

func (s *Builder) loadGoType(resource *view.Resource, typeSrc *option.TypeSrcConfig) error {
	if typeSrc == nil {
		return nil
	}
	s.normalizeURL(typeSrc)

	dirTypes, err := xreflect.ParseTypes(typeSrc.URL, xreflect.TypeLookupFn(config.Config.LookupType))
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

		s.addTypeDef(resource, &view.TypeDefinition{
			Reference: shared.Reference{
				Ref: ref,
			},
			Alias:    typeSrc.Alias,
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
	packageValue, err := dirTypes.Value(config.PackageName)
	if err != nil {
		return "", nil
	}

	lit, ok := packageValue.(*ast.BasicLit)
	if !ok {
		return "", nil
	}

	return strconv.Unquote(lit.Value)
}

func (s *Builder) Type(resource *view.Resource, typeName string) (string, error) {
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

	return typeName, s.loadGoType(resource, &option.TypeSrcConfig{
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

func (s *Builder) loadGoTypes(builder *routeBuilder) error {
	if err := s.loadGoType(builder.routerResource.Resource, builder.option.TypeSrc); err != nil {
		return err
	}

	cursor := parsly.NewCursor("", []byte(builder.sqlStmt), 0)
	defer func() {
		builder.sqlStmt = strings.TrimSpace(builder.sqlStmt[cursor.Pos:])
	}()

	matched := cursor.MatchAfterOptional(whitespaceMatcher, importKeywordMatcher)
	if matched.Code != importKeywordToken {
		return nil
	}

	matched = cursor.MatchAfterOptional(whitespaceMatcher, exprGroupMatcher, quotedMatcher)
	switch matched.Code {
	case quotedToken:
		text := matched.Text(cursor)
		typeSrc, err := s.parseTypeSrc(text[1:len(text)-1], cursor)
		if err != nil {
			return err
		}

		return s.loadGoType(builder.routerResource.Resource, typeSrc)
	case exprGroupToken:
		exprContent := matched.Text(cursor)
		exprGroupCursor := parsly.NewCursor("", []byte(exprContent[1:len(exprContent)-1]), 0)

		for {

			matched = exprGroupCursor.MatchAfterOptional(whitespaceMatcher, quotedMatcher)
			switch matched.Code {
			case quotedToken:
				text := matched.Text(exprGroupCursor)
				typeSrc, err := s.parseTypeSrc(text[1:len(text)-1], exprGroupCursor)
				if err != nil {
					return err
				}
				if err = s.loadGoType(builder.routerResource.Resource, typeSrc); err != nil {
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

func (s *Builder) parseTypeSrc(imported string, cursor *parsly.Cursor) (*option.TypeSrcConfig, error) {
	var alias string
	matched := cursor.MatchAfterOptional(whitespaceMatcher, aliasKeywordMatcher)
	if matched.Code == aliasKeywordToken {
		matched = cursor.MatchAfterOptional(whitespaceMatcher, quotedMatcher)
		if matched.Code != quotedToken {
			return nil, cursor.NewError(quotedMatcher)
		}

		alias = strings.Trim(matched.Text(cursor), "\"")
	}

	index := strings.LastIndex(imported, ".")
	if index == -1 {
		return nil, fmt.Errorf(`unsupported import format: %v, supported: "[path].[type]"`, imported)
	}

	return &option.TypeSrcConfig{
		URL:   imported[:index],
		Types: []string{imported[index+1:]},
		Alias: alias,
	}, nil
}

func (s *Builder) readArtificialParamHints(builder *routeBuilder, SQL string) (string, error) {
	SQLBytes := []byte(SQL)
	cursor := parsly.NewCursor("", SQLBytes, 0)
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

			if err = s.buildParamHint(builder, selector, contentCursor); err != nil {
				return "", err
			}

			builder.sqlStmt = strings.Replace(builder.sqlStmt, string(cursor.Input[setStart:selEnd]), "", 1)

			for i := setStart; i < cursor.Pos; i++ {
				SQLBytes[i] = ' '
			}

		default:
			return string(SQLBytes), nil
		}
	}
}

func (s *Builder) buildParamHint(builder *routeBuilder, selector *expr.Select, cursor *parsly.Cursor) error {
	paramHint, err := s.parseParamHint(cursor)
	if paramHint == "" || err != nil {
		return err
	}

	holderName := strings.Trim(view.FirstNotEmpty(selector.FullName, selector.ID), "${}")
	if pathStartIndex := strings.Index(holderName, "."); pathStartIndex >= 0 {
		hint, sql := sanitize.SplitHint(paramHint)

		aTransform := &option.TransformOption{}
		if err = tryUnmarshalHint(hint, aTransform); err != nil {
			return err
		}

		_, paramName := sanitize.GetHolderName(holderName)
		builder.transforms = append(builder.transforms, &marshal.Transform{
			ParamName:   paramName,
			Kind:        aTransform.TransformKind,
			Path:        holderName[pathStartIndex+1:],
			Codec:       aTransform.Codec,
			Source:      strings.TrimSpace(sql),
			Transformer: aTransform.Transformer,
		})

		return nil
	}

	builder.paramsIndex.AddParamHint(holderName, &sanitize.ParameterHint{
		Parameter: holderName,
		Hint:      paramHint,
	})

	return nil
}

func (s *Builder) parseParamHint(cursor *parsly.Cursor) (string, error) {
	matched := cursor.MatchAfterOptional(whitespaceMatcher, commentMatcher)
	if matched.Code == commentToken {
		return matched.Text(cursor), nil
	}

	aConfig := &option.ParameterConfig{}
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
				aConfig.Cardinality = view.Many
				dataType = dataType[2:]
			} else {
				aConfig.Cardinality = view.One
			}

			aConfig.DataType = dataType
			if len(types) > 1 {
				aConfig.CodecType = types[1]
			}

			possibilities = []*parsly.Token{exprGroupMatcher}

		case exprGroupToken:
			inContent := matched.Text(cursor)
			inContent = strings.TrimSpace(inContent[1 : len(inContent)-1])

			segments := strings.Split(inContent, "/")
			aConfig.Kind = segments[0]

			target := ""
			if len(segments) > 1 {
				target = strings.Join(segments[1:], ".")
			}

			aConfig.Target = &target

			if err := s.readParamConfigs(aConfig, cursor); err != nil {
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
	marshal, err := json.Marshal(aConfig)
	if err != nil {
		return "", err
	}

	return string(marshal), nil
}

func (s *Builder) readParamConfigs(config *option.ParameterConfig, cursor *parsly.Cursor) error {
	for cursor.Pos < cursor.InputSize {
		matched := cursor.MatchOne(dotMatcher)
		if matched.Code != dotToken {
			return nil
		}

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
		case "WithStatusCode":
			statusCode, err := strconv.Atoi(content)
			if err != nil {
				return err
			}

			config.StatusCode = &statusCode
		case "UtilParam":
			config.Util = true
		}

		cursor.MatchOne(whitespaceMatcher)
	}

	return nil
}

func (s *Builder) updateParamsByHints(resource *view.Resource, paramIndex *ParametersIndex) error {
	for _, parameter := range paramIndex.parameters {
		if err := s.updateParamByHint(resource, paramIndex, parameter); err != nil {
			return err
		}
	}

	return nil
}

func (s *Builder) readRouterOptionIfNeeded(resource *router.Resource) (string, []*session, error) {
	basePath := url.Join(s.options.RouteURL, s.options.RoutePrefix)
	routerURL := s.options.CustomRouterURL
	if routerURL == "" {
		return view.FirstNotEmpty(s.options.Generate.Name, s.fileName(s.options.Location)), []*session{newSession(basePath, s.options.Location, s.options.PluginDst, "", "", s.options.GoFileOutput)}, nil
	}

	routerContent, err := afs.New().DownloadWithURL(context.Background(), routerURL)
	if err != nil {
		return "", nil, err
	}

	routerConfig := &option.RouterConfig{}
	hintContent := sanitize.ExtractHint(string(routerContent))

	hint, _ := sanitize.SplitHint(hintContent)
	if err = tryUnmarshalHint(hint, routerConfig); err != nil {
		return "", nil, err
	}

	if template := strings.TrimSpace(strings.Replace(string(routerContent), hintContent, "", 1)); template != "" {
		resource.Interceptor = &router.RouteInterceptor{
			Template: template,
		}
	}

	if routerConfig.URL != "" {
		resource.URL = combineURLs(s.options.ApiURIPrefix, s.options.RoutePrefix, routerConfig.URL)
	}

	routes := make([]*session, 0, len(routerConfig.Routes))
	routerFileName := view.FirstNotEmpty(s.options.Generate.Name, s.fileName(routerURL))
	basePath = url.Join(basePath, routerFileName)
	for _, route := range routerConfig.Routes {
		var actualSourceURL string
		templatesFolder := path.Dir(routerURL)
		if strings.HasPrefix(route.SourceURL, "/") {
			actualSourceURL = route.SourceURL
		} else {
			actualSourceURL = path.Join(templatesFolder, route.SourceURL)
		}

		aRouteFile := newSession(basePath, actualSourceURL, s.options.PluginDst, "", s.fileName(actualSourceURL), s.options.GoFileOutput)
		aRouteFile.pathDiff = path.Base(basePath)
		aRouteFile.routePrefix = routerConfig.URL
		routes = append(routes, aRouteFile)
	}

	return routerFileName, routes, nil
}

func (s *Builder) fileName(URL string) string {
	routerFileName := path.Base(URL)
	if ext := path.Ext(routerFileName); ext != "" {
		routerFileName = strings.Replace(routerFileName, ext, "", 1)
	}
	return routerFileName
}

func (s *Builder) flushLogs(logger io.Writer) {
	for _, log := range s.logs {
		_, _ = logger.Write([]byte(log))
	}
}

func (s *Builder) ensureSideefectsImports(bundle *bundleMetadata) error {
	packageName := bundle.moduleName
	if asBase := path.Base(packageName); len(asBase) <= 2 {
		packageName = asBase
	}

	source := fmt.Sprintf(`
			package %v
			
			import (
				_ "%v"
			)
`, packageName, bundle.moduleName+"/"+importsDirectory)

	asBytes, err := goFormat.Source([]byte(source))
	if err != nil {
		return err
	}

	return s.fs.Upload(context.Background(), path.Join(bundle.url, fileSideefectsImports+".go"), file.DefaultFileOsMode, bytes.NewReader(asBytes))

}

func (s *Builder) ensureChecksum(bundle *bundleMetadata) error {
	checksumPath := path.Join(bundle.url, checksumDirectory)
	if ok, _ := s.fs.Exists(context.Background(), checksumPath); ok {
		return nil
	}

	if err := s.fs.Create(context.Background(), checksumPath, file.DefaultDirOsMode, true); err != nil {
		return err
	}

	return s.updateLastGenPluginMeta(bundle, TimeNow())
}

func combineURLs(basePath string, segments ...string) string {
	basePath = strings.TrimRight(basePath, "/")
	var actualSegments []string
	for _, segment := range segments {
		if segment == "" {
			continue
		}

		actualSegments = append(actualSegments, strings.TrimLeft(segment, "/"))
	}

	if basePath == "" {
		switch len(actualSegments) {
		case 0:
			return ""
		case 1:
			return actualSegments[0]
		default:
			return url.Join(actualSegments[0], actualSegments[1:]...)
		}
	}

	return url.Join(basePath, actualSegments...)
}
