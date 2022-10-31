package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/router"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/template/sanitize"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/metadata/ast/query"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/format"
	"gopkg.in/yaml.v3"
	"io"
	"strings"
)

type (
	Builder struct {
		tablesMeta   *TableMetaRegistry
		routeBuilder *routeBuilder
		options      *Options
		config       *standalone.Config
		logger       io.Writer
		fs           afs.Service
	}

	routeBuilder struct {
		configProvider *ViewConfigurer
		paramsIndex    *ParametersIndex
		routerResource *router.Resource
		route          *router.Route
		option         *option.Route
		sqlStmt        string
		views          map[string]*view.View
	}

	viewConfig struct {
		viewName        string
		queryJoin       *query.Join
		unexpandedTable *option.Table
		outputConfig    option.Output

		relations      []*viewConfig
		relationsIndex map[string]int
		metasBuffer    map[string]*option.Table
		templateMeta   *option.Table
		aKey           *relationKey
		fileName       string
		viewType       view.Mode
		expandedTable  *option.Table
	}

	viewParamConfig struct {
		viewName string
		viewFile string

		viewConfig *viewConfig
		params     []*option.Parameter
	}
)

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
	if c.unexpandedTable.OuterAlias != "" {
		return
	}

	c.unexpandedTable.OuterAlias = alias
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

func (c *viewConfig) AddMetaTemplate(holder string, config *option.Table) {
	if c.unexpandedTable.OuterAlias == holder {
		c.templateMeta = config
		return
	}

	if index, ok := c.relationsIndex[holder]; ok {
		c.relations[index].templateMeta = config
		return
	}

	c.metasBuffer[holder] = config
}

func (c *viewConfig) AddRelation(viewConfig *viewConfig) {
	holderName := viewConfig.unexpandedTable.OuterAlias

	c.relationsIndex[holderName] = len(c.relations)
	c.relations = append(c.relations, viewConfig)

	if metaConfig, ok := c.metasBuffer[holderName]; ok {
		viewConfig.templateMeta = metaConfig
		delete(c.metasBuffer, holderName)
	}
}

func (c *viewConfig) ViewConfig(holder string) (*viewConfig, bool) {
	if holder == c.unexpandedTable.OuterAlias {
		return c, true
	}

	for _, relation := range c.relations {
		if relation.unexpandedTable.OuterAlias == holder {
			return relation, true
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

	if err := s.buildViewParams(); err != nil {
		return err
	}

	config := s.routeBuilder.configProvider.ViewConfig()

	if err := s.buildMainView(ctx, config); err != nil {
		return err
	}

	if err := s.indexExcludedColumns(config); err != nil {
		return err
	}

	if err := s.uploadFiles(); err != nil {
		return err
	}

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
		Service:     s.routeBuilder.configProvider.ServiceType(),
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

	return nil
}

func (s *Builder) unmarshalRouterOutput(startExpr *option.Column, output *router.Output) error {
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

	return NewConfigProviderReader(s.options.Generate.Name, SQL, s.routeBuilder.option, s.routeBuilder.paramsIndex.hints, serviceType, s.routeBuilder.paramsIndex.consts)
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

	SQL := string(SQLbytes)
	hint := sanitize.ExtractHint(SQL)
	if hint != "" {
		SQL = strings.Replace(SQL, hint, "", 1)
	}

	hints := sanitize.ExtractParameterHints(SQL)
	SQL = sanitize.RemoveParameterHints(SQL, hints)

	tryUnmrashalHintWithWarn(hint, s.routeBuilder.option)

	s.routeBuilder.sqlStmt = SQL
	s.routeBuilder.paramsIndex.AddHints(hints.Index())
	return nil
}

func (s *Builder) buildExecModeConfigProvider(SQL string) (ViewConfigurer, error) {
	panic("handle later!")
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
	variables := s.routeBuilder.option.Const
	if len(variables) == 0 {
		return nil
	}

	fileName := "variables"
	if s.routeBuilder.option.ConstFileURL != "" {
		fileName = s.routeBuilder.option.ConstFileURL
	}

	s.routeBuilder.routerResource.With = append(s.routeBuilder.routerResource.With, fileName)
	variablesDep := &view.Resource{ModTime: TimeNow(), Parameters: s.buildConstParameters(s.routeBuilder.option)}
	variablesURL := s.options.DepURL(fileName)
	return fsAddYAML(s.fs, variablesURL, variablesDep)
}

func (s *Builder) buildConstParameters(route *option.Route) []*view.Parameter {
	params := make([]*view.Parameter, 0, len(route.Const))
	for paramName := range route.Const {
		aParam := s.paramByName(paramName)
		constParam := view.Parameter{
			Name: paramName,
			In: &view.Location{
				Kind: view.LiteralKind,
				Name: paramName,
			},
			Const: route.Const[paramName],
		}

		*aParam = constParam
		params = append(params, aParam)
	}

	return params
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

func (s *Builder) buildMainView(ctx context.Context, config *viewConfig) error {
	s.inheritRouteFromMainConfig(config.outputConfig)

	aView, err := s.buildAndAddView(ctx, config, &view.Config{
		Limit: 25,
		Constraints: &view.Constraints{
			Filterable: []string{"*"},
			Criteria:   true,
			Limit:      true,
			Offset:     true,
			Projection: true,
		},
	}, true)
	if err != nil {
		return err
	}

	s.routeBuilder.route.View = &view.View{Reference: shared.Reference{Ref: aView.Name}}
	return nil
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

func (s *Builder) columnTypes(table *option.Table) Columns {
	meta := s.tablesMeta.TableMeta(view.NotEmptyOf(table.OuterAlias, table.Name))
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

	result := &view.Warmup{
		IndexColumn: view.NotEmptyOf(on.child.Alias, on.child.Field, on.child.Column),
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

func (s *Builder) addParameters(params ...*view.Parameter) {
	for i := range params {
		s.routeBuilder.routerResource.Resource.Parameters = append(s.routeBuilder.routerResource.Resource.Parameters, params[i])
		s.routeBuilder.paramsIndex.AddParameter(params[i])
	}
}

func (s *Builder) addTypeDef(schema *view.Definition) {
	s.routeBuilder.routerResource.Resource.Types = append(s.routeBuilder.routerResource.Resource.Types, schema)
}

func (s *Builder) inheritRouteFromMainConfig(config option.Output) {
	s.routeBuilder.route.ResponseField = view.NotEmptyOf(config.ResponseField, s.routeBuilder.route.ResponseField)
	s.routeBuilder.route.Style = router.Style(view.NotEmptyOf(config.Style, string(s.routeBuilder.route.Style)))
}

func (s *Builder) indexExcludedColumns(config *viewConfig) error {
	return s.appendExcluded(&s.routeBuilder.route.Exclude, config, "")
}

func (s *Builder) appendExcluded(excluded *[]string, config *viewConfig, path string) error {
	for _, column := range config.unexpandedTable.Columns {
		for _, except := range column.Except {
			colFormat, err := format.NewCase(view.DetectCase(except))
			if err != nil {
				return err
			}

			excludedFieldPath := colFormat.Format(except, format.CaseUpperCamel)
			if path != "" {
				excludedFieldPath = path + "." + excludedFieldPath
			}

			*excluded = append(*excluded, excludedFieldPath)
		}
	}

	for _, relation := range config.relations {
		childPath := relation.aKey.owner.Field
		if path != "" {
			childPath = path + "." + childPath
		}
		if err := s.appendExcluded(excluded, relation, childPath); err != nil {
			return err
		}
	}

	return nil
}

func (s *Builder) buildViewParams() error {
	paramViews := s.routeBuilder.configProvider.ViewParams()

	for _, paramViewConfig := range paramViews {
		externalParams := s.prepareExternalParameters(paramViewConfig)

		childViewConfig := paramViewConfig.viewConfig

		aView, err := s.buildAndAddView(context.TODO(), paramViewConfig.viewConfig, &view.Config{
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
			return err
		}

		paramName := aView.Name
		typeDef := s.buildSchemaFromTable(paramName, childViewConfig.unexpandedTable, s.columnTypes(childViewConfig.unexpandedTable))
		s.addTypeDef(typeDef)

		aParam := childViewConfig.unexpandedTable.TableMeta.DataViewParameter

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
		s.addParameters(aParam)
	}

	return nil
}

func (s *Builder) prepareExternalParameters(paramViewConfig *viewParamConfig) []*view.Parameter {
	var externalParams []*view.Parameter

	for _, parameter := range paramViewConfig.params {
		if parameter.Auth != "" {
			externalParams = append(externalParams, &view.Parameter{
				Name:            parameter.Auth,
				In:              &view.Location{Name: "Authorization", Kind: view.HeaderKind},
				ErrorStatusCode: 401,
				Required:        boolPtr(true),
				Codec:           &view.Codec{Name: "JwtClaim"},
				Schema:          &view.Schema{DataType: "JwtTokenInfo"},
			})

			continue
		}
	}

	return externalParams
}
