package cmd

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/command"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/config"
	"github.com/viant/datly/gateway/runtime/standalone"
	codegen "github.com/viant/datly/internal/codegen"
	"github.com/viant/datly/router"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/template/sanitize"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/view"
	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	qexpr "github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	sio "github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/toolbox"
	rdata "github.com/viant/toolbox/data"
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
		Options          *options.Options
		extensionTypes   map[string]bool
		constFileContent constFileContent
		constIndex       ParametersIndex
		tablesMeta       *TableMetaRegistry
		options          *Options
		config           *standalone.Config
		logger           io.Writer
		fs               afs.Service
		fileNames        *uniqueIndex
		viewNames        *uniqueIndex
		types            *uniqueIndex
		plugins          []*pluginGenDeta
		bundles          map[string]*bundleMetadata
		logs             []string
		caches           view.Caches
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

	ViewConfig struct {
		parent          *ViewConfig
		mainHolder      string
		viewName        string
		isAuxiliary     bool
		queryJoin       *query.Join
		unexpandedTable *Table
		expandedTable   *Table

		outputConfig option.OutputConfig

		relations      []*ViewConfig
		relationsIndex map[string]int
		metasBuffer    map[string]*Table
		templateMeta   *templateMetaConfig
		aKey           *relationKey
		fileName       string
		viewType       view.Mode
		batchEnabled   map[string]bool
		Spec           *codegen.Spec
	}

	templateMetaConfig struct {
		table  *Table
		output *option.OutputConfig
		name   string
		except []string
	}

	ViewParamConfig struct {
		viewName string
		viewFile string

		viewConfig *ViewConfig
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
		index  map[string]bool
	}

	paramJSONHintConfig struct {
		option.ParameterConfig
		option.TransformOption
	}
)

func (c *ViewConfig) ActualHolderName() string {
	name := c.expandedTable.HolderName
	detectCase, err := format.NewCase(formatter.DetectCase(name))
	if err != nil {
		return name
	}
	if detectCase != format.CaseUpperCamel {
		name = detectCase.Format(name, format.CaseUpperCamel)
	}
	return name
}

func (c *ViewConfig) excludedColumns() map[string]bool {
	exceptIndex := map[string]bool{}
	for _, column := range c.expandedTable.Columns {
		for _, except := range column.Except {
			exceptIndex[strings.ToLower(except)] = true
		}
	}
	return exceptIndex
}

func (c *ViewConfig) listedColumns() map[string]bool {
	includeIndex := map[string]bool{}
	for _, column := range c.expandedTable.Inner {
		if column.Name == "*" {
			return includeIndex
		}
	}
	for _, column := range c.expandedTable.Inner {
		if column.Alias != "" {
			includeIndex[strings.ToLower(column.Alias)] = true
		}
		includeIndex[strings.ToLower(column.Name)] = true
	}
	return includeIndex
}

func (c *constFileContent) MergeFrom(params ...*view.Parameter) {
	if len(params) == 0 {
		return
	}
	if len(c.index) == 0 {
		c.index = map[string]bool{}
	}
	for i, candidate := range params {
		if c.index[candidate.Name] || candidate.In.Kind != view.KindLiteral {
			continue
		}
		c.params = append(c.params, params[i])
	}
}

func (c *constFileContent) AddConst(name string, value interface{}) {
	if len(c.index) == 0 {
		c.index = map[string]bool{}
	}
	if c.index[name] {
		return
	}
	param := &view.Parameter{Name: name, In: &view.Location{Kind: view.KindLiteral}, Const: value}
	switch value.(type) {
	case string:
		param.Schema = &view.Schema{DataType: "string"}
	case int, int64, uint64:
		param.Schema = &view.Schema{DataType: "int"}
	case bool:
		param.Schema = &view.Schema{DataType: "bool"}
	}
	c.params = append(c.params, param)
}

func (c *constFileContent) dedupe() {
	var params []*view.Parameter
	c.index = map[string]bool{}
	if len(c.params) == 0 {
		return
	}
	for i, candidate := range c.params {
		if c.index[candidate.Name] {
			continue
		}
		params = append(params, c.params[i])
	}
	c.params = params
}

func (c *ViewConfig) refName() string {
	if c.queryJoin == nil {
		return ""
	}
	rel, ref := extractRelationAliases(c.queryJoin)
	if c.queryJoin.Alias == rel {
		return ref
	}
	return rel
}
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

func (c *ViewConfig) ensureTableName(tableName string) {
	if c.unexpandedTable.Name != "" {
		return
	}

	c.unexpandedTable.Name = tableName
}

func (c *ViewConfig) ensureOuterAlias(alias string) {
	if c.unexpandedTable.HolderName != "" {
		return
	}

	c.unexpandedTable.HolderName = alias
}

func (c *ViewConfig) ensureInnerAlias(name string) {
	if c.unexpandedTable.InnerAlias != "" {
		return
	}

	c.unexpandedTable.InnerAlias = name
}

func (c *ViewConfig) ensureFileName(name string) {
	if c.fileName != "" {
		return
	}

	c.fileName = name
}

func (c *ViewConfig) AddMetaTemplate(metaName string, holder string, config *Table) {
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

func (c *ViewConfig) AddRelation(viewConfig *ViewConfig) {
	holderName := viewConfig.unexpandedTable.HolderName

	c.relationsIndex[holderName] = len(c.relations)
	c.relations = append(c.relations, viewConfig)

	if metaConfig, ok := c.metasBuffer[holderName]; ok {
		viewConfig.templateMeta.table = metaConfig
		delete(c.metasBuffer, holderName)
	}
}

func (c *ViewConfig) ViewConfig(holder string) (*ViewConfig, bool) {
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

func (c *ViewConfig) metaConfigByName(holder string) (*templateMetaConfig, bool) {
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

func (c *ViewConfig) buildSpec(ctx context.Context, db *sql.DB, pkg string) (err error) {
	name := c.ActualHolderName()
	if c.Spec, err = codegen.NewSpec(ctx, db, c.TableName(), c.SQL()); err != nil {
		return err
	}
	if len(c.Spec.Columns) == 0 {
		return fmt.Errorf("not found table %v(%v) columns", c.TableName(), c.SQL())
	}
	excludedColumns := c.excludedColumns()
	listedColumns := c.listedColumns()
	cardinality := view.One
	if c.IsToMany() {
		cardinality = view.Many
	}
	if err = c.Spec.BuildType(pkg, name, cardinality, listedColumns, excludedColumns); err != nil {
		return err
	}
	for _, relation := range c.relations {
		if err = relation.buildSpec(ctx, db, pkg); err != nil {
			return err
		}
		relation.Spec.Parent = c.Spec
		cardinality := view.One
		if relation.outputConfig.IsMany() {
			cardinality = view.Many
		}
		c.Spec.AddRelation(relation.ActualHolderName(), relation.queryJoin, relation.Spec, cardinality)
	}
	return nil
}

func (c *ViewConfig) SQL() string {
	SQL := ""
	if join := c.queryJoin; join != nil {
		SQL = sqlparser.Stringify(c.queryJoin.With)
		SQL = trimParenthasis(SQL)
	}

	return SQL
}

func (s *ViewConfig) TableName() string {
	if s.expandedTable != nil {
		return s.expandedTable.Name
	}
	if s.unexpandedTable != nil {
		return s.unexpandedTable.Name
	}
	return ""
}

func (s *Builder) Build(ctx context.Context) error {
	if err := s.loadAndInitConfig(ctx); err != nil {
		return err
	}

	routerResource := &router.Resource{
		Resource: view.NewResource(config.Config.FlattenTypes()),
	}
	routerResource.Resource.SetFs(s.fs)
	paramIndex := NewParametersIndex(nil, nil)
	var viewCaches = s.caches.Unique()
	consts := &s.constFileContent

	fileName, routerRoutes, err := s.readRouterOptionIfNeeded(routerResource)
	if err != nil || (len(routerRoutes) == 1 && routerRoutes[0].sourceURL == "") {
		if s.options.PartialConfigURL != "" || s.options.isInit { //partial config can be updated
			if err = s.uploadConfigFiles(consts, routerResource, viewCaches); err != nil {
				return err
			}
		}
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

	SQL, err := s.prepareDSQLIfNeeded(ctx, builder)
	if err != nil {
		return err
	}
	builder.sqlStmt = SQL

	if err = s.parseDSQL(ctx, builder, []byte(builder.sqlStmt)); err != nil {
		return err
	}

	if err := s.readRouteSettings(builder); err != nil {
		return err
	}

	//if builder.sqlStmt == "" && builder.option.HandlerType != "" {
	//	builder.sqlStmt = "$Campaign"
	//}

	if strings.TrimSpace(builder.sqlStmt) == "" {
		return nil
	}

	if consts.URL == "" {
		consts.URL = builder.option.ConstFileURL
	} else if consts.URL != builder.option.ConstFileURL && builder.option.ConstFileURL != "" {
		return fmt.Errorf("missmatch const destination, %v - %v", consts.URL, builder.option.ConstFileURL)
	}

	if err := s.initRoute(builder); err != nil {
		return err
	}

	if err := s.initConfigProvider(builder); err != nil {
		return err
	}

	if err := s.buildRouterOutput(builder); err != nil {
		return err
	}

	if err := s.initRouterResource(builder); err != nil {
		return err
	}

	if err := s.buildViews(ctx, builder); err != nil {
		return err
	}

	consts.dedupe()
	if err := s.moveConstParameters(builder, &consts.params); err != nil {
		return err
	}

	if builder.option.Cache != nil {
		*viewCaches = append(*viewCaches, builder.option.Cache)
	}

	return nil
}

func (s *Builder) convertHandlerIfNeeded(builder *routeBuilder) (string, error) {
	if builder.option.StateType == "" || builder.option.HandlerType == "" {
		return "", nil
	}
	statePath := s.options.RelativePath
	if statePath != "" {
		statePath = path.Join(statePath, s.options.GoModulePkg)
	} else {
		statePath = s.options.DSQLOutput
	}

	statePackage := builder.option.StatePackage()
	state, err := codegen.NewState(statePath, builder.option.StateType, config.Config.LookupType)
	if err != nil {
		return "", err
	}
	entityParam := state[0]
	entityType := entityParam.Schema.Type()
	if entityType == nil {
		return "", fmt.Errorf("entity type was empty")
	}
	aType, err := codegen.NewType(statePackage, entityParam.Name, entityType)

	if err != nil {
		return "", err
	}
	tmpl := codegen.NewTemplate(builder.option, &codegen.Spec{Type: aType})
	if entityParam.In.Kind == view.KindRequestBody {
		if entityParam.In.Name != "" {
			tmpl.Imports.AddType(aType.ExpandType(entityParam.In.Name))
		}
	}
	tmpl.Imports.AddType(builder.option.StateType)
	tmpl.Imports.AddType(builder.option.HandlerType)

	tmpl.EnsureImports(aType)
	tmpl.State = state

	if builder.option.Declare == nil {
		builder.option.Declare = map[string]string{}
	}

	builder.option.Declare["Handler"] = simpledName(builder.option.HandlerType)
	builder.option.Declare["State"] = simpledName(builder.option.StateType)

	//builder.option.TypeSrc = &option.TypeSrcConfig{}
	//builder.option.TypeSrc.URL = path.Join(statePath, statePackage)
	//builder.option.TypeSrc.Types = append(builder.option.TypeSrc.Types, builder.option.HandlerType, builder.option.StateType)

	dSQL, err := tmpl.GenerateDSQL(codegen.WithoutBusinessLogic())
	fmt.Printf("%v %v\n", dSQL, err)

	return dSQL + " $" + entityParam.Name, err
}

func simpledName(typeName string) string {
	fragments := strings.Split(typeName, ".")
	return fragments[len(fragments)-1]
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
	if table := aConfig.unexpandedTable; table != nil && len(table.Inner) == 0 && table.SQL != "" {
		tableSQL := expandConsts(table.SQL, builder.option)
		innerSQL, _ := ExtractCondBlock(tableSQL)
		if innerQuery, _ := sqlparser.ParseQuery(innerSQL); innerQuery != nil {
			updaterInnerColumns(table, innerQuery, builder.option)
		}
	}

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

	if s.options.PartialConfigURL != "" {
		partialConfig, err := standalone.NewConfigFromURL(ctx, s.options.PartialConfigURL)
		if err != nil {
			return err
		}

		s.mergeFromPreviousConfig(aConfig, partialConfig)
	}

	err = s.initConfig(ctx, aConfig)
	if err != nil {
		return err
	}

	s.config = aConfig
	if s.options.WriteLocation != "" {
		if err := fsAddJSON(s.fs, aConfig.URL, aConfig); err != nil {
			return err
		}
	}

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
		if err := s.loadConstants(sourceURL, &builder.option.Const); err != nil {
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

func (s *Builder) loadConstants(sourceURL string, dest *map[string]interface{}) error {
	if dest == nil {
		*dest = map[string]interface{}{}
	}
	if sourceURL == "" {
		return nil
	}
	content, err := s.fs.DownloadWithURL(context.Background(), sourceURL)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(bytes.TrimSpace(content), dest); err != nil {
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
	builder.route = &router.Route{
		Method:           builder.option.Method,
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

	if builder.option.HandlerType != "" {
		builder.route.Handler = &router.Handler{
			HandlerType: builder.option.HandlerType,
			StateType:   builder.option.StateType,
		}

		builder.route.Service = router.ServiceTypeExecutor
	}

	builder.paramsIndex.AddUriParams(extractURIParams(builder.route.URI))
	if async := builder.option.Async; async != nil {
		ref, err := s.ConnectorRef(async.Connector)
		if err != nil {
			return err
		}

		builder.route.Async = &router.Async{
			EnsureDBTable:    async.EnsureTable == nil || *async.EnsureTable,
			Connector:        ref,
			PrincipalSubject: async.PrincipalSubject,
			ExpiryTimeInS:    async.ExpiryTimeInS,
			Dataset:          async.Dataset,
			BucketURL:        async.BucketURL,
		}
	}

	return nil
}

func (s *Builder) buildRouterOutput(builder *routeBuilder) error {
	if builder.option.DateFormat != "" {
		builder.route.Output.DateFormat = builder.option.DateFormat
	}

	builder.route.Output.CSV = builder.option.CSV
	builder.route.Output.TabularJSON = builder.option.TabularJSON
	builder.route.Output.DataFormat = builder.option.DataFormat
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
	serviceType := router.ServiceTypeReader

	if IsSQLExecMode(SQL) {
		serviceType = router.ServiceTypeExecutor
	}
	if builder.route.Handler != nil {
		serviceType = router.ServiceTypeHandler
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

	builder.sqlStmt = string(SQLbytes)
	return nil
}

func (s *Builder) parseDSQL(ctx context.Context, builder *routeBuilder, SQL []byte) (err error) {
	if envURL := s.options.EnvURL; envURL != "" {
		envContent, err := s.fs.DownloadWithURL(ctx, envURL)
		if err != nil {
			return err
		}

		aMap := rdata.NewMap()
		if err = json.Unmarshal(envContent, &aMap); err != nil {
			return err
		}

		env := rdata.NewMap()
		env.SetValue("env", aMap)

		templateContent := env.ExpandWithoutUDF(string(SQL))
		SQL = []byte(templateContent)
	}

	hint, SQLs := s.extractRouteSettings(SQL)

	if SQLs, err = s.extractParameterDeclaration(builder, SQLs); err != nil {
		return err
	}

	hints := sanitize.ExtractParameterHints(SQLs)
	SQLs = sanitize.RemoveParameterHints(SQLs, hints)

	tryUnmrashalHintWithWarn(hint, builder.option)

	for paramName, paramType := range builder.option.Declare {
		actualName, err := s.Type(builder.routerResource.Resource, paramType)
		if err != nil {
			return err
		}

		builder.option.Declare[paramName] = actualName
	}

	builder.sqlStmt = SQLs
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

func (s *Builder) uploadConfigFiles(consts *constFileContent, resource *router.Resource, caches []*view.Cache) error {
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
	return nil
}

func (s *Builder) uploadFiles(resourceName string, consts *constFileContent, resource *router.Resource, caches []*view.Cache) error {
	if err := s.uploadConfigFiles(consts, resource, caches); err != nil {
		return err
	}
	return fsAddYAML(s.fs, s.options.RouterURL(resourceName), resource)
}

func (s *Builder) uploadConnectionsDep(resource *router.Resource) error {
	resource.With = append(resource.With, "connections")
	connectors := s.options.Connectors()

	dependency := &view.Resource{
		ModTime:    TimeNow(),
		Connectors: connectors,
	}
	if resource.Resource == nil {
		return nil
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

var constFileName = "const"

func (s *Builder) uploadVariablesDep(resource *router.Resource, consts *constFileContent) error {
	if len(consts.params) == 0 {
		return nil
	}
	consts.dedupe()
	if consts.URL != "" {
		constFileName = consts.URL
	}
	resource.With = append(resource.With, constFileName)
	variablesDep := &view.Resource{ModTime: TimeNow(), Parameters: consts.params}
	variablesURL := s.options.DepURL(constFileName)
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

func (s *Builder) buildMainView(ctx context.Context, builder *routeBuilder, config *ViewConfig) (*view.View, error) {
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

func (b *routeBuilder) paramByName(name string) *view.Parameter {
	param, ok := b.paramsIndex.Param(name)
	if !ok {
		b.routerResource.Resource.AddParameters(param)
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

func (s *Builder) indexExcludedColumns(builder *routeBuilder, config *ViewConfig) error {
	err := s.appendExcluded(builder.route, config, "")
	if err != nil {
		return err
	}

	if err := s.appendMetaExcluded(&builder.route.Exclude, config, ""); err != nil {
		return err
	}

	return err
}

func (s *Builder) appendExcluded(route *router.Route, config *ViewConfig, path string) error {
	if err := s.excludeTableColumns(route, config, path); err != nil {
		return err
	}

	for _, relation := range config.relations {
		holderName, err := s.normalizeFieldName(relation.unexpandedTable.HolderName)
		if err != nil {
			return err
		}

		if err := s.appendExcluded(route, relation, combineSegments(path, holderName)); err != nil {
			return err
		}

		if err := s.appendMetaExcluded(&route.Exclude, relation, path); err != nil {
			return err
		}
	}

	return nil
}

func (s *Builder) appendMetaExcluded(excluded *[]string, config *ViewConfig, path string) error {
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

func (s *Builder) excludeTableColumns(route *router.Route, viewConfig *ViewConfig, path string) error {
	table := viewConfig.expandedTable
	for _, column := range table.Columns {
		for _, except := range column.Except {
			actualFieldName, err := s.normalizeFieldName(except)
			if err != nil {
				return err
			}
			ns, _ := s.normalizeFieldName(column.Ns)
			prefix := path
			if strings.ToLower(viewConfig.mainHolder) != strings.ToLower(ns) { //avoid prefix
				if prefix == "" {
					prefix = ns
				} else {
					prefix += "." + ns
				}
			}
			excludedFieldPath := combineSegments(prefix, actualFieldName)
			route.Exclude = append(route.Exclude, excludedFieldPath)
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
			Limit: 1000,
		}, false, externalParams...)

		if err != nil {
			return nil, err
		}

		if len(paramViewConfig.params) > 0 {
			for _, candidate := range paramViewConfig.params {
				if candidate.Name == childViewConfig.viewName && candidate.ParameterConfig.DataType != "" {
					dataType := candidate.ParameterConfig.DataType

					aView.Schema = &view.Schema{
						DataType:    dataType,
						Cardinality: candidate.ParameterConfig.Cardinality,
					}
				}
			}
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
					Kind: view.KindDataView,
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

func (s *Builder) prepareExternalParameters(builder *routeBuilder, paramViewConfig *ViewParamConfig) ([]*view.Parameter, error) {
	var externalParams []*view.Parameter

	for _, parameter := range paramViewConfig.params {

		if parameter.Auth != "" {
			authParam := &view.Parameter{
				Name:            parameter.Auth,
				In:              &view.Location{Name: "Authorization", Kind: view.KindHeader},
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

		if parameter.In != nil && parameter.In.Kind == view.KindLiteral {
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

	return s.updateViewParam(resource, param, paramConfig, SQL, paramIndex)
}

func (s *Builder) updateViewParam(resource *view.Resource, param *view.Parameter, config *option.ParameterConfig, SQL string, index *ParametersIndex) error {
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
	if config.Cardinality == view.Many {
		param.Schema.Cardinality = view.Many
		//if !strings.HasPrefix(paramType, "[]") {
		//	param.Schema.DataType = "[]" + paramType
		//}
	}

	if config.Codec != "" {
		if param.Output == nil {
			param.Output = &view.Codec{}
		}

		param.Output.Ref = config.Codec
		param.Output.OutputType = param.Schema.DataType

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
		if param.Output == nil {
			param.Output = &view.Codec{Reference: shared.Reference{Ref: config.Codec}}
		}
	}

	return nil
}

func (s *Builder) isUtilParam(builder *routeBuilder, param *view.Parameter) bool {
	return builder.paramsIndex.utilsIndex[param.Name]
}

func (s *Builder) inheritRouteServiceType(builder *routeBuilder, aView *view.View) {
	switch aView.Mode {
	case "", view.ModeQuery:
		builder.route.Service = router.ServiceTypeReader
	case view.ModeExec:
		builder.route.Service = router.ServiceTypeExecutor
	}
}

func (s *Builder) generateRuleIfNeeded(ctx context.Context, SQL []byte) (string, error) {
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
	dsqlOutput := s.options.DSQLOutput
	if s.options.GoModulePkg != "" {
		goFileOutput = path.Join(goFileOutput, s.options.GoModulePkg)
		dsqlOutput = path.Join(dsqlOutput, s.options.GoModulePkg)
	}

	routeBuilder := s.newRouteBuilder(
		&router.Resource{Resource: view.EmptyResource()},
		NewParametersIndex(nil, nil),
		newSession(path.Dir(s.options.Location), s.options.Location, s.options.PluginDst, dsqlOutput, dsqlOutput, goFileOutput),
	)

	cmd := command.New()
	template, err := s.buildCodeTemplate(ctx, routeBuilder, SQL, s.options.PrepareRule)
	if err != nil {
		return "", err
	}
	if err = cmd.Generate(ctx, s.Options.Generate, template); err != nil {
		return "", err
	}
	SQL, err = s.fs.DownloadWithURL(ctx, s.Options.Generate.DSQLLocation())
	return string(SQL), err
}

func (s *Builder) loadGoType(resource *view.Resource, typeSrc *option.TypeSrcConfig) error {
	if typeSrc == nil {
		return nil
	}
	s.normalizeURL(typeSrc)

	dirTypes, err := xreflect.ParseTypes(typeSrc.URL, xreflect.WithTypeLookupFn(config.Config.LookupType))
	if err != nil {
		return err
	}

	aPluginMeta := &pluginGenDeta{
		URL:       typeSrc.URL,
		filesMeta: dirTypes,
	}

	if len(s.extensionTypes) == 0 {
		s.extensionTypes = map[string]bool{}
	}
	for _, typeName := range typeSrc.Types {
		if strings.HasPrefix(typeName, "*") {
			typeName = typeName[1:]
		}
		if err != nil {
			return err
		}
		if shouldGenPlugin := s.shouldGenPlugin(typeName, dirTypes); shouldGenPlugin {
			s.extensionTypes[typeName] = true
		}
		///---------------- TODO fix me
		expandDependentTypes(s.extensionTypes, dirTypes.Methods(typeName))
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
		shouldGenPlugin := s.extensionTypes[actualName]
		if !shouldGenPlugin {
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
			Package:  strings.ToLower(packageName),
		})
	}

	if len(aPluginMeta.Types) > 0 {
		s.plugins = append(s.plugins, aPluginMeta)
	}

	return nil
}

func expandDependentTypes(types map[string]bool, methods []*ast.FuncDecl) {
	for _, m := range methods {

		if len(m.Type.Params.List) > 0 {
			for _, field := range m.Type.Params.List {
				parameterType := StringifyAst(field.Type)
				if strings.HasPrefix(parameterType, "*") {
					parameterType = parameterType[1:]
				}
				types[parameterType] = true
			}
		}
	}
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

func (s *Builder) extractParameterDeclaration(builder *routeBuilder, SQL string) (string, error) {
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

			matched = contentCursor.MatchAfterOptional(whitespaceMatcher, parameterDeclarationMatcher)
			if matched.Code != parameterDeclarationToken {
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
	hint, SQL := sanitize.SplitHint(paramHint)

	if pathStartIndex := strings.Index(holderName, "."); pathStartIndex >= 0 {

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
			Source:      strings.TrimSpace(SQL),
			Transformer: aTransform.Transformer,
		})

		return nil
	}

	qlQuery, _ := sanitize.TryParseStructQLHint(paramHint)
	if qlQuery == nil {
		paramConfig := option.ParameterConfig{}
		hint, sqlQuery := sanitize.SplitHint(paramHint)
		if err = tryUnmarshalHint(hint, &paramConfig); err != nil {
			return err
		}

		if paramConfig.Kind == string(view.KindParam) && paramConfig.Target != nil {
			qlQuery = &sanitize.StructQLQuery{
				SQL:    sqlQuery,
				Source: *paramConfig.Target,
			}
		}
	}

	builder.paramsIndex.AddParamHint(holderName, &sanitize.ParameterHint{
		Parameter:     holderName,
		Hint:          paramHint,
		StructQLQuery: qlQuery,
	})

	return nil
}

func (s *Builder) parseParamHint(cursor *parsly.Cursor) (string, error) {
	aConfig := &paramJSONHintConfig{}
	possibilities := []*parsly.Token{typeMatcher, exprGroupMatcher}
	for len(possibilities) > 0 {
		matched := cursor.MatchAfterOptional(whitespaceMatcher, possibilities...)
		switch matched.Code {
		case typeToken:
			typeContent := matched.Text(cursor)
			typeContent = strings.TrimSpace(typeContent[1 : len(typeContent)-1])

			s.tryUpdateConfigType(typeContent, aConfig)

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

			if err := s.readParamConfigs(&aConfig.ParameterConfig, cursor); err != nil {
				return "", err
			}
			possibilities = []*parsly.Token{}
		default:
			possibilities = []*parsly.Token{}
		}
	}

	matched := cursor.MatchAfterOptional(whitespaceMatcher, commentMatcher)
	actualHint := map[string]interface{}{}
	var sql string
	if matched.Code == commentToken {
		aComment := matched.Text(cursor)
		aComment = aComment[2 : len(aComment)-2]

		hint, SQL := sanitize.SplitHint(aComment)
		if hint != "" {
			if err := json.Unmarshal([]byte(hint), &actualHint); err != nil {
				return "", err
			}
		}

		sql = SQL
	}

	configJson, err := mergeJsonStructs(aConfig.TransformOption, aConfig.ParameterConfig, actualHint)
	if err != nil {
		return "", err
	}

	result := string(configJson)
	if sql != "" {
		result += " " + sql
	}

	return result, nil
}

func (s *Builder) tryUpdateConfigType(typeContent string, aConfig *paramJSONHintConfig) {
	if typeContent == "?" {
		return
	}

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
}

func mergeJsonStructs(args ...interface{}) ([]byte, error) {
	result := map[string]interface{}{}

	for _, arg := range args {
		marshalled, err := json.Marshal(arg)
		if err != nil {
			return nil, err
		}

		if string(marshalled) == "null" || string(marshalled) == "" {
			continue
		}

		if err := json.Unmarshal(marshalled, &result); err != nil {
			return nil, err
		}
	}

	return json.Marshal(result)
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
	if index := strings.LastIndex(packageName, "/"); index != -1 {
		packageName = packageName[index+1:]
	}
	if index := strings.LastIndex(packageName, "."); index != -1 {
		packageName = packageName[index+1:]
	}
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
		return fmt.Errorf("failed to generate sideeffect import: %w, %s", err, source)
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

func (s *Builder) detectSinkColumn(ctx context.Context, db *sql.DB, SQL string) ([]sink.Column, error) {
	SQL = trimParenthasis(SQL)

	query, err := sqlparser.ParseQuery(SQL)
	if query != nil {
		from := sqlparser.Stringify(query.From.X)
		if query.List.IsStarExpr() && !strings.Contains(from, "SELECT") {
			return nil, nil //use table metadata
		}
		query.Window = nil
		query.Qualify = nil
		query.Limit = nil
		query.Offset = nil
		SQL = sqlparser.Stringify(query)
		SQL += " LIMIT 1"
	}
	stmt, err := db.PrepareContext(ctx, SQL)
	if err != nil {
		panic(1)
	}
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx)

	if err != nil {
		panic(err)
		return nil, err
	}
	defer rows.Close()
	var result []sink.Column
	rows.Next()
	if rows != nil {
		if columnsTypes, _ := rows.ColumnTypes(); len(columnsTypes) != 0 {
			columns := sio.TypesToColumns(columnsTypes)
			for _, item := range columns {
				result = append(result, sink.Column{
					Name: item.Name(),
					Type: item.DatabaseTypeName(),
				})
			}
		}
	}
	return result, nil
}

func (s *Builder) prepareDSQLIfNeeded(ctx context.Context, builder *routeBuilder) (string, error) {
	if s.options.PrepareRule != "" {
		return s.generateRuleIfNeeded(ctx, []byte(builder.sqlStmt))
	}

	hint, _ := sanitize.SplitHint(builder.sqlStmt)

	if err := tryUnmarshalHint(hint, builder.option); err != nil {
		return "", err
	}

	if builder.option.HandlerType == "" {
		return builder.sqlStmt, nil
	}

	return s.convertHandlerIfNeeded(builder)
}

func trimParenthasis(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return text
	}
	if text[0] == '(' {
		text = text[1:]
	}
	if text[len(text)-1] == ')' {
		text = text[:len(text)-1]
	}
	return text
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

func StringifyAst(expr ast.Expr) string {
	builder := strings.Builder{}
	stringifyAst(expr, &builder)
	return builder.String()
}

func stringifyAst(expr ast.Expr, builder *strings.Builder) error {
	switch actual := expr.(type) {
	case *ast.BasicLit:
		builder.WriteString(actual.Value)
	case *ast.Ident:
		builder.WriteString(actual.Name)
	case *ast.IndexExpr:
		if err := stringifyAst(actual.X, builder); err != nil {
			return err
		}
		builder.WriteString("[")
		if err := stringifyAst(actual.Index, builder); err != nil {
			return err
		}
		builder.WriteString("]")
	case *ast.SelectorExpr:
		if err := stringifyAst(actual.X, builder); err != nil {
			return err
		}
		builder.WriteString(".")
		return stringifyAst(actual.Sel, builder)
	case *ast.ParenExpr:
		builder.WriteString("(")
		if err := stringifyAst(actual.X, builder); err != nil {
			return err
		}
		builder.WriteString(")")
	case *ast.CallExpr:
		if err := stringifyAst(actual.Fun, builder); err != nil {
			return err
		}
		builder.WriteString("(")
		for i := 0; i < len(actual.Args); i++ {
			if i > 0 {
				builder.WriteString(",")
			}
			if err := stringifyAst(actual.Args[i], builder); err != nil {
				return err
			}
		}
		builder.WriteString(")")
	case *ast.BinaryExpr:
		if err := stringifyAst(actual.X, builder); err != nil {
			return err
		}
		builder.WriteString(actual.Op.String())
		return stringifyAst(actual.Y, builder)
	case *ast.UnaryExpr:
		builder.WriteString(actual.Op.String())
		return stringifyAst(actual.X, builder)
	case *ast.ArrayType:
		return stringifyAst(actual.Elt, builder)
	case *ast.StarExpr:
		builder.WriteString("*")
		return stringifyAst(actual.X, builder)
	default:
		return fmt.Errorf("unsupported node: %T", actual)
	}
	return nil
}

func extractRelationAliases(join *query.Join) (string, string) {
	relAlias := ""
	refAlias := ""
	sqlparser.Traverse(join.On, func(n node.Node) bool {
		switch actual := n.(type) {
		case *qexpr.Binary:
			if xSel, ok := actual.X.(*qexpr.Selector); ok {

				if xSel.Name == join.Alias {

					refAlias = xSel.Name
				} else if relAlias == "" {
					relAlias = xSel.Name
				}
			}
			if ySel, ok := actual.Y.(*qexpr.Selector); ok {
				if ySel.Name == join.Alias {
					refAlias = ySel.Name
				} else if relAlias == "" {
					relAlias = ySel.Name
				}
			}
			return true
		}
		return true
	})
	return relAlias, refAlias
}

func (c *ViewConfig) IsToMany() bool {
	if c.parent == nil {
		return c.outputConfig.IsMany()
	}
	if c.parent.IsToMany() {
		return true
	}
	return false
}
