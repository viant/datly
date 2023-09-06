package translator

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/config"
	"github.com/viant/datly/gateway/router"
	"github.com/viant/datly/internal/asset"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/plugin"
	"github.com/viant/datly/internal/translator/parser"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/service"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/discover"
	"github.com/viant/datly/view/state"
	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
	"github.com/viant/toolbox/format"
	"github.com/viant/xreflect"
	"gopkg.in/yaml.v3"
	"path"
	"reflect"
	"strings"
	"time"
)

type Service struct {
	Repository *Repository //TODO init repo with basic config and dependencies
	Plugins    []*options.Plugin
	fs         afs.Service
	signature  *contract.Service
}

func (s *Service) Translate(ctx context.Context, rule *options.Rule, dSQL string, opts *options.Options) (err error) {
	resource := NewResource(rule, s.Repository.Config.repository, &s.Repository.Messages)
	resource.State.Append(s.Repository.State...)
	if err = resource.InitRule(&dSQL, ctx, s.Repository.fs, opts); err != nil {
		return err
	}
	if err = resource.IncludeSnippets(ctx, s.fs, &dSQL); err != nil {
		return err
	}
	if err = resource.parseImports(ctx, &dSQL); err != nil {
		return err
	}

	if err = resource.ExtractDeclared(&dSQL); err != nil {
		return err
	}

	parameters := resource.Declarations.OutputState
	componentParameters := parameters.FilterByKind(state.KindComponent)
	if err = s.updateComponentType(ctx, resource, componentParameters); err != nil {
		return err
	}

	dSQL = rule.NormalizeSQL(dSQL, handleVeltyExpression)
	if resource.IsExec() || resource.Rule.Handler != nil {
		if err := s.translateExecutorDSQL(ctx, resource, dSQL); err != nil {
			return err
		}
	} else {
		if err = s.translateReaderDSQL(ctx, resource, dSQL); err != nil {
			return err
		}
	}
	s.Repository.Resource = append(s.Repository.Resource, resource)
	return nil
}

func (s *Service) discoverComponentContract(ctx context.Context, resource *Resource, location *state.Location) (*contract.Signature, error) {
	var err error
	if s.signature == nil {
		prefix := path.Join(s.Repository.Config.APIPrefix, resource.rule.Prefix)
		if s.signature, err = contract.New(ctx, prefix, s.Repository.Config.RouteURL); err != nil {
			return nil, err
		}
	}

	componentTypeName := normalizeComponentType(location)
	location.Name = strings.ReplaceAll(location.Name, ".", "/")
	method, URI := shared.ExtractPath(location.Name)
	ret, err := s.signature.Signature(method, URI)
	if ret != nil {
		ret.AdjustedRegisteredType(componentTypeName)
	}
	return ret, err
}

func normalizeComponentType(location *state.Location) string {
	componentTypeName := strings.ReplaceAll(location.Name, "-", "_")
	componentTypeName = strings.ReplaceAll(componentTypeName, ".", "_")
	componentTypeName = format.CaseLowerUnderscore.Format(componentTypeName, format.CaseUpperCamel)
	return componentTypeName
}

func (s *Service) translateExecutorDSQL(ctx context.Context, resource *Resource, DSQL string) (err error) {
	if err = s.buildExecutorView(ctx, resource, DSQL); err != nil {
		return err
	}
	resource.buildParameterViews()
	if err := resource.ensureViewParametersSchema(ctx, s.buildQueryViewletType); err != nil {
		return err
	}
	if err := resource.ensurePathParametersSchema(ctx); err != nil {
		return err
	}

	if err = resource.Rule.Viewlets.Each(func(viewlet *Viewlet) error {
		return s.persistView(viewlet, resource, view.ModeExec)
	}); err != nil {
		return err
	}

	if err = s.persistRouterRule(ctx, resource, service.TypeExecutor); err != nil {
		return err
	}
	return nil
}

func (s *Service) buildExecutorView(ctx context.Context, resource *Resource, DSQL string) (err error) {
	ruleName := s.Repository.RuleName(resource.rule)
	resource.Rule.Root = ruleName
	viewlet := NewViewlet(ruleName, DSQL, nil, resource)
	viewlet.Table, err = s.extractDMLTables(ctx, resource)
	if err != nil {
		return fmt.Errorf("failed to build exec view: %v, unable to extract DML tables: %w", ruleName, err)
	}
	if viewlet.Table != nil {
		viewlet.View.TableBatches = map[string]bool{
			viewlet.Table.Name: true,
		}
	}
	viewlet.Connector = s.DefaultConnector()
	resource.Rule.Viewlets.Append(viewlet)
	SQL := viewlet.Resource.State.Expand(viewlet.SQL)
	aTemplate, err := parser.NewTemplate(viewlet.SQL, &viewlet.Resource.State)
	if err != nil {
		return fmt.Errorf("invalid DSQL: %w, %s", err, SQL)
	}
	aTemplate.DetectTypes(viewlet.UpdateParameterType)
	viewlet.SanitizedSQL = aTemplate.Sanitize()
	return err
}

func (s *Service) translateReaderDSQL(ctx context.Context, resource *Resource, dSQL string) error {
	aQuery, err := sqlparser.ParseQuery(dSQL, handleVeltyExpression())
	if err != nil {
		return err
	}
	resource.Rule.Root = aQuery.From.Alias
	if err = resource.Rule.Viewlets.Init(ctx, aQuery, resource, s.initReaderViewlet, s.buildQueryViewletType); err != nil {
		return err
	}
	root := resource.Rule.RootView()
	if err := root.buildView(resource.Rule, view.ModeQuery); err != nil {
		return err
	}

	resource.Rule.updateExclude(resource.Rule.RootViewlet())

	cache := discover.Columns{Items: make(map[string]view.Columns)}

	if err = s.generateViewColumns(err, resource, cache); err != nil {
		return err
	}

	rootViewlet := resource.Rule.RootViewlet()
	if err = s.updateOutputParameters(resource, rootViewlet); err != nil {
		return err
	}

	if err = resource.Rule.Viewlets.Each(func(viewlet *Viewlet) error {
		return s.persistView(viewlet, resource, view.ModeQuery)
	}); err != nil {
		return err
	}

	if err = s.persistRouterRule(ctx, resource, service.TypeReader); err != nil {
		return err
	}
	return nil
}

func (s *Service) generateViewColumns(err error, resource *Resource, columnDiscovery discover.Columns) error {
	var hasSummary bool
	if err = resource.Rule.Viewlets.Each(func(viewlet *Viewlet) error {
		if viewlet.View.Template != nil && viewlet.View.Template.Summary != nil {
			hasSummary = true
		}
		if columns := viewlet.Spec.Columns; len(columns) > 0 {
			columnDiscovery.Items[viewlet.Name] = view.NewColumns(columns)
			//TODO add meta column generation for SUMMARY/Meta tempalte
		}
		s.updateViewOutputType(viewlet, true)
		return nil
	}); err != nil {
		return err
	}
	if !hasSummary && !resource.Rule.IsGeneratation { //TODO add support
		err = s.persistViewMetaColumn(columnDiscovery, resource)
		if err != nil {
			return err
		}
	}
	return err
}

func (s *Service) updateViewOutputType(viewlet *Viewlet, withTypeDef bool) {
	if viewlet.Resource.Rule.IsGeneratation {
		return
	}
	if schema := viewlet.View.Schema; schema != nil && (schema.Type() != nil || schema.DataType != "") {
		return
	}

	for _, relField := range viewlet.Spec.Type.RelationFields {
		relViewlet := viewlet.Resource.Rule.Viewlets.Lookup(relField.Relation)
		s.updateViewOutputType(relViewlet, false)

		relType := relViewlet.View.Schema.Type()
		if relType.Kind() == reflect.Struct {
			relType = reflect.PtrTo(relType)
		}
		if relField.Cardinality == state.Many {
			relType = reflect.SliceOf(relType)
		}
		relField.Schema = state.NewSchema(relType)
		relField.Schema.Cardinality = relField.Cardinality
		relField.Name = relViewlet.Holder
	}

	fields := viewlet.Spec.Type.Fields()
	if len(fields) > 0 {
		viewlet.View.Schema = &state.Schema{}
		paramSchema := reflect.StructOf(fields)
		viewlet.View.Schema.SetType(paramSchema)
	}
	if !withTypeDef {
		return
	}
	viewlet.TypeDefinition = viewlet.Spec.TypeDefinition("", false)
	viewlet.TypeDefinition.Cardinality = ""
	viewlet.TypeDefinition.Name = TypeDefinitionName(viewlet)

}

func TypeDefinitionName(viewlet *Viewlet) string {
	return strings.Title(viewlet.Name) + "Output"
}

func (s *Service) persistViewMetaColumn(cache discover.Columns, resource *Resource) error {
	if len(cache.Items) == 0 {
		return nil
	}
	cache.ModTime = time.Now()
	data, err := yaml.Marshal(cache)
	if err != nil {
		return fmt.Errorf("failed to encode: %T, %w", cache, err)
	}
	baseRuleURL := s.Repository.RuleBaseURL(resource.rule)
	ruleName := s.Repository.RuleName(resource.rule)
	cacheURL := url.Join(baseRuleURL, ".meta", ruleName+".yaml")
	s.Repository.Files.Append(asset.NewFile(cacheURL, string(data)))
	return nil
}

func handleVeltyExpression() sqlparser.Option {
	return sqlparser.WithErrorHandler(func(err error, cur *parsly.Cursor, destNode interface{}) error {
		fromNode, ok := destNode.(*query.From)
		if !ok {
			return err
		}
		match := cur.MatchOne(parser.IfBlockMatcher)
		if match.Code == parser.IfBlockToken {
			fromNode.Unparsed = match.Text(cur)
			return nil
		}

		return err
	})
}

func (s *Service) persistRouterRule(ctx context.Context, resource *Resource, serviceType service.Type) error {
	baseRuleURL := s.Repository.RuleBaseURL(resource.rule)
	route := &resource.Rule.Route
	ruleName := s.Repository.RuleName(resource.rule)
	route.Service = serviceType
	route.View = view.NewRefView(resource.Rule.Root)
	route.Content.CSV = resource.Rule.CSV
	route.Content.TabularJSON = resource.Rule.TabularJSON
	route.Content.XML = resource.Rule.XML
	route.Output.DataFormat = resource.Rule.DataFormat

	s.applyAsyncOption(resource, route)

	if resource.rule.Generated { //translation from generator
		resource.Rule.applyGeneratorOutputSetting()
	} else {
		resource.Rule.Route.URI = path.Join(resource.repository.APIPrefix, resource.rule.Prefix, resource.Rule.Route.URI)
	}

	aState, err := resource.State.Compact(resource.rule.Module)
	if err != nil {
		return fmt.Errorf("failed to compact aState: %w", err)
	}
	resource.Resource.Parameters = aState.RemoveReserved().ViewParameters()
	if serviceType == service.TypeExecutor {
		resource.Rule.Route.Output.Field = aState.BodyField()
	}

	routerResource, err := s.buildRouterResource(ctx, resource)
	if err != nil {
		return fmt.Errorf("failed to build router resource: %+v, %w", routerResource, err)
	}
	data, err := asset.EncodeYAML(routerResource)
	if err != nil {
		return fmt.Errorf("failed to encode: %+v, %w", routerResource, err)
	}
	s.Repository.Files.Append(asset.NewFile(url.Join(baseRuleURL, ruleName+".yaml"), string(data)))
	return nil
}

func (s *Service) applyAsyncOption(resource *Resource, route *router.Route) {
	async := resource.Rule.Async
	if async == nil {
		return
	}

	if async.Jobs.Connector == nil {
		async.Jobs.Connector = view.NewRefConnector(s.DefaultConnector())
	}
	if async.JobID == nil {
		async.JobID = &state.Parameter{Name: "JobID", In: state.NewQueryLocation("")}
	}

}

func (s *Service) persistView(viewlet *Viewlet, resource *Resource, mode view.Mode) error {
	if mode == view.ModeQuery {
		resource.Rule.updateExclude(viewlet)
	}
	viewlet.applyOutputShorthands()
	if viewlet.IsMetaView() {
		return nil
	}
	if resource.Rule.Async != nil {
		viewlet.View.View.Async = &view.Async{
			MarshalRelations: true,
			Table:            "",
		}
	}
	baseRuleURL := s.Repository.RuleBaseURL(resource.rule)
	ruleName := s.Repository.RuleName(resource.rule)
	if err := viewlet.View.buildView(resource.Rule, mode); err != nil {
		return err
	}

	//TODO move cache to dependency but allow local different TTL override
	//	aView := &viewlet.View.View
	//if aView.Columns != nil {
	//	s.GitRepository.AppendCache(aView.Columns)
	//}

	resource.Resource.Views = append(resource.Resource.Views, &viewlet.View.View)
	viewlet.View.GenerateFiles(baseRuleURL, ruleName, &s.Repository.Files)
	if viewlet.TypeDefinition != nil {
		viewType := reflect.StructOf(viewlet.Spec.Type.Fields())
		viewlet.TypeDefinition.DataType = viewType.String()
		viewlet.TypeDefinition.Fields = nil
		resource.AppendTypeDefinition(viewlet.TypeDefinition)
	}
	return nil
}

// initReaderViewlet detect SQL dependent Table columns with implicit parameters type to produce sanitized SQL
func (s *Service) initReaderViewlet(ctx context.Context, viewlet *Viewlet) error {
	if viewlet.Connector == "" {
		viewlet.Connector = s.DefaultConnector()
	}
	db, err := s.Repository.LookupDb(viewlet.Connector)
	if err != nil {
		return err
	}
	SQL := viewlet.Resource.State.Expand(viewlet.SQL)
	if err = viewlet.discoverTables(ctx, db, SQL); err != nil {
		return err
	}

	if viewlet.Table != nil && viewlet.Table.OutputJSONHint != "" {
		if err = viewlet.mergeTableJSONHint(viewlet.Table.OutputJSONHint); err != nil {
			return err
		}
	}

	aTemplate, err := parser.NewTemplate(viewlet.SQL, &viewlet.Resource.State)
	if err != nil {
		return fmt.Errorf("invalid DSQL: %w, %s", err, SQL)
	}
	aTemplate.DetectTypes(viewlet.UpdateParameterType)
	viewlet.SanitizedSQL = aTemplate.Sanitize()
	viewlet.View.Name = viewlet.Name
	return nil
}

func (s *Service) DefaultConnector() string {
	return s.Repository.Connectors[0].Name
}

// buildQueryViewletType build SQL/Table specification (field/column/keys) type
func (s *Service) buildQueryViewletType(ctx context.Context, viewlet *Viewlet) error {
	db, err := s.Repository.LookupDb(viewlet.Connector)
	if err != nil {
		return err
	}
	if viewlet.Table == nil {
		if err = s.initReaderViewlet(ctx, viewlet); err != nil {
			return err
		}
	}
	if viewlet.Expanded, err = viewlet.Resource.expandSQL(viewlet); err != nil {
		return err
	}
	return s.buildViewletType(ctx, db, viewlet)
}

func (s *Service) buildViewletType(ctx context.Context, db *sql.DB, viewlet *Viewlet) (err error) {
	if viewlet.Spec, err = inference.NewSpec(ctx, db, &s.Repository.Messages, viewlet.Table.Name, viewlet.Expanded.Query, viewlet.Expanded.Args...); err != nil {
		return fmt.Errorf("failed to create spec for %v, %w", viewlet.Name, err)
	}
	viewlet.Spec.Namespace = viewlet.Name
	pkg := ""
	cardinality := state.Many
	if err = viewlet.Spec.BuildType(pkg, viewlet.Name, cardinality, viewlet.whitelistMap(), nil); err != nil {
		return err
	}
	return nil
}

func (s *Service) Init(ctx context.Context) error {
	return s.Repository.Init(ctx)
}

func (s *Service) buildRouterResource(ctx context.Context, resource *Resource) (*router.Resource, error) {
	result := &router.Resource{}
	if resource.Rule.Cache != nil {
		s.Repository.Caches.Append(resource.Rule.Cache)
	}
	if len(s.Repository.Caches) > 0 {
		resource.Rule.With = append(resource.Rule.With, "cache")
	}

	result.With = resource.Rule.With
	//
	if err := s.handleCustomTypes(ctx, resource); err != nil {
		return nil, err
	}

	result.Resource = &resource.Resource
	result.ColumnsDiscovery = true
	resource.Rule.applyRootViewRouteShorthands()
	route := &resource.Rule.Route
	result.Routes = append(result.Routes, route)
	return result, nil
}

func (s *Service) handleCustomTypes(ctx context.Context, resource *Resource) (err error) {
	if len(resource.CustomTypeURLs) == 0 {
		return nil
	}
	modLocation := resource.rule.GoModuleLocation()
	var info *plugin.Info
	URL := resource.CustomTypeURLs[0]
	customTypeLocation := url.Path(URL)
	if strings.Contains(customTypeLocation, modLocation) {
		info, err = plugin.NewInfo(ctx, modLocation)
	} else {
		info, err = plugin.NewInfo(ctx, URL)
	}
	if err != nil {
		return fmt.Errorf("failed to detect custom type: %v %w", URL, err)
	}

	if info.IntegrationMode == plugin.ModeStandalone {
		pluginCmd := &options.Plugin{}
		pluginCmd.Name = resource.rule.RuleName()
		pluginCmd.Source = append(pluginCmd.Source, URL)
		pluginCmd.Repository = s.Repository.Config.repository.RepositoryURL
		if err := pluginCmd.Init(); err != nil {
			return fmt.Errorf("failed to create standalone plugin for %v, %w", URL, err)
		}
		pluginCmd.BuildArgs = nil
		//pluginCmd.BuildArgs = []string{"'-gcflags \"all=-N -l\"'"}
		s.Plugins = append(s.Plugins, pluginCmd)
	}
	resource.AdjustCustomType(info)
	return nil
}

func (s *Service) updateComponentType(ctx context.Context, resource *Resource, parameters inference.State) error {
	if len(parameters) == 0 {
		return nil
	}
	for _, parameter := range parameters {
		signature, err := s.discoverComponentContract(ctx, resource, parameter.In)
		if err != nil {
			return fmt.Errorf("failed to discover component %v output type: %w", parameter.In.Name, err)
		}
		parameter.In.Name = signature.Method + ":" + signature.URI
		parameter.Schema = signature.Output
		for _, typeDef := range signature.Types {
			if err = config.Config.Types.Register(typeDef.Name, xreflect.WithTypeDefinition(typeDef.DataType)); err != nil {
				return err
			}
		}
		for i := range signature.Types {
			resource.AppendTypeDefinition(signature.Types[i])
		}
	}
	return nil
}

func New(config *Config, service afs.Service) *Service {
	ret := &Service{Repository: NewRepository(config), fs: service}
	return ret
}
