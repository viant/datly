package translator

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/asset"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/plugin"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/internal/translator/parser"
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
	async2 "github.com/viant/xdatly/handler/async"
	"path"
	"reflect"
	"strings"
)

type Service struct {
	Repository *Repository //TODO init repo with basic config and dependencies
	Plugins    []*options.Plugin
	fs         afs.Service
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

	if err = s.persistRouterRule(ctx, resource, router.ServiceTypeExecutor); err != nil {
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
	query, err := sqlparser.ParseQuery(dSQL, handleVeltyExpression())
	if err != nil {
		return err
	}
	resource.Rule.Root = query.From.Alias
	if err = resource.Rule.Viewlets.Init(ctx, query, resource, s.initReaderViewlet, s.buildQueryViewletType); err != nil {
		return err
	}
	root := resource.Rule.RootView()
	if err := root.buildView(resource.Rule, view.ModeQuery); err != nil {
		return err
	}

	resource.Rule.updateExclude(resource.Rule.RootViewlet())
	if err = resource.Rule.Viewlets.Each(func(viewlet *Viewlet) error {
		return s.persistView(viewlet, resource, view.ModeQuery)
	}); err != nil {
		return err
	}
	if err = s.persistRouterRule(ctx, resource, router.ServiceTypeReader); err != nil {
		return err
	}
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

func (s *Service) persistRouterRule(ctx context.Context, resource *Resource, service router.ServiceType) error {
	baseRuleURL := s.Repository.RuleBaseURL(resource.rule)

	route := &resource.Rule.Route
	ruleName := s.Repository.RuleName(resource.rule)
	route.Service = service
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
	if service == router.ServiceTypeExecutor {
		resource.Rule.Route.Field = aState.BodyField()
	}

	if len(resource.OutputState) > 0 {
		outputState, err := resource.OutputState.Compact(resource.rule.Module)
		if err != nil {
			return err
		}
		resource.Rule.Route.Output.Parameters = outputState.ViewParameters()
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
	setter.SetStringIfEmpty(&async.Connector, s.DefaultConnector())
	route.Async = &router.Async{
		EnsureDBTable:    async.EnsureTable == nil || *async.EnsureTable,
		Connector:        view.NewRefConnector(async.Connector),
		PrincipalSubject: async.PrincipalSubject,
		ExpiryTimeInS:    async.ExpiryTimeInS,
		Config: async2.Config{
			Dataset:   async.Dataset,
			BucketURL: async.BucketURL,
		},
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
	//if aView.Cache != nil {
	//	s.GitRepository.AppendCache(aView.Cache)
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

func New(config *Config, service afs.Service) *Service {
	ret := &Service{Repository: NewRepository(config), fs: service}
	return ret
}
