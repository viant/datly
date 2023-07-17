package translator

import (
	"context"
	"fmt"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/asset"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/translator/parser"
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
	"reflect"
)

type Service struct {
	Repository *Repository //TODO init repo with basic config and dependencies
}

func (s *Service) Translate(ctx context.Context, rule *options.Rule, dSQL string) error {
	resource := NewResource(rule, s.Repository.Config.repository)
	resource.State.Append(s.Repository.State...)
	if err := resource.InitRule(&dSQL); err != nil {
		return err
	}
	if err := resource.ExtractDeclared(&dSQL); err != nil {
		return err
	}
	if resource.IsExec() {
		if err := s.translateExecutorDSQL(ctx, resource, dSQL); err != nil {
			return err
		}
	} else {
		if err := s.translateReaderDSQL(ctx, resource, dSQL); err != nil {
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
	if err := resource.ensureViewParametersSchema(ctx, s.buildViewletType); err != nil {
		return err
	}

	if err = resource.Rule.Viewlets.Each(func(viewlet *Viewlet) error {
		return s.persistView(viewlet, resource, view.ModeExec)
	}); err != nil {
		return err
	}
	if err = s.persistRouterRule(resource, router.ServiceTypeExecutor); err != nil {
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
	query, err := sqlparser.ParseQuery(dSQL)
	if err != nil {
		return err
	}
	resource.Rule.Root = query.From.Alias
	if err = resource.Rule.Viewlets.Init(ctx, query, resource, s.initReaderViewlet, s.buildViewletType); err != nil {
		return err
	}
	root := resource.Rule.RootView()
	if err := root.buildView(resource.Rule, view.ModeQuery); err != nil {
		return err
	}
	if err = resource.Rule.Viewlets.Each(func(viewlet *Viewlet) error {
		return s.persistView(viewlet, resource, view.ModeQuery)
	}); err != nil {
		return err
	}
	if err = s.persistRouterRule(resource, router.ServiceTypeReader); err != nil {
		return err
	}
	return nil
}

func (s *Service) persistRouterRule(resource *Resource, service router.ServiceType) error {
	baseRuleURL := s.Repository.RuleBaseURL(resource.rule)
	ruleName := s.Repository.RuleName(resource.rule)
	resource.Rule.Route.Service = service
	resource.Rule.Route.View = view.NewRefView(resource.Rule.Root)
	state, err := resource.State.Compact(resource.rule.Module)
	if err != nil {
		return fmt.Errorf("failed to compact state: %w", err)
	}
	resource.Resource.Parameters = state.RemoveReserved().ViewParameters()
	routerResource := s.buildRouterResource(resource)
	data, err := asset.EncodeYAML(routerResource)
	if err != nil {
		return fmt.Errorf("failed to encode: %+v, %w", routerResource, err)
	}
	s.Repository.Files.Append(asset.NewFile(url.Join(baseRuleURL, ruleName+".yaml"), string(data)))
	return nil
}

func (s *Service) persistView(viewlet *Viewlet, resource *Resource, mode view.Mode) error {
	if mode == view.ModeQuery {
		resource.Rule.updateExclude(viewlet)
	}
	if viewlet.IsMetaView() {
		return nil
	}
	baseRuleURL := s.Repository.RuleBaseURL(resource.rule)
	ruleName := s.Repository.RuleName(resource.rule)
	if err := viewlet.View.buildView(resource.Rule, mode); err != nil {
		return err
	}

	//TODO move cache to dependency but allow local different TTL override
	//	aView := &viewlet.View.View
	//if aView.Cache != nil {
	//	s.Repository.AppendCache(aView.Cache)
	//}

	resource.Resource.Views = append(resource.Resource.Views, &viewlet.View.View)
	viewlet.View.GenerateFiles(baseRuleURL, ruleName, &s.Repository.Files)
	if viewlet.TypeDefinition != nil {
		viewType := reflect.StructOf(viewlet.Spec.Type.Fields())
		viewlet.TypeDefinition.DataType = viewType.String()
		viewlet.TypeDefinition.Fields = nil
		resource.Resource.Types = append(resource.Resource.Types, viewlet.TypeDefinition)
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

// buildViewletType build SQL/Table specification (field/column/keys) type
func (s *Service) buildViewletType(ctx context.Context, viewlet *Viewlet) error {
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
	if viewlet.Spec, err = inference.NewSpec(ctx, db, viewlet.Table.Name, viewlet.Expanded.Query, viewlet.Expanded.Args...); err != nil {
		return fmt.Errorf("failed to create spec for %v, %w", viewlet.Name, err)
	}
	viewlet.Spec.Namespace = viewlet.Name
	pkg := ""
	cardinality := view.Many
	if err := viewlet.Spec.BuildType(pkg, viewlet.Name, cardinality, viewlet.whitelistMap(), nil); err != nil {
		return err
	}
	return nil
}

func (s *Service) Init(ctx context.Context) error {
	return s.Repository.Init(ctx)
}

func (s *Service) buildRouterResource(resource *Resource) *router.Resource {
	result := &router.Resource{}
	if resource.Rule.Cache != nil {
		s.Repository.Caches.Append(resource.Rule.Cache)
	}
	if len(s.Repository.Caches) > 0 {
		resource.Rule.With = append(resource.Rule.With, "cache")
	}

	result.With = resource.Rule.With
	result.Resource = &resource.Resource
	result.ColumnsDiscovery = true
	resource.Rule.applyRootViewOutputShorthands()

	route := &resource.Rule.Route

	result.Routes = append(result.Routes, route)
	return result
}

func New(config *Config) *Service {
	ret := &Service{Repository: NewRepository(config)}
	return ret
}
