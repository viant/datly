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
	if !resource.IsExec() {
		if err := s.translateQuery(ctx, resource, dSQL); err != nil {
			return err
		}
	}
	s.Repository.Resource = append(s.Repository.Resource, resource)
	return nil
}

func (s *Service) translateQuery(ctx context.Context, resource *Resource, dSQL string) error {
	if err := parser.ExtractParameterHints(dSQL, &resource.State); err != nil {
		return err
	}
	dSQL = parser.RemoveParameterHints(dSQL, resource.State)
	query, err := sqlparser.ParseQuery(dSQL)
	if err != nil {
		return err
	}
	resource.Rule.Root = query.From.Alias
	if err = resource.Rule.Namespaces.Init(ctx, query, resource, s.initNamespace, s.buildNamespaceType); err != nil {
		return err
	}
	root := resource.Rule.RootView()
	if err := root.BuildView(resource.Rule); err != nil {
		return err
	}
	if err = resource.Rule.Namespaces.Each(func(namespace *Namespace) error {
		return s.persistView(namespace, resource)
	}); err != nil {
		return err
	}
	if err = s.persistRouterRule(resource); err != nil {
		return err
	}
	return nil
}

func (s *Service) persistRouterRule(resource *Resource) error {
	baseRuleURL := s.Repository.RuleBaseURL(resource.rule)
	ruleName := s.Repository.RuleName(resource.rule)
	resource.Rule.Route.Service = "Reader"
	resource.Rule.Route.View = view.NewRefView(resource.Rule.Root)
	resource.Resource.Parameters = resource.State.ViewParameters()
	routerResource := s.buildRouterResource(resource)
	data, err := asset.EncodeYAML(routerResource)
	if err != nil {
		return fmt.Errorf("failed to encode: %+v, %w", routerResource, err)
	}
	s.Repository.Files.Append(asset.NewFile(url.Join(baseRuleURL, ruleName+".yaml"), string(data)))
	return nil
}

func (s *Service) persistView(namespace *Namespace, resource *Resource) error {
	baseRuleURL := s.Repository.RuleBaseURL(resource.rule)
	ruleName := s.Repository.RuleName(resource.rule)
	if err := namespace.View.BuildView(resource.Rule); err != nil {
		return err
	}
	resource.Rule.updateExclude(namespace)
	resource.Resource.Views = append(resource.Resource.Views, &namespace.View.View)
	namespace.View.GenerateFiles(baseRuleURL, ruleName, &s.Repository.Files)
	return nil
}

//initNamespace detect SQL dependent Table columns with implicit parameters type to produce sanitized SQL
func (s *Service) initNamespace(ctx context.Context, n *Namespace) error {
	if n.Connector == "" {
		n.Connector = s.Repository.Connectors[0].Name
	}
	db, err := s.Repository.LookupDb(n.Connector)
	if err != nil {
		return err
	}
	SQL := n.Resource.State.Expand(n.SQL)
	if err = n.discoverTables(ctx, db, SQL); err != nil {
		return err
	}
	aTemplate, err := parser.NewTemplate(n.SQL, &n.Resource.State)
	if err != nil {
		return fmt.Errorf("invalid DSQL: %w, %s", err, SQL)
	}
	aTemplate.DetectTypes(n.UpdateParameterType)
	n.SanitizedSQL = aTemplate.Sanitize()
	n.View.Name = n.Name
	return nil
}

//buildNamespaceType build SQL/Table specification (field/column/keys) type
func (s *Service) buildNamespaceType(ctx context.Context, n *Namespace) error {
	db, err := s.Repository.LookupDb(n.Connector)
	if err != nil {
		return err
	}
	if n.Expanded, err = n.Resource.expandSQL(n); err != nil {
		return err
	}
	if n.Spec, err = inference.NewSpec(ctx, db, n.Table.Name, n.Expanded.Query, n.Expanded.Args...); err != nil {
		return fmt.Errorf("failed to create spec for %v, %w", n.Name, err)
	}
	n.Spec.Namespace = n.Name
	pkg := ""
	cardinality := view.Many
	if err := n.Spec.BuildType(pkg, n.Name, cardinality, n.whitelistMap(), nil); err != nil {
		return err
	}
	return nil
}

func (s *Service) Init(ctx context.Context) error {
	return s.Repository.Init(ctx)
}

func (s *Service) buildRouterResource(resource *Resource) *router.Resource {
	result := &router.Resource{}
	result.With = resource.Rule.With
	result.Resource = &resource.Resource
	result.ColumnsDiscovery = true
	result.Routes = append(result.Routes, &resource.Rule.Route)
	return result
}

func New(config *Config) *Service {
	ret := &Service{Repository: NewRepository(config)}
	return ret
}
