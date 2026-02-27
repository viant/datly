package command

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/shape"
	shapeCompile "github.com/viant/datly/repository/shape/compile"
	"github.com/viant/datly/repository/shape/dql/ir"
	dqlyaml "github.com/viant/datly/repository/shape/dql/render/yaml"
	shapeLoad "github.com/viant/datly/repository/shape/load"
	datlyservice "github.com/viant/datly/service"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"gopkg.in/yaml.v3"
)

func (s *Service) translateShapeIR(ctx context.Context, opts *options.Options) error {
	rule := opts.Rule()
	compiler := shapeCompile.New()
	loader := shapeLoad.New()
	for rule.Index = 0; rule.Index < len(rule.Source); rule.Index++ {
		// Reuse legacy signature bootstrap so shape IR flow gets the same registry/signature context when available.
		if err := s.ensureTranslator(opts); err == nil && s.translator != nil {
			_ = s.translator.InitSignature(ctx, rule)
		}
		sourceURL := rule.SourceURL()
		_, name := url.Split(sourceURL, file.Scheme)
		fmt.Printf("translating %v (shape-ir)\n", name)
		dql, err := rule.LoadSource(ctx, s.fs, sourceURL)
		if err != nil {
			return err
		}
		dql = strings.TrimSpace(dql)
		if dql == "" {
			return fmt.Errorf("source %s was empty", sourceURL)
		}
		shapeSource := &shape.Source{
			Name:      strings.TrimSuffix(name, path.Ext(name)),
			Path:      url.Path(sourceURL),
			DQL:       dql,
			Connector: strings.TrimSpace(rule.Connector),
		}
		planResult, err := compiler.Compile(ctx, shapeSource)
		if err != nil {
			return fmt.Errorf("failed to compile %s: %w", sourceURL, err)
		}
		componentArtifact, err := loader.LoadComponent(ctx, planResult)
		if err != nil {
			return fmt.Errorf("failed to load %s: %w", sourceURL, err)
		}
		component, ok := componentArtifact.Component.(*shapeLoad.Component)
		if !ok || component == nil {
			return fmt.Errorf("unexpected component artifact for %s", sourceURL)
		}

		payload, err := buildShapeRulePayload(opts, dql, componentArtifact.Resource, component)
		if err != nil {
			return err
		}
		routeYAML, err := yaml.Marshal(payload)
		if err != nil {
			return err
		}
		document, err := ir.FromYAML(routeYAML)
		if err != nil {
			return fmt.Errorf("failed to build IR from %s: %w", sourceURL, err)
		}
		encoded, err := dqlyaml.Encode(document)
		if err != nil {
			return fmt.Errorf("failed to encode IR for %s: %w", sourceURL, err)
		}

		routeYAMLPath, _, _, _, err := routePathForShape(rule, opts.Repository().RepositoryURL, sourceURL)
		if err != nil {
			return err
		}
		irPath := strings.TrimSuffix(routeYAMLPath, ".yaml") + ".ir.yaml"
		if err = s.fs.Upload(ctx, irPath, file.DefaultFileOsMode, strings.NewReader(string(encoded))); err != nil {
			return fmt.Errorf("failed to persist route ir %s: %w", irPath, err)
		}
	}
	return nil
}

func buildShapeRulePayload(opts *options.Options, dql string, resource *view.Resource, component *shapeLoad.Component) (*shapeRuleFile, error) {
	rule := opts.Rule()
	rootView := ""
	if component != nil {
		rootView = strings.TrimSpace(component.RootView)
	}
	if rootView == "" && resource != nil && len(resource.Views) > 0 && resource.Views[0] != nil {
		rootView = resource.Views[0].Name
	}
	method, uri := parseShapeRulePath(dql, rule.RuleName(), opts.Repository().APIPrefix)
	route := &repository.Component{
		Path: contract.Path{
			Method: method,
			URI:    uri,
		},
		Contract: contract.Contract{
			Service: serviceTypeForMethod(method),
		},
		View: &view.View{Reference: shared.Reference{Ref: rootView}},
	}
	if component != nil {
		route.TypeContext = component.TypeContext
		if component.Directives != nil && component.Directives.MCP != nil {
			route.Name = strings.TrimSpace(component.Directives.MCP.Name)
			route.Description = strings.TrimSpace(component.Directives.MCP.Description)
			route.DescriptionURI = strings.TrimSpace(component.Directives.MCP.DescriptionPath)
		}
	}
	payload := &shapeRuleFile{
		Resource: resource,
		Routes:   []*repository.Component{route},
	}
	if component != nil && component.TypeContext != nil {
		payload.TypeContext = component.TypeContext
	}
	return payload, nil
}

func serviceTypeForMethod(method string) datlyservice.Type {
	if strings.EqualFold(method, "GET") {
		return datlyservice.TypeReader
	}
	return datlyservice.TypeExecutor
}
