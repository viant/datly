package command

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"path/filepath"
	"strings"

	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/shape"
	shapeCompile "github.com/viant/datly/repository/shape/compile"
	shapeLoad "github.com/viant/datly/repository/shape/load"
	datlyservice "github.com/viant/datly/service"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"gopkg.in/yaml.v3"
)

func (s *Service) translateShape(ctx context.Context, opts *options.Options) error {
	rule := opts.Rule()
	compiler := shapeCompile.New()
	loader := shapeLoad.New()
	for rule.Index = 0; rule.Index < len(rule.Source); rule.Index++ {
		sourceURL := rule.SourceURL()
		_, name := url.Split(sourceURL, file.Scheme)
		fmt.Printf("translating %v (shape)\n", name)
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
		if err = s.persistShapeRoute(ctx, opts, sourceURL, dql, componentArtifact.Resource, component); err != nil {
			return err
		}
	}
	paths := url.Join(opts.Repository().RepositoryURL, "Datly", "routes", "paths.yaml")
	if ok, _ := s.fs.Exists(ctx, paths); ok {
		_ = s.fs.Delete(ctx, paths)
	}
	return nil
}

type shapeRuleFile struct {
	Resource    *view.Resource          `yaml:"Resource,omitempty"`
	Routes      []*repository.Component `yaml:"Routes,omitempty"`
	TypeContext any                     `yaml:"TypeContext,omitempty"`
}

func (s *Service) persistShapeRoute(ctx context.Context, opts *options.Options, sourceURL, dql string, resource *view.Resource, component *shapeLoad.Component) error {
	rule := opts.Rule()
	routeYAML, routeRoot, relDir, stem, err := routePathForShape(rule, opts.Repository().RepositoryURL, sourceURL)
	if err != nil {
		return err
	}
	if resource != nil {
		for _, item := range resource.Views {
			if item == nil || item.Template == nil {
				continue
			}
			if strings.TrimSpace(item.Template.Source) == "" {
				continue
			}
			sqlRel := strings.TrimSpace(item.Template.SourceURL)
			if sqlRel == "" {
				sqlRel = path.Join(stem, item.Name+".sql")
			}
			sqlDest := path.Join(routeRoot, relDir, filepath.ToSlash(sqlRel))
			if err = s.fs.Upload(ctx, sqlDest, file.DefaultFileOsMode, strings.NewReader(item.Template.Source)); err != nil {
				return fmt.Errorf("failed to persist sql %s: %w", sqlDest, err)
			}
			item.Template.SourceURL = sqlRel
		}
	}
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
			Service: serviceForMethod(method),
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
	data, err := yaml.Marshal(payload)
	if err != nil {
		return err
	}
	if err = s.fs.Upload(ctx, routeYAML, file.DefaultFileOsMode, strings.NewReader(string(data))); err != nil {
		return fmt.Errorf("failed to persist route yaml %s: %w", routeYAML, err)
	}
	return nil
}

func routePathForShape(rule *options.Rule, repoURL, sourceURL string) (routeYAML string, routeRoot string, relDir string, stem string, err error) {
	sourcePath := filepath.Clean(url.Path(sourceURL))
	basePath := filepath.Clean(rule.BaseRuleURL())
	relative, relErr := filepath.Rel(basePath, sourcePath)
	if relErr != nil || strings.HasPrefix(relative, "..") {
		relative = filepath.Base(sourcePath)
	}
	relative = filepath.ToSlash(relative)
	relDir = filepath.ToSlash(path.Dir(relative))
	if relDir == "." {
		relDir = ""
	}
	stem = strings.TrimSuffix(path.Base(relative), path.Ext(relative))
	routeRoot = url.Join(repoURL, "Datly", "routes")
	routeYAML = url.Join(routeRoot, relDir, stem+".yaml")
	return routeYAML, routeRoot, relDir, stem, nil
}

type shapeRuleHeader struct {
	Method string `json:"Method"`
	URI    string `json:"URI"`
}

func parseShapeRulePath(dql, ruleName, apiPrefix string) (string, string) {
	method := "GET"
	uri := "/" + strings.Trim(strings.TrimSpace(ruleName), "/")
	if prefix := strings.TrimSpace(apiPrefix); prefix != "" {
		uri = strings.TrimRight(prefix, "/") + uri
	}
	start := strings.Index(dql, "/*")
	end := strings.Index(dql, "*/")
	if start != -1 && end > start+2 {
		raw := strings.TrimSpace(dql[start+2 : end])
		if strings.HasPrefix(raw, "{") && strings.HasSuffix(raw, "}") {
			header := &shapeRuleHeader{}
			if err := json.Unmarshal([]byte(raw), header); err == nil {
				if candidate := strings.TrimSpace(strings.ToUpper(header.Method)); candidate != "" {
					method = candidate
				}
				if candidate := strings.TrimSpace(header.URI); candidate != "" {
					uri = candidate
				}
			}
		}
	}
	return method, uri
}

func serviceForMethod(method string) datlyservice.Type {
	if strings.EqualFold(method, "GET") {
		return datlyservice.TypeReader
	}
	return datlyservice.TypeExecutor
}
