package gateway

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/gorouter"
	shapeLoad "github.com/viant/datly/repository/shape/load"
	shapePlan "github.com/viant/datly/repository/shape/plan"
	shapeScan "github.com/viant/datly/repository/shape/scan"
	"github.com/viant/datly/view/state"
)

func (r *Service) applyGoBootstrap(ctx context.Context, repo *repository.Service, cfg *GoBootstrap) error {
	if cfg == nil || len(cfg.Packages) == 0 {
		return nil
	}
	baseDir, err := locateGoBootstrapBaseDir(r.Config)
	if err != nil {
		return err
	}
	routes, err := gorouter.Discover(ctx, baseDir, cfg.Packages, cfg.Exclude)
	if err != nil {
		return err
	}
	scanner := shapeScan.New()
	planner := shapePlan.New()
	loader := shapeLoad.New()
	for _, route := range routes {
		if route == nil || route.Source == nil {
			continue
		}
		component, err := compileGoBootstrapComponent(ctx, scanner, planner, loader, repo, route)
		if err != nil {
			return err
		}
		exists, lookupErr := hasRepositoryProvider(ctx, repo, &component.Path)
		if lookupErr != nil {
			return lookupErr
		}
		if exists {
			continue
		}
		repo.Register(component)
	}
	return nil
}

func locateGoBootstrapBaseDir(cfg *Config) (string, error) {
	if cfg == nil {
		return "", fmt.Errorf("go bootstrap config was nil")
	}
	candidates := []string{cfg.DependencyURL, cfg.RouteURL, cfg.ContentURL}
	for _, candidate := range candidates {
		base := normalizeBootstrapPath(candidate)
		if base == "" {
			continue
		}
		if root := walkToGoMod(base); root != "" {
			return root, nil
		}
	}
	if wd, err := os.Getwd(); err == nil {
		if root := walkToGoMod(wd); root != "" {
			return root, nil
		}
	}
	return "", fmt.Errorf("failed to locate Go bootstrap base dir")
}

func normalizeBootstrapPath(candidate string) string {
	candidate = strings.TrimSpace(candidate)
	if candidate == "" {
		return ""
	}
	candidate = strings.TrimPrefix(candidate, "file://localhost")
	candidate = strings.TrimPrefix(candidate, "file://")
	if candidate == "" {
		return ""
	}
	return filepath.Clean(candidate)
}

func walkToGoMod(base string) string {
	base = filepath.Clean(base)
	info, err := os.Stat(base)
	if err != nil {
		return ""
	}
	if !info.IsDir() {
		base = filepath.Dir(base)
	}
	for {
		if _, err := os.Stat(filepath.Join(base, "go.mod")); err == nil {
			return base
		}
		parent := filepath.Dir(base)
		if parent == base {
			return ""
		}
		base = parent
	}
}

func compileGoBootstrapComponent(ctx context.Context, scanner *shapeScan.StructScanner, planner *shapePlan.Planner, loader *shapeLoad.Loader, repo *repository.Service, route *gorouter.RouteSource) (*repository.Component, error) {
	scanResult, err := scanner.Scan(ctx, route.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to scan Go bootstrap route %s: %w", route.Name, err)
	}
	planResult, err := planner.Plan(ctx, scanResult)
	if err != nil {
		return nil, fmt.Errorf("failed to plan Go bootstrap route %s: %w", route.Name, err)
	}
	componentArtifact, err := loader.LoadComponent(ctx, planResult, shape.WithLoadTypeContextPackages(true))
	if err != nil {
		return nil, fmt.Errorf("failed to load Go bootstrap route %s: %w", route.Name, err)
	}
	mergeBootstrapSharedResources(componentArtifact.Resource, repo)
	normalizeBootstrapTemplateParameters(componentArtifact.Resource)
	normalizeBootstrapViewAliases(componentArtifact.Resource)
	normalizeBootstrapCaches(componentArtifact.Resource)
	loaded, ok := componentArtifact.Component.(*shapeLoad.Component)
	if !ok || loaded == nil {
		return nil, fmt.Errorf("unexpected Go bootstrap component artifact for %s", route.Name)
	}
	return materializeBootstrapComponent(ctx, repo, componentArtifact, loaded, route.Name)
}

func materializeBootstrapComponent(ctx context.Context, repo *repository.Service, componentArtifact *shape.ComponentArtifact, loaded *shapeLoad.Component, sourceName string) (*repository.Component, error) {
	bootstrapMetadata := snapshotBootstrapViewMetadata(componentArtifact.Resource)
	rootView := lookupRootView(componentArtifact.Resource, loaded.RootView)
	if rootView == nil && bootstrapRequiresRootView(loaded) {
		return nil, fmt.Errorf("missing root view %q for %s", loaded.RootView, sourceName)
	}
	if rootView == nil {
		rootView = bootstrapHandlerView(componentArtifact.Resource, loaded, sourceName)
	}
	method := strings.TrimSpace(strings.ToUpper(loaded.Method))
	uri := strings.TrimSpace(loaded.URI)
	if method == "" && len(loaded.ComponentRoutes) > 0 && loaded.ComponentRoutes[0] != nil {
		method = strings.TrimSpace(strings.ToUpper(loaded.ComponentRoutes[0].Method))
	}
	if uri == "" && len(loaded.ComponentRoutes) > 0 && loaded.ComponentRoutes[0] != nil {
		uri = strings.TrimSpace(loaded.ComponentRoutes[0].RoutePath)
	}
	if method == "" {
		method = "GET"
	}
	if uri == "" {
		return nil, fmt.Errorf("missing shape component route for %s", sourceName)
	}
	var outputType reflect.Type
	if shouldMaterializeBootstrapOutputType(loaded, rootView) {
		pkgPath := bootstrapTypePackage(loaded)
		lookupType := componentArtifact.Resource.LookupType()
		outputType, err = loaded.OutputReflectType(pkgPath, lookupType)
		if err != nil {
			return nil, fmt.Errorf("failed to materialize bootstrap output type for %s: %w", sourceName, err)
		}
	}
	reportConfig := bootstrapReport(loaded)
	if reportConfig == nil && rootView != nil && rootView.Groupable && strings.EqualFold(method, "GET") {
		reportConfig = (&repository.Report{Enabled: true}).Normalize()
	}
	componentMeta := contract.Meta{}
	if len(loaded.ComponentRoutes) > 0 && loaded.ComponentRoutes[0] != nil {
		componentMeta.Name = strings.TrimSpace(loaded.ComponentRoutes[0].Name)
	}
	componentMCP := contract.ModelContextProtocol{}
	if loaded.Directives != nil && loaded.Directives.MCP != nil {
		componentMeta.Name = strings.TrimSpace(loaded.Directives.MCP.Name)
		componentMeta.Description = strings.TrimSpace(loaded.Directives.MCP.Description)
		componentMeta.DescriptionURI = strings.TrimSpace(loaded.Directives.MCP.DescriptionPath)
		componentMCP.MCPTool = true
	}
	componentModel := &repository.Component{
		Meta:                 componentMeta,
		ModelContextProtocol: componentMCP,
		Path: contract.Path{
			Method: method,
			URI:    uri,
		},
		Contract: contract.Contract{
			Input: contract.Input{
				Type: state.Type{
					Parameters: loaded.InputParameters(),
				},
			},
			Output: contract.Output{
				CaseFormat:  bootstrapOutputCaseFormat(loaded),
				Cardinality: bootstrapOutputCardinality(loaded, rootView),
				Type: state.Type{
					Parameters: loaded.OutputParameters(),
				},
			},
			Service: defaultServiceForMethod(method, rootView),
		},
		View:        rootView,
		Report:      reportConfig,
		TypeContext: loaded.TypeContext,
	}
	if outputType != nil {
		if componentModel.Contract.Output.Type.Schema == nil {
			componentModel.Contract.Output.Type.Schema = state.NewSchema(nil)
		}
		componentModel.Contract.Output.Type.SetType(outputType)
	}
	loadOptions := []repository.Option{}
	if repo != nil {
		loadOptions = append(loadOptions, repository.WithResources(repo.Resources()))
		loadOptions = append(loadOptions, repository.WithExtensions(repo.Extensions()))
	}
	components, err := repository.LoadComponentsFromMap(ctx, map[string]any{
		"Resource":   componentArtifact.Resource,
		"Components": []*repository.Component{componentModel},
	}, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to materialize bootstrap component for %s: %w", sourceName, err)
	}
	mergeBootstrapViewMetadata(components.Resource, bootstrapMetadata)
	if err = components.Init(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize bootstrap component for %s: %w", sourceName, err)
	}
	if len(components.Components) == 0 || components.Components[0] == nil {
		return nil, fmt.Errorf("empty initialized bootstrap component for %s", sourceName)
	}
	mergeBootstrapView(components.Components[0].View, lookupRootView(bootstrapMetadata, loaded.RootView))
	if loaded.Report != nil && loaded.Report.Enabled && components.Components[0].View != nil {
		components.Components[0].View.Groupable = true
	}
	return components.Components[0], nil
}

func bootstrapReport(loaded *shapeLoad.Component) *repository.Report {
	if loaded == nil || loaded.Report == nil || !loaded.Report.Enabled {
		return nil
	}
	return (&repository.Report{
		Enabled:    loaded.Report.Enabled,
		Input:      strings.TrimSpace(loaded.Report.Input),
		Dimensions: strings.TrimSpace(loaded.Report.Dimensions),
		Measures:   strings.TrimSpace(loaded.Report.Measures),
		Filters:    strings.TrimSpace(loaded.Report.Filters),
		OrderBy:    strings.TrimSpace(loaded.Report.OrderBy),
		Limit:      strings.TrimSpace(loaded.Report.Limit),
		Offset:     strings.TrimSpace(loaded.Report.Offset),
	}).Normalize()
}
