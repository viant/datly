package standalone

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"time"

	afsurl "github.com/viant/afs/url"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/bootstrap/connector"
	bootstrapindex "github.com/viant/datly/bootstrap/index"
	packageresources "github.com/viant/datly/bootstrap/resources"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/mcp"
	"github.com/viant/datly/report"
	"github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlx/io/read/cache/aerospike"
	"github.com/viant/x"
	xmodule "github.com/viant/x/module"
	xcodec "github.com/viant/xdatly/codec"
)

type source struct {
	Workspace      *xmodule.Workspace
	resources      *resource.Store
	caches         aerospike.Pool
	config         *config.Config
	connections    *connector.Set
	codecs         xcodec.Factory
	codecFactories map[string]xcodec.Factory
	registry       *x.Registry
	http           gateway.Config
	holders        []any
	requireLinked  bool
	logger         *slog.Logger
}

func (s *source) compile(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
	if s.config.GoBootstrap != nil && s.config.GoBootstrap.EagerComponents {
		return s.compileEager(ctx, types)
	}
	if s.config.GoBootstrap == nil || len(s.config.GoBootstrap.Packages) == 0 {
		return &application.Build{Resources: s.resources, Types: types, HTTP: s.http, Version: s.config.Version}, nil
	}
	started := time.Now()
	workspace := s.Workspace
	var err error
	if workspace == nil {
		workspace, err = (xmodule.LocalWorkspace{BaseDir: s.config.BaseDir, ModuleDirs: s.config.ModuleDirs}).Resolve(ctx)
		if err != nil {
			return nil, err
		}
	}
	snapshot, err := (bootstrapindex.Builder{Config: bootstrapindex.Config{Workspace: workspace, BaseDir: s.config.BaseDir, ModuleDirs: s.config.ModuleDirs, Include: s.config.GoBootstrap.Packages, Exclude: s.config.GoBootstrap.Exclude, Types: types, Holders: s.holders, RequireLinked: s.requireLinked}}).Build(ctx)
	if err != nil {
		return nil, err
	}
	entries := snapshot.Entries()
	if len(entries) == 0 {
		return nil, fmt.Errorf("no authored components found")
	}
	packages := make([]string, 0, len(entries))
	seen := map[string]bool{}
	for _, entry := range entries {
		if scope := entry.Key().Scope; !seen[scope] {
			seen[scope] = true
			packages = append(packages, scope)
		}
	}
	assets, err := (packageresources.Loader{Workspace: workspace, Packages: packages, Holders: s.holders}).Load(ctx)
	if err != nil {
		return nil, err
	}
	seed, err := types.Clone()
	if err != nil {
		return nil, err
	}
	built := &application.Build{Index: snapshot, Materializer: &indexedMaterializer{source: s, workspace: workspace, seed: seed}, Resources: assets.Store, Types: types, HTTP: s.http, Version: s.config.Version}
	if s.logger != nil {
		built.BootstrapLogger = s.logger
	}
	built.HTTP.StaticContent = append([]*spec.StaticContent(nil), s.http.StaticContent...)
	for _, entry := range entries {
		if entry.Component.Static == nil {
			continue
		}
		content := entry.Component.Static.Clone()
		content.ContentURL, err = s.config.Const.Path(content.ContentURL)
		if err != nil {
			return nil, err
		}
		if built.HTTP.StaticLocalRoot == nil && built.HTTP.ContentURL == "" && content.ContentURL != "" && afsurl.IsRelative(content.ContentURL) {
			content.ContentURL = entry.Sources[0].Dir + string(filepath.Separator) + content.ContentURL
		}
		built.HTTP.StaticContent = append(built.HTTP.StaticContent, content)
	}
	imported := map[string]bool{}
	for _, content := range built.HTTP.StaticContent {
		if content == nil || content.Namespace == "" || s.resources == nil || imported[content.Namespace] {
			continue
		}
		linked, ok := s.resources.Lookup(content.Namespace)
		if !ok {
			continue
		}
		if _, exists := built.Resources.Lookup(content.Namespace); exists {
			return nil, fmt.Errorf("static namespace %q conflicts with discovered package resources", content.Namespace)
		}
		imported[content.Namespace] = true
		if err := built.Resources.Register(content.Namespace, linked); err != nil {
			return nil, err
		}
	}
	preload := map[string]spec.Key{}
	if s.config.Warmup != nil {
		for _, entry := range entries {
			if entry.Warmup || hasWarmupConfiguration(entry.Component) || hasDelegatedWarmupTarget(entry.Component) {
				preload[entry.Key().String()] = entry.Key()
			}
			if target := delegatedWarmupTarget(snapshot, entry.Component); target.Kind != "" {
				preload[target.String()] = target
			}
		}
	}
	for _, configured := range built.HTTP.Async {
		for _, target := range []spec.RouteRef{configured.Route} {
			if indexed, _, _, ok := snapshot.Route(target.Method, target.Path); ok {
				preload[indexed.Key().String()] = indexed.Key()
			}
		}
		if configured.Inspect != nil {
			target := configured.Inspect.Target
			if indexed, _, _, ok := snapshot.Route(target.Method, target.Path); ok {
				preload[indexed.Key().String()] = indexed.Key()
			}
		}
	}
	for _, key := range preload {
		built.Preload = append(built.Preload, key)
	}
	sort.Slice(built.Preload, func(i, j int) bool { return built.Preload[i].String() < built.Preload[j].String() })
	if s.config.MCP != nil {
		built.MCP = mcp.Config{Authorization: s.config.MCP.Authorization, Folders: s.config.MCP.Folders}
	}
	if s.logger != nil {
		components, routes, mcpTools := indexedBootstrapCounts(snapshot)
		s.logger.Info("datly bootstrap indexed done", "components", components, "routes", routes, "mcp", mcpTools, "preload", len(built.Preload), "elapsed", time.Since(started).String())
	}
	return built, nil
}

func indexedBootstrapCounts(snapshot *bootstrapindex.Snapshot) (components, routes, mcpTools int) {
	if snapshot == nil {
		return 0, 0, 0
	}
	entries := snapshot.Entries()
	components = len(entries)
	for _, entry := range entries {
		if entry == nil || entry.Component == nil {
			continue
		}
		routes += len(entry.Component.Routes)
		for _, endpoint := range entry.Component.Routes {
			if endpoint == nil {
				continue
			}
			mcpTools += len(endpoint.MCP)
		}
	}
	return components, routes, mcpTools
}

func hasWarmupConfiguration(component *spec.Component) bool {
	if component == nil {
		return false
	}
	if component.CacheWarmup() != nil {
		return true
	}
	return hasViewWarmupBinding(component.RootView, map[*spec.View]bool{})
}

func hasDelegatedWarmupTarget(component *spec.Component) bool {
	return component != nil && component.Settings != nil && component.Settings.WarmupTarget != nil && component.Settings.WarmupTarget.String() != ""
}

func delegatedWarmupTarget(snapshot *bootstrapindex.Snapshot, component *spec.Component) spec.Key {
	if !hasDelegatedWarmupTarget(component) || snapshot == nil {
		return spec.Key{}
	}
	ref := component.Settings.WarmupTarget
	entry, _, _, ok := snapshot.Route(ref.Method, ref.Path)
	if !ok || entry == nil {
		return spec.Key{}
	}
	return entry.Key()
}

func hasViewWarmupBinding(view *spec.View, visited map[*spec.View]bool) bool {
	if view == nil || visited[view] {
		return false
	}
	visited[view] = true
	if view.Source != nil && view.Source.Bindings != nil && strings.TrimSpace(view.Source.Bindings.CacheWarmup) != "" {
		return true
	}
	for _, relation := range view.Relations {
		if relation != nil && hasViewWarmupBinding(relation.View, visited) {
			return true
		}
	}
	return false
}

func (s *source) compileEager(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
	if s.config.GoBootstrap == nil || len(s.config.GoBootstrap.Packages) == 0 {
		return &application.Build{Resources: s.resources, Types: types, HTTP: s.http, Version: s.config.Version}, nil
	}
	discovered, err := bootstrap.ReflectPackages(s.config.GoBootstrap.Packages)
	if err != nil {
		return nil, err
	}
	if len(discovered.Components) == 0 {
		return nil, fmt.Errorf("no linked reflected components found")
	}
	resources := resource.New()
	for _, packagePath := range discovered.Packages {
		for namespace, embedded := range bootstrap.LinkedResources(s.holders, packagePath) {
			if _, exists := resources.Lookup(namespace); exists {
				return nil, fmt.Errorf("duplicate linked resource namespace %q", namespace)
			}
			if err = resources.Register(namespace, embedded); err != nil {
				return nil, err
			}
		}
	}
	built := &application.Build{Resources: resources, Types: discovered.Types, HTTP: s.http, Version: s.config.Version}
	if s.config.MCP != nil {
		built.MCP = mcp.Config{Authorization: s.config.MCP.Authorization, Folders: s.config.MCP.Folders}
	}
	built.HTTP.StaticContent = append([]*spec.StaticContent(nil), s.http.StaticContent...)
	components := &sourceComponent{source: s}
	type reflectedComponent struct {
		component *spec.Component
		source    *bootstrap.RouteSource
	}
	byKey := map[string]*reflectedComponent{}
	var order []string
	for _, source := range discovered.Components {
		component, resolveErr := source.Resolve(source.LinkedInputType, source.LinkedOutputType)
		if resolveErr != nil {
			return nil, resolveErr
		}
		key := component.Key.String()
		if current := byKey[key]; current != nil {
			for _, candidate := range component.Routes {
				duplicate := false
				for _, existing := range current.component.Routes {
					if existing != nil && candidate != nil && strings.EqualFold(existing.Method, candidate.Method) && existing.Path == candidate.Path {
						duplicate = true
						break
					}
				}
				if !duplicate {
					current.component.Routes = append(current.component.Routes, candidate)
				}
			}
			continue
		}
		byKey[key] = &reflectedComponent{component: component, source: source}
		order = append(order, key)
	}
	sort.Strings(order)
	inputs := make([]bootstrap.ArtifactInput, 0, len(order))
	for _, key := range order {
		current := byKey[key]
		input, inputErr := components.reflectedArtifactInput(current.component, current.source, discovered.Types, resources)
		if inputErr != nil {
			return nil, inputErr
		}
		inputs = append(inputs, input)
	}
	compilation, err := report.NewProjectCompiler(report.ProjectConfig{Registry: s.registry, Types: built.Types}).CompileArtifacts(inputs)
	if err != nil {
		return nil, err
	}
	built.Components, err = compilation.RuntimeComponents(ctx, components)
	if err != nil {
		return nil, err
	}
	built.Types, err = compilation.Types()
	if err != nil {
		return nil, err
	}
	return built, nil
}

func (s *source) init(ctx context.Context, registry *x.Registry) (*typecatalog.Catalog, error) {
	if registry == nil {
		registry = x.NewRegistry()
	}
	var err error
	s.registry, err = (x.Cloner{}).Registry(registry)
	if err != nil {
		return nil, err
	}
	exports, err := bootstrap.NewArtifactBuilder(registry)
	if err != nil {
		return nil, err
	}
	if s.config.JWTValidator != nil {
		s.codecs, err = auth.New(ctx, &auth.Config{JWTValidator: s.config.JWTValidator})
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("JWTValidator initialization failed")
		}
	}
	if len(s.codecFactories) > 0 {
		s.codecs = &applicationCodecs{factories: s.codecFactories, fallback: s.codecs}
	}
	return exports.Catalog(nil)
}
