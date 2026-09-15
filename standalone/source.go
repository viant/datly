package standalone

import (
	"context"
	"fmt"
	afsurl "github.com/viant/afs/url"
	"path/filepath"
	"sort"

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
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlx/io/read/cache/aerospike"
	"github.com/viant/x"
	xmodule "github.com/viant/x/module"
	xcodec "github.com/viant/xdatly/codec"
)

type source struct {
	Workspace   *xmodule.Workspace
	resources   *resource.Store
	caches      aerospike.Pool
	config      *config.Config
	connections *connector.Set
	codecs      xcodec.Factory
	registry    *x.Registry
	http        gateway.Config
}

func (s *source) compile(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
	if s.config.GoBootstrap != nil && s.config.GoBootstrap.EagerComponents {
		return s.compileEager(ctx, types)
	}
	if s.config.GoBootstrap == nil || len(s.config.GoBootstrap.Packages) == 0 {
		return &application.Build{Resources: s.resources, Types: types, HTTP: s.http, Version: s.config.Version}, nil
	}
	workspace := s.Workspace
	var err error
	if workspace == nil {
		workspace, err = (xmodule.LocalWorkspace{BaseDir: s.config.BaseDir, ModuleDirs: s.config.ModuleDirs}).Resolve(ctx)
		if err != nil {
			return nil, err
		}
	}
	snapshot, err := (bootstrapindex.Builder{Config: bootstrapindex.Config{Workspace: workspace, BaseDir: s.config.BaseDir, ModuleDirs: s.config.ModuleDirs, Include: s.config.GoBootstrap.Packages, Exclude: s.config.GoBootstrap.Exclude, Types: types}}).Build(ctx)
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
	assets, err := (packageresources.Loader{Workspace: workspace, Packages: packages}).Load(ctx)
	if err != nil {
		return nil, err
	}
	seed, err := types.Clone()
	if err != nil {
		return nil, err
	}
	built := &application.Build{Index: snapshot, Materializer: &indexedMaterializer{source: s, workspace: workspace, seed: seed}, Resources: assets.Store, Types: types, HTTP: s.http, Version: s.config.Version}
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
			if entry.Component.CacheWarmup() != nil {
				preload[entry.Key().String()] = entry.Key()
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
	return built, nil
}

func (s *source) compileEager(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
	if s.config.GoBootstrap == nil || len(s.config.GoBootstrap.Packages) == 0 {
		return &application.Build{Resources: s.resources, Types: types, HTTP: s.http, Version: s.config.Version}, nil
	}
	discovery := transcribe.Discovery{Const: s.config.Const, Workspace: s.Workspace, BaseDir: s.config.BaseDir, ModuleDirs: s.config.ModuleDirs, Include: s.config.GoBootstrap.Packages, Exclude: s.config.GoBootstrap.Exclude, Connector: s.config.Connector, Types: types, Registry: s.registry}
	project, err := discovery.Compile(ctx)
	if err != nil {
		return nil, err
	}
	if len(project.Components) == 0 {
		return nil, fmt.Errorf("no authored components found")
	}
	built := &application.Build{Resources: project.Resources, Types: project.Components[0].Source.Types, HTTP: s.http, Version: s.config.Version}
	if s.config.MCP != nil {
		built.MCP = mcp.Config{Authorization: s.config.MCP.Authorization, Folders: s.config.MCP.Folders}
	}
	built.HTTP.StaticContent = append([]*spec.StaticContent(nil), s.http.StaticContent...)
	components := &sourceComponent{source: s}
	var inputs []bootstrap.ArtifactInput
	for _, compiled := range project.Components {
		if compiled.Component.Static != nil {
			content := compiled.Component.Static.Clone()
			content.ContentURL, err = s.config.Const.Path(content.ContentURL)
			if err != nil {
				return nil, err
			}
			if built.HTTP.StaticLocalRoot == nil && built.HTTP.ContentURL == "" && content.ContentURL != "" && afsurl.IsRelative(content.ContentURL) {
				content.ContentURL = compiled.Source.BaseDir() + string(filepath.Separator) + content.ContentURL
			}
			built.HTTP.StaticContent = append(built.HTTP.StaticContent, content)
			continue
		}
		input, err := components.artifactInput(compiled)
		if err != nil {
			return nil, err
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
	return exports.Catalog(nil)
}
