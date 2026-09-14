package standalone

import (
	"context"
	"fmt"
	afsurl "github.com/viant/afs/url"
	"path/filepath"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/bootstrap/connector"
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
	if s.config.GoBootstrap == nil || len(s.config.GoBootstrap.Packages) == 0 {
		return &application.Build{Resources: s.resources, Types: types, HTTP: s.http, Version: s.config.Version}, nil
	}
	discovery := transcribe.Discovery{Workspace: s.Workspace, BaseDir: s.config.BaseDir, ModuleDirs: s.config.ModuleDirs, Include: s.config.GoBootstrap.Packages, Exclude: s.config.GoBootstrap.Exclude, Connector: s.config.Connector, Types: types, Registry: s.registry}
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
