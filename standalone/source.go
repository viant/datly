package standalone

import (
	"context"
	"fmt"
	afsurl "github.com/viant/afs/url"
	"path/filepath"
	"reflect"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/bootstrap/connector"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/mcp"
	"github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	viewprovider "github.com/viant/datly/sql/reader/provider"
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
	exports     *bootstrap.ArtifactBuilder
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
	for _, compiled := range project.Components {
		if compiled.Component.Static != nil {
			content := compiled.Component.Static.Clone()
			if built.HTTP.StaticLocalRoot == nil && built.HTTP.ContentURL == "" && content.ContentURL != "" && afsurl.IsRelative(content.ContentURL) {
				content.ContentURL = compiled.Source.BaseDir() + string(filepath.Separator) + content.ContentURL
			}
			built.HTTP.StaticContent = append(built.HTTP.StaticContent, content)
			continue
		}
		if compiled.VeltyHandler != nil || compiled.GoHandler != nil {
			return nil, fmt.Errorf("component %s requires a generated, linked handler", compiled.Component.Key.String())
		}
		settings := compiled.Component.Settings
		if settings == nil || settings.InputType == "" || settings.OutputType == "" {
			return nil, fmt.Errorf("component %s requires authored linked input/output contracts", compiled.Component.Key.String())
		}
		resolver, err := typecatalog.NewResolver(compiled.Source.Types, typecatalog.PackageAuthority, compiled.TypeContext)
		if err != nil {
			return nil, err
		}
		input, err := resolver.Type(settings.InputType)
		if err != nil {
			return nil, err
		}
		output, err := resolver.Type(settings.OutputType)
		if err != nil {
			return nil, err
		}
		if input == nil || output == nil || input.Kind() != reflect.Struct || output.Kind() != reflect.Struct {
			return nil, fmt.Errorf("component %s input/output contracts must be linked structs", compiled.Component.Key.String())
		}
		artifact, err := s.exports.Build(bootstrap.ArtifactInput{Component: compiled.Component, Types: compiled.Source.Types, InputType: input, OutputType: output, Resources: compiled.Source.Resources, CodecFactory: s.codecs})
		if err != nil {
			return nil, err
		}
		entry := registry.RegisteredComponent{}
		if len(artifact.ViewDependencies) != 0 {
			views, err := viewprovider.New(viewprovider.Config{Dependencies: artifact.ViewDependencies, Input: artifact.Input, SQL: s.connections.SQL})
			if err != nil {
				return nil, err
			}
			entry.Providers = append(entry.Providers, views)
		}
		if settings.Report != nil {
			return nil, fmt.Errorf("standalone report derivation is not configured")
		}
		if artifact.ReaderCompilation() != nil {
			entry.Reader, err = artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: s.connections.SQL, Aerospike: &s.caches})
			if err != nil {
				return nil, err
			}
		}
		if artifact.Handler != nil && settings.DefaultConnector != "" {
			db, err := s.connections.ResolveDB(ctx, settings.DefaultConnector)
			if err != nil {
				return nil, err
			}
			entry.DataSource = dml.Source{DB: db}
		}
		registered, err := artifact.Registration(entry)
		if err != nil {
			return nil, err
		}
		built.Components = append(built.Components, registered)
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
	s.exports, err = bootstrap.NewArtifactBuilder(registry)
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
	return s.exports.Catalog(nil)
}
