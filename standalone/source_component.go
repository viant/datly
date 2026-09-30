package standalone

import (
	"context"
	"fmt"
	"reflect"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/report"
	rhandler "github.com/viant/datly/runtime/handler"
	writerhandler "github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/typecatalog"
)

// sourceComponent attaches standalone capabilities to canonical project artifacts.
// Report handlers and registration metadata remain owned by report/bootstrap.
type sourceComponent struct{ source *source }

func (c *sourceComponent) reflectedArtifactInput(component *spec.Component, source *bootstrap.RouteSource, types *typecatalog.Catalog, resources *resource.Store) (bootstrap.ArtifactInput, error) {
	if component == nil || source == nil || source.LinkedInputType == nil || source.LinkedOutputType == nil {
		return bootstrap.ArtifactInput{}, fmt.Errorf("reflected component contract is incomplete")
	}
	resources, err := linkedDefaultResources(resources, source.LinkedInputType)
	if err != nil {
		return bootstrap.ArtifactInput{}, err
	}
	var handler rhandler.TypedHandler
	if source.LinkedHandler != nil {
		handler, err = source.LinkedHandler()
		if err != nil {
			return bootstrap.ArtifactInput{}, err
		}
	}
	if component.Settings != nil && component.Settings.Mutation != "" {
		handler, err = writerhandler.New(component, source.LinkedInputType, source.LinkedOutputType, component.Settings.Mutation)
		if err != nil {
			return bootstrap.ArtifactInput{}, err
		}
	}
	return bootstrap.ArtifactInput{Const: c.source.config.Const, Component: component, Types: types, InputType: source.LinkedInputType, OutputType: source.LinkedOutputType, Handler: handler, HandlerOwnedOutput: handler != nil, Resources: resources, CodecFactory: c.source.codecs}, nil
}

func linkedDefaultResources(resources *resource.Store, inputType reflect.Type) (*resource.Store, error) {
	embedder := linkedInputEmbedder(inputType)
	if embedder == nil || embedder.EmbedFS() == nil {
		return resources, nil
	}
	if resources == nil {
		resources = resource.New()
	}
	return resources.WithDefault(embedder.EmbedFS())
}

func linkedInputEmbedder(inputType reflect.Type) bootstrap.Embedder {
	for inputType != nil && inputType.Kind() == reflect.Pointer {
		inputType = inputType.Elem()
	}
	if inputType == nil || inputType.Kind() != reflect.Struct {
		return nil
	}
	if value := reflect.New(inputType).Interface(); value != nil {
		if embedder, ok := value.(bootstrap.Embedder); ok {
			return embedder
		}
	}
	if value := reflect.New(inputType).Elem().Interface(); value != nil {
		if embedder, ok := value.(bootstrap.Embedder); ok {
			return embedder
		}
	}
	return nil
}

func (c *sourceComponent) artifactInput(compiled *transcribe.Result) (bootstrap.ArtifactInput, error) {
	if compiled.VeltyHandler != nil || compiled.GoHandler != nil {
		return bootstrap.ArtifactInput{}, fmt.Errorf("component %s requires a generated, linked handler", compiled.Component.Key.String())
	}
	settings := compiled.Component.Settings
	if settings == nil || settings.InputType == "" || settings.OutputType == "" {
		return bootstrap.ArtifactInput{}, fmt.Errorf("component %s requires authored linked input/output contracts", compiled.Component.Key.String())
	}
	var input, output reflect.Type
	var handler rhandler.TypedHandler
	var err error
	if compiled.Source.LinkedInputType != nil && compiled.Source.LinkedOutputType != nil {
		input, output = compiled.Source.LinkedInputType, compiled.Source.LinkedOutputType
		if compiled.Source.LinkedHandler != nil {
			handler, err = compiled.Source.LinkedHandler()
			if err != nil {
				return bootstrap.ArtifactInput{}, err
			}
		}
	} else {
		resolver, err := typecatalog.NewResolver(compiled.Source.Types, typecatalog.PackageAuthority, compiled.TypeContext)
		if err != nil {
			return bootstrap.ArtifactInput{}, err
		}
		input, err = resolver.Type(settings.InputType)
		if err != nil {
			return bootstrap.ArtifactInput{}, err
		}
		output, err = resolver.Type(settings.OutputType)
		if err != nil {
			return bootstrap.ArtifactInput{}, err
		}
	}
	if input == nil || output == nil || input.Kind() != reflect.Struct || output.Kind() != reflect.Struct {
		return bootstrap.ArtifactInput{}, fmt.Errorf("component %s input/output contracts must be linked structs", compiled.Component.Key.String())
	}
	if settings.Mutation != "" {
		handler, err = writerhandler.New(compiled.Component, input, output, settings.Mutation)
		if err != nil {
			return bootstrap.ArtifactInput{}, err
		}
	}
	return bootstrap.ArtifactInput{Const: c.source.config.Const, Component: compiled.Component, Types: compiled.Source.Types, InputType: input, OutputType: output, Handler: handler, HandlerOwnedOutput: handler != nil, Resources: compiled.Source.Resources, CodecFactory: c.source.codecs}, nil
}

func (c *sourceComponent) Configure(ctx context.Context, artifact *report.ComponentArtifact) (report.RuntimeCapabilities, error) {
	result := report.RuntimeCapabilities{}
	// Reader predicates can bind the connector capability too (for example,
	// Studio's owner-scoped ACL predicate). Do not reserve it for custom
	// handlers: both route kinds execute under the same configured SQL host.
	result.Invocation.Connector = c.source.connections.SQL
	views, err := artifact.NewViewProvider(bootstrap.ViewRuntimeConfig{SQL: c.source.connections.SQL})
	if err != nil {
		return result, err
	}
	if views != nil {
		result.Providers = append(result.Providers, views)
	}
	if reader := artifact.ReaderCompilation(); reader != nil {
		result.Reader, err = reader.NewExecution(bootstrap.ReaderRuntimeConfig{CacheIdentity: c.source.connections.CacheIdentity(), SQL: c.source.connections.SQL, Aerospike: &c.source.caches, CacheSettings: c.source.config.Caches})
		if err != nil {
			return result, err
		}
	}
	component := artifact.Component()
	if artifact.HasLinkedHandler() && component.Settings != nil && component.Settings.DefaultConnector != "" {
		db, err := c.source.connections.ResolveDB(ctx, component.Settings.DefaultConnector)
		if err != nil {
			return result, err
		}
		result.DataSource = dml.Source{DB: db}
	}
	return result, nil
}
