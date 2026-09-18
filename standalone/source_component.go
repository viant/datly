package standalone

import (
	"context"
	"fmt"
	"reflect"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/report"
	rhandler "github.com/viant/datly/runtime/handler"
	writerhandler "github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/typecatalog"
)

// sourceComponent attaches standalone capabilities to canonical project artifacts.
// Report handlers and registration metadata remain owned by report/bootstrap.
type sourceComponent struct{ source *source }

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
