package standalone

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	bootstrapindex "github.com/viant/datly/bootstrap/index"
	"github.com/viant/datly/report"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
)

type linkedComponent struct {
	component   *spec.Component
	source      *bootstrap.RouteSource
	metadata    *linkedMetadata
	metadataErr error
}

// linkedMaterializer uses the same artifact/report/runtime compiler as eager
// reflection, but only for the requested owner in the indexed generation.
type linkedMaterializer struct {
	source     *source
	components map[string]*linkedComponent
	types      *typecatalog.Catalog
	resources  *resource.Store
}

func (m *linkedMaterializer) Materialize(ctx context.Context, entry *bootstrapindex.Entry, resolver bootstrapindex.Resolver) (*bootstrapindex.Loaded, error) {
	started := time.Now()
	if m == nil || m.source == nil || entry == nil || entry.Component == nil {
		return nil, fmt.Errorf("linked standalone component source is required")
	}
	owner := entry.Key()
	if entry.Owner.Name != "" {
		owner = entry.Owner
		loaded, err := resolver.Load(ctx, owner)
		if err != nil {
			return nil, err
		}
		for _, related := range loaded.Related {
			if related != nil && related.Component != nil && related.Component.Key == entry.Key() {
				return &bootstrapindex.Loaded{Registration: related}, nil
			}
		}
		return nil, fmt.Errorf("linked component %s did not produce derived registration %s", owner.String(), entry.Key().String())
	}
	current := m.components[owner.String()]
	if current == nil {
		return nil, fmt.Errorf("linked component not found: %s", owner.String())
	}
	if current.metadataErr != nil {
		return nil, current.metadataErr
	}
	if current.metadata == nil {
		return nil, fmt.Errorf("linked component %s metadata was not prepared before publication", owner.String())
	}
	types, err := current.metadata.types.Clone()
	if err != nil {
		return nil, fmt.Errorf("linked component %s types: %w", owner.String(), err)
	}
	components := &sourceComponent{source: m.source}
	input, err := components.reflectedArtifactInput(current.metadata.component.Clone(), current.source, types, current.metadata.resources)
	if err != nil {
		return nil, fmt.Errorf("linked component %s: %w", owner.String(), err)
	}
	compilation, err := report.NewProjectCompiler(report.ProjectConfig{Registry: m.source.registry, Types: types}).CompileArtifacts([]bootstrap.ArtifactInput{input})
	if err != nil {
		return nil, err
	}
	registrations, err := compilation.RuntimeComponents(ctx, components)
	if err != nil {
		return nil, err
	}
	var primary *registry.RegisteredComponent
	var related []*registry.RegisteredComponent
	for _, registration := range registrations {
		if registration != nil && registration.Component != nil && registration.Component.Key == entry.Key() {
			primary = registration
		} else {
			related = append(related, registration)
		}
	}
	if primary == nil {
		return nil, fmt.Errorf("linked component %s did not produce its primary registration", entry.Key().String())
	}
	if m.source.logger != nil {
		m.source.logger.Info("datly bootstrap linked materialize", "component", entry.Key().String(), "related", len(related), "elapsed", time.Since(started).String())
	}
	return &bootstrapindex.Loaded{Registration: primary, Related: related}, nil
}

// Only physical metadata is prepared before the generation becomes visible.
// Runtime handlers/artifacts remain lazy, generation-owned singleflight loads.
// No caller invocation transaction is borrowed by the shared materializer.
type linkedMetadata struct {
	component *spec.Component
	types     *typecatalog.Catalog
	resources *resource.Store
}

func (m *linkedMaterializer) prepareMetadata(ctx context.Context) error {
	keys := make([]string, 0, len(m.components))
	for key := range m.components {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		current := m.components[key]
		prepared, err := m.prepareComponentMetadata(ctx, current)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A discovery failure is retained for this generation; an explicit reload
		// after schema/connectivity repair retries it. Unrelated routes remain usable.
		if err != nil {
			current.metadataErr = fmt.Errorf("linked component %s columns: %w", key, err)
			continue
		}
		current.metadata = prepared
	}
	return ctx.Err()
}
func (m *linkedMaterializer) prepareComponentMetadata(ctx context.Context, current *linkedComponent) (*linkedMetadata, error) {
	types, err := m.types.Clone()
	if err != nil {
		return nil, err
	}
	components := &sourceComponent{source: m.source}
	resources, err := components.reflectedResources(current.source, m.resources)
	if err != nil {
		return nil, err
	}
	component := current.component.Clone()
	if m.source.connections != nil {
		compiled, err := (&transcribe.PackageCompilation{
			Source:    &transcribe.Source{Scope: current.source.PackagePath, Name: current.component.Name, Const: m.source.config.Const, Connector: m.source.config.Connector, Resources: resources, ColumnRefiner: column.New(m.source.connections)},
			Component: current.source, InputType: current.source.LinkedInputType, OutputType: current.source.LinkedOutputType, Types: types,
		}).Compile(ctx)
		if err != nil {
			return nil, err
		}
		component = compiled.Component.Clone()
		types, err = compiled.Source.Types.Clone()
		if err != nil {
			return nil, err
		}
	}
	return &linkedMetadata{component: component, types: types, resources: resources}, nil
}
