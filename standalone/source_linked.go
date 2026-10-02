package standalone

import (
	"context"
	"fmt"
	"time"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	bootstrapindex "github.com/viant/datly/bootstrap/index"
	"github.com/viant/datly/report"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

type linkedComponent struct {
	component *spec.Component
	source    *bootstrap.RouteSource
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
	types, err := m.types.Clone()
	if err != nil {
		return nil, fmt.Errorf("linked component %s types: %w", owner.String(), err)
	}
	components := &sourceComponent{source: m.source}
	input, err := components.reflectedArtifactInput(current.component.Clone(), current.source, types, m.resources)
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
