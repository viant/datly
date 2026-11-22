package standalone

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	bootstrapindex "github.com/viant/datly/bootstrap/index"
	"github.com/viant/datly/report"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

type linkedComponent struct {
	component            *spec.Component
	source               *bootstrap.RouteSource
	outputRepresentation *xshape.Representation
}

// bindLinkedOutputRepresentations checks declarations against selected, merged
// linked components before either materialization path can publish them.
func (s *source) bindLinkedOutputRepresentations(components map[string]*linkedComponent) error {
	for target, representation := range s.linkedOutputRepresentations {
		linked := components[target.Component.String()]
		if linked == nil || !linked.matchesRoute(target.Route) {
			return fmt.Errorf("linked output representation target is not selected: %s", target)
		}
		if representation == nil || representation.Original() != linked.source.LinkedOutputType {
			return fmt.Errorf("linked output representation for %s does not match its linked type", target)
		}
		if linked.outputRepresentation != nil {
			return fmt.Errorf("multiple linked output representations for %s", target.Component.String())
		}
		linked.outputRepresentation = representation
	}
	return nil
}

func (c *linkedComponent) matchesRoute(target spec.RouteRef) bool {
	for _, route := range c.component.Routes {
		if route != nil && strings.EqualFold(route.Method, target.Method) && route.Path == target.Path {
			return true
		}
	}
	return false
}

func (c *linkedComponent) applyOutputRepresentation(registration *registry.RegisteredComponent) error {
	if c.outputRepresentation == nil {
		return nil
	}
	output, err := registration.Output.WithRepresentation(c.outputRepresentation)
	if err != nil {
		return fmt.Errorf("linked output representation for %s: %w", registration.Component.Key.String(), err)
	}
	registration.Output = output
	return nil
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
	if err := current.applyOutputRepresentation(primary); err != nil {
		return nil, err
	}
	if m.source.logger != nil {
		m.source.logger.Info("datly bootstrap linked materialize", "component", entry.Key().String(), "related", len(related), "elapsed", time.Since(started).String())
	}
	return &bootstrapindex.Loaded{Registration: primary, Related: related}, nil
}
