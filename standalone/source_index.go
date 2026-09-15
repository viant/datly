package standalone

import (
	"context"
	"fmt"

	"github.com/viant/datly/bootstrap"
	bootstrapindex "github.com/viant/datly/bootstrap/index"
	"github.com/viant/datly/report"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/typecatalog"
	xmodule "github.com/viant/x/module"
)

type indexedMaterializer struct {
	source    *source
	workspace *xmodule.Workspace
	seed      *typecatalog.Catalog
}

func (m *indexedMaterializer) Materialize(ctx context.Context, entry *bootstrapindex.Entry, _ bootstrapindex.Resolver) (*bootstrapindex.Loaded, error) {
	if m == nil || m.source == nil || entry == nil || entry.Component == nil {
		return nil, fmt.Errorf("indexed standalone component source is required")
	}
	types, err := m.seed.Clone()
	if err != nil {
		return nil, err
	}
	owner := entry.Key()
	if entry.Owner.Name != "" {
		owner = entry.Owner
	}
	selection := []string{owner.Scope}
	discovery := transcribe.Discovery{Const: m.source.config.Const, Workspace: m.workspace, Include: selection, Exclude: m.source.config.GoBootstrap.Exclude, Connector: m.source.config.Connector, Types: types, Registry: m.source.registry}
	project, err := discovery.Compile(ctx)
	if err != nil {
		return nil, err
	}
	var selected *transcribe.Result
	for _, candidate := range project.Components {
		if candidate != nil && candidate.Component != nil && candidate.Component.Key == owner {
			selected = candidate
			break
		}
	}
	if selected == nil {
		return nil, fmt.Errorf("indexed component source no longer resolves: %s", entry.Key().String())
	}
	components := &sourceComponent{source: m.source}
	input, err := components.artifactInput(selected)
	if err != nil {
		return nil, err
	}
	compilation, err := report.NewProjectCompiler(report.ProjectConfig{Registry: m.source.registry, Types: selected.Source.Types}).CompileArtifacts([]bootstrap.ArtifactInput{input})
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
		return nil, fmt.Errorf("indexed component %s did not produce its primary registration", entry.Key().String())
	}
	return &bootstrapindex.Loaded{Registration: primary, Related: related}, nil
}
