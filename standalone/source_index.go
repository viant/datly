package standalone

import (
	"context"
	"fmt"
	"sort"
	"time"

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
	started := time.Now()
	if m == nil || m.source == nil || entry == nil || entry.Component == nil {
		return nil, fmt.Errorf("indexed standalone component source is required")
	}
	component := entry.Key().String()
	types, err := m.seed.Clone()
	if err != nil {
		m.logMaterialize(component, 0, started, err)
		return nil, err
	}
	owner := entry.Key()
	if entry.Owner.Name != "" {
		owner = entry.Owner
	}
	selection := []string{owner.Scope}
	discovery := transcribe.Discovery{Const: m.source.config.Const, Workspace: m.workspace, Include: selection, TypeInclude: indexedMaterializerTypeSelection(owner.Scope, entry.Sources), Exclude: m.source.config.GoBootstrap.Exclude, Connector: m.source.config.Connector, Types: types, Registry: m.source.registry, Holders: m.source.holders, RequireLinked: m.source.requireLinked}
	project, err := discovery.Compile(ctx)
	if err != nil {
		m.logMaterialize(component, 0, started, err)
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
		err := fmt.Errorf("indexed component source no longer resolves: %s", entry.Key().String())
		m.logMaterialize(component, 0, started, err)
		return nil, err
	}
	components := &sourceComponent{source: m.source}
	input, err := components.artifactInput(selected)
	if err != nil {
		m.logMaterialize(component, 0, started, err)
		return nil, err
	}
	compilation, err := report.NewProjectCompiler(report.ProjectConfig{Registry: m.source.registry, Types: selected.Source.Types}).CompileArtifacts([]bootstrap.ArtifactInput{input})
	if err != nil {
		m.logMaterialize(component, 0, started, err)
		return nil, err
	}
	registrations, err := compilation.RuntimeComponents(ctx, components)
	if err != nil {
		m.logMaterialize(component, 0, started, err)
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
		err := fmt.Errorf("indexed component %s did not produce its primary registration", entry.Key().String())
		m.logMaterialize(component, len(related), started, err)
		return nil, err
	}
	m.logMaterialize(component, len(related), started, nil)
	return &bootstrapindex.Loaded{Registration: primary, Related: related}, nil
}

func (m *indexedMaterializer) logMaterialize(component string, related int, started time.Time, err error) {
	if m == nil || m.source == nil || m.source.logger == nil {
		return
	}
	args := []any{"component", component, "related", related, "elapsed", time.Since(started).String()}
	if err != nil {
		args = append(args, "status", "error", "error", err)
		m.source.logger.Error("datly bootstrap indexed materialize", args...)
		return
	}
	args = append(args, "status", "ok")
	m.source.logger.Info("datly bootstrap indexed materialize", args...)
}

func indexedMaterializerTypeSelection(ownerScope string, sources []bootstrapindex.Source) []string {
	seen := map[string]bool{}
	var result []string
	add := func(packagePath string) {
		if packagePath == "" || packagePath == ownerScope || seen[packagePath] {
			return
		}
		seen[packagePath] = true
		result = append(result, packagePath)
	}
	for _, source := range sources {
		if source.Kind == bootstrapindex.SourceGo {
			add(source.PackagePath)
		}
	}
	sort.Strings(result)
	return result
}
