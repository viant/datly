package transcribe

import (
	"context"
	"fmt"

	readerpredicate "github.com/viant/datly/runtime/predicate/velty"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/typecatalog"
	xmodule "github.com/viant/x/module"
	xshape "github.com/viant/x/shape"
)

func collectPredicatePackages(packages map[string]bool, prepared *dql.PreparedSource, scope string) error {
	if prepared.Err() != nil || prepared.Directives == nil {
		// Let the component compiler report authored diagnostics with source spans.
		return nil
	}
	typeContext := compileTypeContext(&Source{Scope: scope}, prepared.TypeContext)
	names, err := readerpredicate.References(&spec.Component{Parameters: prepared.Directives.Params}, typeContext)
	if err != nil {
		return err
	}
	return addPredicatePackages(packages, names)
}

func addPredicatePackages(packages map[string]bool, names []string) error {
	for _, name := range names {
		path, _, err := (xshape.Resolver{}).CanonicalReference(name)
		if err != nil {
			return fmt.Errorf("predicate type %s: %w", name, err)
		}
		if path != "" {
			packages[path] = true
		}
	}
	return nil
}

func (d *Discovery) loadComponentPredicateDependencies(ctx context.Context, workspace *xmodule.Workspace, catalog *typecatalog.Catalog, component *spec.Component, scope *typecatalog.ResolutionContext) error {
	names, err := readerpredicate.References(component, scope)
	if err != nil {
		return err
	}
	packages := map[string]bool{}
	if err := addPredicatePackages(packages, names); err != nil {
		return err
	}
	loader := &dqlPackageDiscovery{workspace: workspace, catalog: catalog, registry: d.Registry}
	_, err = loader.loadImports(ctx, packages)
	return err
}
