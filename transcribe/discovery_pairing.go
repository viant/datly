package transcribe

import (
	"context"
	"fmt"
	"strings"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

// declaredPackageSource joins explicit DQL authority to an existing holder even
// when the authored source keeps a different directory and filename. Loading
// this authority must not select its other components for independent emission.
func (c *discoveryCompilation) declaredPackageSource(ctx context.Context, source *Source) (*bootstrap.PackageComponentSource, error) {
	preparation, err := prepareSourceCompilation(ctx, source)
	if err != nil {
		return nil, err
	}
	prepared := preparation.prepared
	if prepared.TypeContext == nil || prepared.TypeContext.PackagePath == "" {
		return nil, nil
	}
	settings := prepared.Directives.Settings
	if settings == nil || settings.InputType == "" || settings.OutputType == "" || prepared.Directives.Route == nil {
		return nil, nil
	}
	pkg := prepared.TypeContext.PackagePath
	candidates, err := c.declaredPackageCandidates(ctx, pkg)
	if err != nil || len(candidates) == 0 {
		return nil, err
	}
	typeContext := &typecatalog.ResolutionContext{DefaultPackage: pkg, PackagePath: pkg, PackageDir: source.BaseDir()}
	for _, imported := range prepared.TypeContext.Imports {
		typeContext.Imports = append(typeContext.Imports, typecatalog.PackageImport{Alias: imported.Alias, Package: imported.Package})
	}
	resolver, err := typecatalog.NewResolver(c.catalog, typecatalog.PackageAuthority, typeContext)
	if err != nil {
		return nil, err
	}
	input, err := resolver.ResolveShape(settings.InputType)
	if err != nil {
		return nil, err
	}
	output, err := resolver.ResolveShape(settings.OutputType)
	if err != nil {
		return nil, err
	}
	authoredRoutes := prepared.Directives.ComponentRoutes(source.Name, "")
	var matched *bootstrap.PackageComponentSource
	conflict := false
	for _, candidate := range candidates {
		component, err := resolvePackageDeclarations(candidate, resolver)
		if err != nil {
			return nil, err
		}
		contractsMatch := input != nil && output != nil && input.Descriptor != nil && output.Descriptor != nil && input.Identity == candidate.InputType.Key() && output.Identity == candidate.OutputType.Key()
		allRoutes, anyRoute := declaredRoutesMatch(authoredRoutes, component.Routes)
		if !contractsMatch || !allRoutes {
			conflict = conflict || anyRoute || contractsMatch
			continue
		}
		if matched != nil {
			return nil, fmt.Errorf("declared package %q has ambiguous component authority for the explicit routes and contracts", pkg)
		}
		matched = candidate
	}
	if matched == nil {
		if conflict {
			return nil, fmt.Errorf("declared package %q has conflicting component route or input/output contract authority", pkg)
		}
		return nil, nil
	}
	source.Scope, source.Name = pkg, matched.ComponentName()
	return matched, nil
}

func (c *discoveryCompilation) declaredPackageCandidates(ctx context.Context, pkg string) (map[string]*bootstrap.PackageComponentSource, error) {
	if candidates, ok := c.pairingPackages[pkg]; ok {
		return candidates, nil
	}
	selected := *c.discovery
	selected.Include = []string{pkg}
	selected.SourceFile = ""
	candidates, _, err := selected.packageSources(ctx, c.catalog, c.workspace)
	if err != nil {
		return nil, err
	}
	if c.pairingPackages == nil {
		c.pairingPackages = map[string]map[string]*bootstrap.PackageComponentSource{}
	}
	c.pairingPackages[pkg] = candidates
	return candidates, nil
}

func declaredRoutesMatch(authored, existing []*spec.Route) (all, any bool) {
	all = len(authored) != 0
	for _, route := range authored {
		found := false
		for _, candidate := range existing {
			if route != nil && candidate != nil && route.Path == candidate.Path && strings.EqualFold(route.Method, candidate.Method) {
				found = true
				break
			}
		}
		all, any = all && found, any || found
	}
	return all, any
}

// resolvePackageDeclarations reads current holder contracts without admitting a
// previously generated executable graph. Copies keep the original metadata
// locator available to package-only compilation and generation replacement.
func resolvePackageDeclarations(source *bootstrap.PackageComponentSource, resolver *typecatalog.Resolver) (*spec.Component, error) {
	copy := *source
	copy.Routes = make([]*bootstrap.RouteSource, len(source.Routes))
	for i, route := range source.Routes {
		detached := *route
		detached.Tag.ReaderMetadata = ""
		copy.Routes[i] = &detached
	}
	return copy.ResolveDescriptors(resolver)
}
