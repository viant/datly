package transcribe

import (
	"context"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

// generatedCurrentAuthority proves selected declaration ownership, independently
// of a Current name or a generated registration alone. It never changes metadata.
func generatedCurrentAuthority(ctx context.Context, source *Source, component, authored *spec.Component, resolver *typecatalog.Resolver) (map[string]bool, map[string]bool, error) {
	if source.PackageComponent == nil || strings.TrimSpace(source.Text) == "" || component.TypeContext == nil || resolver == nil {
		return nil, nil, nil
	}
	pkg := strings.TrimSpace(component.TypeContext.PackagePath)
	if pkg == "" {
		return nil, nil, nil
	}
	authority := &gen.Plan{Package: pkg, Imports: component.TypeContext.Imports}
	authoredParams := map[string]*spec.Parameter{}
	authoredViewNames := map[string]*spec.Parameter{}
	qualifiedViewNames := map[string]bool{}
	for _, view := range authored.Views {
		if view != nil && strings.Contains(view.TypeName, ".") {
			qualifiedViewNames[strings.ToLower(strings.TrimSpace(view.Name))] = true
		}
	}
	for _, p := range spec.EffectiveParameters(authored.Parameters) {
		if p != nil {
			authoredParams[p.Identity()] = p
			if p.Source.Kind == "view" {
				authoredViewNames[strings.ToLower(strings.TrimSpace(p.Name))] = p
			}
		}
	}
	borrowed := map[string]bool{}
	for _, row := range authoredBorrowedSQLRows(authored) {
		borrowed[row.Package+"."+row.Type] = true
	}
	build := source.GoBuild.WithContext(ctx)
	if build.Dir == "" {
		build.Dir = source.BaseDir()
	}
	declarations := &factoryDeclarations{ctx: ctx, build: build, pkg: pkg}
	own, inherited := map[string]bool{}, map[string]bool{}
	for _, p := range spec.EffectiveParameters(source.PackageComponent.Parameters) {
		if p == nil || p.Source.Kind != "view" {
			continue
		}
		explicit := authoredParams[p.Identity()]
		// Package bootstrap may canonicalize the source view name. A current DQL
		// declaration of the same input field still owns its authored SQL scope.
		if explicit == nil {
			explicit = authoredViewNames[strings.ToLower(strings.TrimSpace(p.Name))]
		}
		if qualifiedViewNames[strings.ToLower(strings.TrimSpace(p.Name))] {
			continue
		}
		if explicit != nil && ((strings.TrimSpace(explicit.TypeExpr) != "" && strings.TrimSpace(explicit.TypeExpr) != "?") || strings.TrimSpace(explicit.OutputTypeExpr) != "" || strings.Contains(reflect.StructTag(explicit.Tag).Get("typeName"), ".")) {
			continue
		}
		canonical, err := authority.CanonicalType(pkg, p.TypeExpr)
		if err != nil {
			continue
		}
		name, err := (xshape.Resolver{}).Named(canonical)
		if err != nil || !strings.HasPrefix(name, pkg+".") || borrowed[name] {
			continue
		}
		resolution, err := resolver.ResolveWithProvenance(name)
		if err != nil || resolution == nil || resolution.Origin != typecatalog.TypeOriginGenerated {
			continue
		}
		if err := declarations.load(); err != nil {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			return nil, nil, nil
		}
		short := strings.TrimPrefix(name, pkg+".")
		records := declarations.types[short]
		if len(records) != 1 || !records[0].generated || !records[0].concreteStruct || !gen.SourceOwnedBy([]byte(records[0].source), component.Name) {
			continue
		}
		descriptor, err := resolver.Descriptor(name)
		if err != nil || !authoredInputDeclaration(records[0], descriptor, pkg, short) {
			continue
		}
		viewName := strings.TrimSpace(p.Source.Name)
		if viewName == "" {
			viewName = p.Name
		}
		view, err := newPackageViewResolver(source.PackageComponent, resolver).independent(viewName)
		if err != nil || view == nil {
			continue
		}
		identity, err := view.Identity()
		if err != nil {
			continue
		}
		own[identity] = true
		if explicit == nil {
			inherited[p.Identity()] = true
		}
	}
	return own, inherited, nil
}

// generatedHelperAuthority proves selected helper source ownership before
// generation refreshes codec-only inherited key projections.
func generatedHelperAuthority(ctx context.Context, source *Source, component, authored *spec.Component, resolver *typecatalog.Resolver) (map[string]string, error) {
	if source.PackageComponent == nil || strings.TrimSpace(source.Text) == "" || component.TypeContext == nil || resolver == nil {
		return nil, nil
	}
	pkg := strings.TrimSpace(component.TypeContext.PackagePath)
	if pkg == "" {
		return nil, nil
	}
	authority := &gen.Plan{Package: pkg, Imports: component.TypeContext.Imports}
	explicit := map[string]*spec.Parameter{}
	for _, p := range spec.EffectiveParameters(authored.Parameters) {
		if p != nil {
			explicit[p.Identity()] = p
		}
	}
	build := source.GoBuild.WithContext(ctx)
	if build.Dir == "" {
		build.Dir = source.BaseDir()
	}
	declarations := &factoryDeclarations{ctx: ctx, build: build, pkg: pkg}
	owned := map[string]string{}
	for _, p := range spec.EffectiveParameters(component.Parameters) {
		if p == nil || p.Source.Kind != "param" || p.Codec == nil || p.Codec.Body != "structql" || reflect.StructTag(p.Tag).Get("typeName") != "" {
			continue
		}
		if a := explicit[p.Identity()]; a != nil && ((strings.TrimSpace(a.TypeExpr) != "" && strings.TrimSpace(a.TypeExpr) != "?") || strings.TrimSpace(a.OutputTypeExpr) != "" || reflect.StructTag(a.Tag).Get("typeName") != "") {
			continue
		}
		expression := p.OutputTypeExpr
		if expression == "" {
			expression = p.TypeExpr
		}
		canonical, err := authority.CanonicalType(pkg, expression)
		if err != nil {
			continue
		}
		name, err := (xshape.Resolver{}).Named(canonical)
		if err != nil || !strings.HasPrefix(name, pkg+".") {
			continue
		}
		if p.TypeExpr != "" && p.OutputTypeExpr != "" {
			inputType, err := authority.CanonicalType(pkg, p.TypeExpr)
			if err != nil || inputType != canonical {
				continue
			}
		}
		borrowed := false
		for _, row := range authoredBorrowedSQLRows(authored) {
			if name == row.Package+"."+row.Type {
				borrowed = true
				break
			}
		}
		if borrowed {
			continue
		}
		resolution, err := resolver.ResolveWithProvenance(name)
		if err != nil || resolution == nil || resolution.Origin != typecatalog.TypeOriginGenerated {
			continue
		}
		if err := declarations.load(); err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, nil
		}
		short := strings.TrimPrefix(name, pkg+".")
		records := declarations.types[short]
		if len(records) != 1 || !records[0].generated || !records[0].concreteStruct || !gen.SourceOwnedBy([]byte(records[0].source), component.Name) {
			continue
		}
		descriptor, err := resolver.Descriptor(name)
		if err != nil || !authoredInputDeclaration(records[0], descriptor, pkg, short) {
			continue
		}
		owned[p.Identity()] = canonical
	}
	return owned, nil
}

// restoreInferredHelpers reuses the already parsed post-derivation projections.
// A selected owned type is inferred only when the generator computes the same
// exact contract; unrelated explicit or differently named shapes remain linked.
func restoreInferredHelpers(component *spec.Component, owned map[string]string, projections gen.Declarations) {
	if component == nil || component.TypeContext == nil || len(owned) == 0 {
		return
	}
	pkg := component.TypeContext.PackagePath
	authority := &gen.Plan{Package: pkg, Imports: component.TypeContext.Imports}
	for _, p := range spec.EffectiveParameters(component.Parameters) {
		if p == nil || owned[p.Identity()] == "" {
			continue
		}
		inferred, ok := gen.InferredHelperType(p, projections)
		if !ok {
			continue
		}
		canonical, err := authority.CanonicalType(pkg, inferred)
		if err != nil || canonical != owned[p.Identity()] {
			continue
		}
		p.TypeExpr, p.OutputTypeExpr = "", ""
	}
}
