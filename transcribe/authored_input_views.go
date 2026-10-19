package transcribe

import (
	"context"
	"fmt"
	"go/ast"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/gobuild"
	"github.com/viant/datly/transcribe/internal/goflags"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	smodel "github.com/viant/x/syntetic/model"
)

// authoredInputProof is compile-only source authority. It does not classify a
// handler/factory or grant runtime capabilities, and is never published.
type authoredInputProof struct {
	declarations *factoryDeclarations
	build        *gobuild.Context
}

// authoredInputViewReferences selects only ordinary local independent reads.
// Package overlays admit only explicit DQL rows through the same source proof;
// inherited views and external/factory linking retain their existing policies.
func authoredInputViewReferences(ctx context.Context, result *Result, authored ...*spec.Component) (gen.ViewReferences, *authoredInputProof, error) {
	if result == nil || result.Source == nil || result.Source.GoBuild == nil || result.Component == nil || result.Component.TypeContext == nil || result.TypeResolver == nil {
		return nil, nil, nil
	}
	pkg := strings.TrimSpace(result.Component.TypeContext.PackagePath)
	if pkg == "" {
		return nil, nil, nil
	}
	build := result.Source.GoBuild.Clone()
	authority := &gen.Plan{Package: pkg, Imports: result.Component.TypeContext.Imports}
	excluded, err := authoredInputRootNames(result.Component, authority)
	if err != nil {
		return nil, nil, err
	}
	declarations := &factoryDeclarations{ctx: ctx, build: build, pkg: pkg}
	var explicit map[string]string
	if result.Source.PackageComponent != nil {
		if len(authored) == 0 || authored[0] == nil {
			return nil, nil, nil
		}
		explicit = map[string]string{}
		for _, parameter := range spec.EffectiveParameters(authored[0].Parameters) {
			if parameter == nil || !strings.EqualFold(parameter.Source.Kind, "view") {
				continue
			}
			canonical, err := authority.CanonicalType(pkg, parameter.TypeExpr)
			if err == nil {
				explicit[parameter.Identity()] = canonical
			}
		}
	}
	// Filter before the shared selector, whose existing foreign branch is wider.
	component := *result.Component
	component.Parameters = nil
	admitted := map[string]bool{}
	flagsChecked := false
	for _, parameter := range spec.EffectiveParameters(result.Component.Parameters) {
		if parameter == nil || !strings.EqualFold(strings.TrimSpace(parameter.Source.Kind), "view") || strings.TrimSpace(parameter.TypeExpr) == "" || strings.TrimSpace(parameter.TypeExpr) == "?" {
			continue
		}
		canonical, err := authority.CanonicalType(pkg, parameter.TypeExpr)
		if err != nil {
			continue
		}
		if explicit != nil && explicit[parameter.Identity()] != canonical {
			continue
		}
		base, err := (xshape.Resolver{}).Named(canonical)
		if err != nil || !strings.HasPrefix(base, pkg+".") || excluded[base] != "" {
			continue
		}
		name := strings.TrimPrefix(base, pkg+".")
		if !ast.IsExported(name) {
			continue
		}
		resolved, err := result.TypeResolver.ResolveWithProvenance(base)
		if err != nil || resolved == nil || resolved.Origin != typecatalog.TypeOriginPackage {
			continue
		}
		if !flagsChecked {
			selectedBuild, overlay, err := authoredInputBuild(ctx, build)
			if err != nil {
				if ctx.Err() != nil {
					return nil, nil, ctx.Err()
				}
				return nil, nil, nil
			}
			if overlay {
				return nil, nil, nil
			}
			build = selectedBuild
			declarations.build = build
			flagsChecked = true
		}
		if err := declarations.load(); err != nil {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			return nil, nil, nil
		}
		records := declarations.types[name]
		if len(records) != 1 || records[0].generated || records[0].typeSpec == nil {
			continue
		}
		descriptor, err := result.TypeResolver.Descriptor(base)
		if err != nil || !authoredInputReadDeclaration(declarations, result.TypeResolver, descriptor, pkg, excluded) || !authoredInputClosure(declarations, result.TypeResolver, pkg, descriptor) {
			continue
		}
		admitted[name] = true
		component.Parameters = append(component.Parameters, parameter)
	}
	if len(component.Parameters) == 0 {
		return nil, nil, nil
	}
	proof := &authoredInputProof{declarations: declarations, build: build}
	if err := proof.validate(ctx); err != nil {
		return nil, nil, err
	}
	rows, err := newPackageViewResolver(&component, result.TypeResolver).typedInputDescriptors(pkg, result.ViewBindings, func(name string) (bool, error) { return admitted[name], nil }, excluded)
	if err != nil {
		return nil, nil, err
	}
	refs := gen.ViewReferences{}
	for _, row := range rows {
		refs[row.identity] = &gen.ViewReference{DescriptorKey: row.descriptor.Key()}
	}
	return refs, proof, nil
}

// The existing SQL root graph owns its rows. Independent reads cannot borrow a
// body or root-derived type merely because an authored declaration has its name.
func authoredInputRootNames(component *spec.Component, authority *gen.Plan) (map[string]string, error) {
	result := map[string]string{}
	var add func(string) error
	add = func(expression string) error {
		if strings.TrimSpace(expression) == "" || strings.TrimSpace(expression) == "?" {
			return nil
		}
		canonical, err := authority.CanonicalType(authority.Package, expression)
		if err != nil {
			return err
		}
		name, err := (xshape.Resolver{}).Named(canonical)
		if err != nil {
			return nil
		}
		result[name] = name
		return nil
	}
	seen := map[*spec.View]bool{}
	var visit func(*spec.View) error
	visit = func(view *spec.View) error {
		if view == nil || seen[view] {
			return nil
		}
		seen[view] = true
		if err := add(view.TypeName); err != nil {
			return err
		}
		for _, relation := range view.Relations {
			if relation != nil {
				if err := visit(relation.View); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := visit(component.RootView); err != nil {
		return nil, err
	}
	for _, parameter := range spec.EffectiveParameters(component.Parameters) {
		if parameter != nil && strings.EqualFold(strings.TrimSpace(parameter.Source.Kind), "body") {
			if err := add(parameter.TypeExpr); err != nil {
				return nil, err
			}
			if err := add(parameter.OutputTypeExpr); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func authoredInputDeclaration(record factoryDeclaration, descriptor *x.Type, pkg, name string) bool {
	if record.typeSpec == nil || record.typeSpec.Assign.IsValid() {
		return false
	}
	if _, ok := record.typeSpec.Type.(*ast.StructType); !ok {
		return false
	}
	return authoredInputDeclarationMatch(record, descriptor, pkg, name)
}

func authoredInputReadDeclaration(declarations *factoryDeclarations, resolver *typecatalog.Resolver, descriptor *x.Type, pkg string, excluded map[string]string) bool {
	seen := map[string]bool{}
	for descriptor != nil && descriptor.PkgPath == pkg && !seen[descriptor.Key()] {
		if excluded[descriptor.Key()] != "" {
			return false
		}
		seen[descriptor.Key()] = true
		records := declarations.types[descriptor.Name]
		if len(records) != 1 || records[0].generated || !authoredInputDeclarationMatch(records[0], descriptor, pkg, descriptor.Name) {
			return false
		}
		selected := records[0].typeSpec
		if !selected.Assign.IsValid() {
			_, ok := selected.Type.(*ast.StructType)
			return ok
		}
		// Preserve the public alias identity while proving its authored local
		// concrete row. The terminal may be private, as in type Row = row.
		target, ok := selected.Type.(*ast.Ident)
		if !ok {
			return false
		}
		var err error
		descriptor, err = resolver.Descriptor(pkg + "." + target.Name)
		if err != nil {
			return false
		}
	}
	return false
}

// Helper aliases and named scalars retain their exact authored declarations;
// an independently borrowed root must resolve to an authored concrete struct.
func authoredInputDeclarationMatch(record factoryDeclaration, descriptor *x.Type, pkg, name string) bool {
	if descriptor == nil || descriptor.Key() != pkg+"."+name || descriptor.Name != name || descriptor.PkgPath != pkg || descriptor.SynteticType == nil {
		return false
	}
	selected, catalog := record.typeSpec, descriptor.SynteticType
	if catalog.Name != name || catalog.PkgPath != pkg {
		return false
	}
	if selected == nil || selected.TypeParams != nil || catalog.TypeSpec == nil || catalog.TypeSpec.TypeParams != nil || selected.Assign.IsValid() != catalog.TypeSpec.Assign.IsValid() {
		return false
	}
	if selected.Name == nil || selected.Name.Name != name || catalog.TypeSpec.Name == nil || catalog.TypeSpec.Name.Name != name {
		return false
	}
	if smodel.RenderTypeBodyFromSpec(selected) != smodel.RenderTypeBodyFromSpec(catalog.TypeSpec) {
		return false
	}
	referenced := map[string]bool{}
	ast.Inspect(selected.Type, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok {
			if identifier, ok := selector.X.(*ast.Ident); ok {
				referenced[identifier.Name] = true
			}
		}
		return true
	})
	for alias := range referenced {
		catalogImport := catalog.Imports[alias]
		if catalogImport == nil {
			for _, candidate := range catalog.Imports {
				if candidate != nil && (candidate.Alias == alias || candidate.Alias == "" && path.Base(candidate.Path) == alias) {
					if catalogImport != nil {
						return false
					}
					catalogImport = candidate
				}
			}
		}
		if catalogImport == nil || catalogImport.Alias != "" && catalogImport.Alias != alias {
			return false
		}
		matches := 0
		for _, imported := range record.imports {
			importPath, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				return false
			}
			if imported.Name != nil {
				if imported.Name.Name == alias && importPath == catalogImport.Path {
					matches++
				}
			} else if importPath == catalogImport.Path {
				matches++
			}
		}
		if matches != 1 {
			return false
		}
	}
	return true
}

// Reuse native descriptor reference authority rather than parsing local fields
// again. A stable physical snapshot alone cannot validate an older catalog.
func authoredInputClosure(declarations *factoryDeclarations, resolver *typecatalog.Resolver, pkg string, root *x.Type) bool {
	seen := map[string]bool{}
	var visit func(*x.Type) bool
	visit = func(descriptor *x.Type) bool {
		if descriptor == nil || descriptor.PkgPath != pkg {
			return false
		}
		key := descriptor.Key()
		if seen[key] {
			return true
		}
		records := declarations.types[descriptor.Name]
		if len(records) != 1 || !authoredInputDeclarationMatch(records[0], descriptor, pkg, descriptor.Name) {
			return false
		}
		seen[key] = true
		references, err := xshape.New(descriptor, nil).References()
		if err != nil {
			return false
		}
		for _, reference := range references {
			if !strings.HasPrefix(reference, pkg+".") {
				continue
			}
			dependency, err := resolver.Descriptor(reference)
			if err != nil || dependency == nil || dependency.Key() != reference || !visit(dependency) {
				return false
			}
		}
		return true
	}
	return visit(root)
}

// Keep the effective Go flag spelling intact, including whole-argument quotes.
// Only the new ordinary path overrides an explicit module-write request; the
// existing factory selector and its policy are unchanged.
func authoredInputBuild(ctx context.Context, build *gobuild.Context) (*gobuild.Context, bool, error) {
	raw, flags, err := authoredInputFlags(ctx, build)
	if err != nil {
		return nil, false, err
	}
	mode := ""
	for _, flag := range flags {
		if flag == "-overlay" || strings.HasPrefix(flag, "-overlay=") {
			return nil, true, nil
		}
		if value, ok := strings.CutPrefix(flag, "-mod="); ok {
			mode = value
		}
	}
	selected := build.Clone()
	if mode == "mod" {
		selected.Env = append(selected.Env, "GOFLAGS="+strings.TrimSpace(raw)+" -mod=readonly")
	}
	return selected, false, nil
}

// Query the Go tool using the same effective directory/environment as selection.
// Persistent GOENV values and quoted GOFLAGS are part of that compiler authority.
func authoredInputFlags(ctx context.Context, build *gobuild.Context) (string, []string, error) {
	command := exec.CommandContext(ctx, "go", "env", "GOFLAGS")
	command.Dir = build.Dir
	command.Env = append(os.Environ(), build.Env...)
	data, err := command.Output()
	if err != nil {
		return "", nil, err
	}
	flags, err := goflags.Split(string(data))
	return string(data), flags, err
}

func authoredInputOverlay(ctx context.Context, build *gobuild.Context) (bool, error) {
	_, flags, err := authoredInputFlags(ctx, build)
	if err != nil {
		return false, err
	}
	for _, flag := range flags {
		if flag == "-overlay" || strings.HasPrefix(flag, "-overlay=") {
			return true, nil
		}
	}
	return false, nil
}

func (p *authoredInputProof) validate(ctx context.Context) error {
	if p == nil {
		return nil
	}
	overlay, err := authoredInputOverlay(ctx, p.build)
	if err != nil || overlay {
		return fmt.Errorf("authored independent read build flags changed during compilation")
	}
	selected, err := selectedFactoryPackage(ctx, p.build, p.declarations.pkg)
	if err != nil {
		return fmt.Errorf("authored independent read selection changed: %w", err)
	}
	files := append(append([]string(nil), selected.GoFiles...), selected.CgoFiles...)
	if selected.Dir != p.declarations.directory || len(files) != len(p.declarations.files) {
		return fmt.Errorf("authored independent read source selection changed during compilation")
	}
	seen := map[string]bool{}
	for _, name := range files {
		before, ok := p.declarations.files[name]
		if !ok || seen[name] {
			return fmt.Errorf("authored independent read source selection changed during compilation")
		}
		seen[name] = true
		after, err := os.ReadFile(filepath.Join(selected.Dir, name))
		if err != nil || string(after) != before {
			return fmt.Errorf("authored independent read source %q changed during compilation", name)
		}
	}
	return nil
}
