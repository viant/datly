package generate

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

// shapeDestinations resolves DQL names and files before local shape planning.
// Partitioning keeps structural generation and regeneration on the same Plan
// and persistence owners for every resulting package.
type shapeDestinations struct {
	planningOnly bool
	input        *Input
	authority    *typecatalog.DestinationAuthority
	byRole       map[string]shapeDestination
	byName       map[string]shapeDestination
	authored     map[string]string
}
type shapeDestination struct{ name, pkg, file string }
type TypeAlias struct{ Name, Type string }

func (d *shapeDestinations) prepare() error {
	d.byRole = map[string]shapeDestination{}
	d.byName = map[string]shapeDestination{}
	d.authored = map[string]string{}
	if d.input.ProjectRoot == "" && !d.planningOnly {
		return nil
	}
	var err error
	if d.input.ProjectRoot != "" {
		d.authority, err = typecatalog.NewDestinationAuthority(d.input.ProjectRoot)
		if err != nil {
			return err
		}
	}
	c := d.input.Component
	if context := c.TypeContext; context != nil {
		aliases := map[string]string{}
		imports := make([]spec.ImportSpec, 0, len(context.Imports))
		for _, item := range context.Imports {
			alias := item.Alias
			if alias == "" {
				alias = packageAlias(item.Package)
			}
			if previous, ok := aliases[alias]; ok {
				if previous != item.Package {
					return fmt.Errorf("ambiguous destination import alias %s: %s and %s", alias, previous, item.Package)
				}
				continue
			}
			aliases[alias] = item.Package
			imports = append(imports, item)
		}
		context.Imports = imports
	}
	if c.Settings != nil {
		g := c.Settings.Generation
		inputFile, outputFile := "", ""
		if g != nil {
			inputFile = g.InputFile
			outputFile = g.OutputFile
		}
		if ref := d.input.Contracts.Input; ref != nil {
			if err = d.linked("input", c.Settings.InputType, inputFile, ref.DescriptorKey); err != nil {
				return err
			}
		}
		if ref := d.input.Contracts.Output; ref != nil {
			if err = d.linked("output", c.Settings.OutputType, outputFile, ref.DescriptorKey); err != nil {
				return err
			}
		}
		if d.input.Contracts.Input == nil {
			c.Settings.InputType, inputFile, err = d.prepareShape("input", c.Settings.InputType, inputFile, d.input.TargetPackage)
			if err != nil {
				return err
			}
		}
		if d.input.Contracts.Output == nil {
			c.Settings.OutputType, outputFile, err = d.prepareShape("output", c.Settings.OutputType, outputFile, d.input.TargetPackage)
			if err != nil {
				return err
			}
		}
		if g != nil {
			g.InputFile = inputFile
			g.OutputFile = outputFile
		}
	}
	inherited := ""
	if c.Settings != nil && c.Settings.Generation != nil {
		inherited = c.Settings.Generation.ViewFile
	}
	visited := map[*spec.View]bool{}
	var views func(*spec.View, string, string, string) error
	views = func(v *spec.View, role, inherited, inheritedPackage string) error {
		if v == nil || visited[v] {
			return nil
		}
		visited[v] = true
		dest := v.Dest
		if dest == "" {
			dest = inherited
		}
		originalDest := dest
		if ref := d.input.Views[role]; ref != nil {
			if err = d.linked("view "+role, v.TypeName, dest, ref.DescriptorKey); err != nil {
				return err
			}
		}
		if d.input.Views[role] == nil {
			v.TypeName, dest, err = d.prepareShape(role, v.TypeName, dest, inheritedPackage)
			if err != nil {
				return err
			}
			v.Dest = dest
		}
		for _, rel := range v.Relations {
			if rel != nil && rel.View != nil {
				id, e := rel.View.Identity()
				if e != nil {
					return e
				}
				if e = views(rel.View, id, originalDest, d.byRole[role].pkg); e != nil {
					return e
				}
			}
		}
		return nil
	}
	if err = views(c.RootView, RootViewPath, inherited, d.input.TargetPackage); err != nil {
		return err
	}
	for _, v := range c.Views {
		id, e := v.Identity()
		if e != nil {
			return e
		}
		fallback := d.input.TargetPackage
		if d.input.Contracts.Input == nil {
			for _, bound := range d.input.ViewBindings {
				if bound == id {
					if dest, ok := d.byRole["input"]; ok && dest.pkg != "" {
						fallback = dest.pkg
					}
					break
				}
			}
		}
		if e = views(v, id, inherited, fallback); e != nil {
			return e
		}
	}
	if c.Settings != nil && c.Settings.Generation != nil && inherited != "" {
		c.Settings.Generation.ViewFile = filepath.Base(inherited)
	}
	// References to explicitly generated shapes are localized only after their
	// declared package has been validated. Linked descriptors are never moved.
	resolver := xshape.Resolver{Rewriter: func(name string) (string, error) {
		if local := d.authored[name]; local != "" {
			return local, nil
		}
		return name, nil
	}}
	for _, p := range spec.EffectiveParameters(c.Parameters) {
		for _, expr := range []*string{&p.TypeExpr, &p.OutputTypeExpr} {
			if *expr != "" {
				*expr, err = resolver.Rewrite(*expr)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (d *shapeDestinations) prepareShape(role, name, file, fallbackPackage string) (string, string, error) {
	original := strings.TrimSpace(name)
	name = original
	explicitPackage := false
	pkg := fallbackPackage
	if pkg == "" {
		pkg = d.input.TargetPackage
	}
	if original != "" {
		ref, err := (xshape.Resolver{}).Reference(original)
		if err != nil {
			return "", "", fmt.Errorf("%s generation type: %w", role, err)
		}
		if len(ref.Wrappers) > 0 || len(ref.Arguments) > 0 || !ref.Exported() {
			return "", "", fmt.Errorf("generated %s type %q must name an exported non-generic type", role, original)
		}
		name = ref.Name
		if ref.Qualifier != "" {
			explicitPackage = true
			pkg = ref.Qualifier
			if c := d.input.Component.TypeContext; c != nil {
				for _, imp := range c.Imports {
					if imp.Alias == pkg {
						pkg = imp.Package
						break
					}
				}
			}
			if d.authority != nil {
				target, err := d.authority.Package(pkg, "")
				if err != nil {
					return "", "", fmt.Errorf("%s destination: %w", role, err)
				}
				pkg = target.ImportPath
			}
		}
	}
	if file != "" {
		if strings.Contains(file, "\\") || filepath.IsAbs(file) {
			return "", "", fmt.Errorf("invalid %s destination %q", role, file)
		}
		for _, part := range strings.Split(file, "/") {
			if part == ".." || part == "." || part == "" {
				return "", "", fmt.Errorf("%s destination %q escapes or has ambiguous path", role, file)
			}
		}
		if filepath.Ext(file) != ".go" {
			return "", "", fmt.Errorf("%s destination %q must be a Go file", role, file)
		}
		if dir := filepath.Dir(file); dir != "." {
			if d.authority != nil {
				target, err := d.authority.Package(filepath.ToSlash(dir), "")
				if err != nil {
					return "", "", err
				}
				if explicitPackage && pkg != target.ImportPath {
					return "", "", fmt.Errorf("%s type package %q conflicts with destination %q", role, pkg, file)
				}
				pkg = target.ImportPath
			}
			file = filepath.Base(file)
		}
	}
	dest := shapeDestination{name: name, pkg: pkg, file: file}
	d.byRole[role] = dest
	if name != "" {
		if previous, ok := d.byName[name]; ok && previous.pkg != pkg {
			return "", "", fmt.Errorf("ambiguous generated type name %q in %s and %s", name, previous.pkg, pkg)
		}
		d.byName[name] = dest
		d.authored[original] = name
	}
	return name, file, nil
}

func (d *shapeDestinations) linked(role, expression, file, key string) error {
	if d.input.TypeResolver == nil {
		return fmt.Errorf("linked %s requires type authority", role)
	}
	descriptor, err := d.input.TypeResolver.Descriptor(key)
	if err != nil {
		return err
	}
	if descriptor == nil {
		return fmt.Errorf("linked %s descriptor %s is unavailable", role, key)
	}
	if expression != "" {
		reference, err := (xshape.Resolver{}).Reference(expression)
		if err != nil {
			return err
		}
		pkg := reference.Qualifier
		if context := d.input.Component.TypeContext; context != nil {
			for _, imp := range context.Imports {
				if imp.Alias == pkg {
					pkg = imp.Package
					break
				}
			}
		}
		if reference.Name != descriptor.Name || pkg != "" && pkg != descriptor.PkgPath {
			return fmt.Errorf("linked %s type %s conflicts with authored destination type %s; linked types cannot be moved", role, key, expression)
		}
	}
	if d.authority != nil && file != "" {
		dir := filepath.Dir(file)
		if dir != "." {
			destination, err := d.authority.Package(filepath.ToSlash(dir), "")
			if err != nil {
				return err
			}
			if destination.ImportPath != descriptor.PkgPath {
				return fmt.Errorf("linked %s type %s cannot move to destination %s", role, key, file)
			}
		}
	}
	return nil
}

func (d *shapeDestinations) contracts(p *Plan) {
	if d.authority == nil {
		return
	}
	for role, contract := range map[string]*ContractPlan{"input": &p.Input, "output": &p.Output} {
		if contract.Ownership != ContractGenerated {
			continue
		}
		destination, ok := d.byRole[role]
		if !ok {
			continue
		}
		contract.Package = destination.pkg
		if !p.localShape(contract.Package) {
			alias := uniqueImportAlias(p, contract.Package)
			ensureImport(p, alias, contract.Package)
		}
	}
}
