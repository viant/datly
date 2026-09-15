package generate

import (
	"fmt"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
	"path/filepath"
	"sort"
	"strings"
)

func (d *shapeDestinations) partition(p *Plan) error {
	if d.authority == nil {
		return nil
	}
	p.Destinations = map[string]string{}
	// Defaults become concrete only in the canonical shape planner.
	add := func(role, name, file string) shapeDestination {
		dest, ok := d.byRole[role]
		if !ok {
			dest.pkg = p.Package
		}
		dest.name = name
		if dest.file == "" {
			dest.file = file
		}
		d.byName[name] = dest
		p.Destinations[role] = dest.pkg + "/" + dest.file + ":" + name
		return dest
	}
	if p.Input.Ownership == ContractGenerated {
		dest := add("input", p.Input.Type, p.Input.Destination)
		p.Input.Package = dest.pkg
		if marker := p.inputMarkerType(); marker != "" {
			d.byName[marker] = shapeDestination{name: marker, pkg: dest.pkg}
		}
	}
	if p.Output.Ownership == ContractGenerated {
		dest := add("output", p.Output.Type, p.Output.Destination)
		p.Output.Package = dest.pkg
	}
	for i := range p.Views {
		v := &p.Views[i]
		if v.Ownership != ViewGenerated {
			continue
		}
		role := v.Identity
		if v.Type == p.RootViewType {
			role = RootViewPath
		}
		dest := add(role, v.Name, v.Destination)
		v.Package = dest.pkg
		if len(v.SetMarkerFields) > 0 {
			d.byName[v.Name+"Has"] = shapeDestination{name: v.Name + "Has", pkg: dest.pkg}
		}
	}
	for i := range p.HelperTypes {
		h := &p.HelperTypes[i]
		h.Package = p.Package
		if p.Input.Ownership == ContractGenerated && p.Input.Package != "" {
			h.Package = p.Input.Package
		}
		d.byName[h.Name] = shapeDestination{name: h.Name, pkg: h.Package}
	}
	groups := map[string]*Plan{}
	group := func(pkg string) *Plan {
		if result := groups[pkg]; result != nil {
			return result
		}
		result := &Plan{OwnerIdentity: p.OwnerIdentity, ComponentPackage: p.ComponentPackage, ComponentName: p.ComponentName, Package: pkg, GoPackage: filepath.Base(pkg), ProjectRoot: p.ProjectRoot, ShapesOnly: true, RouterDest: p.RouterDest, ViewDest: p.ViewDest, Imports: append([]spec.ImportSpec(nil), p.Imports...)}
		groups[pkg] = result
		return result
	}
	alias := func(name, pkg string) {
		imp := uniqueImportAlias(p, pkg)
		ensureImport(p, imp, pkg)
		p.Aliases = append(p.Aliases, TypeAlias{Name: name, Type: imp + "." + name})
	}
	if p.Input.Ownership == ContractGenerated && p.Input.Package != p.Package {
		g := group(p.Input.Package)
		g.Input = p.Input
		g.Input.Fields = append([]Field(nil), p.Input.Fields...)
		alias(p.Input.Type, p.Input.Package)
		if marker := p.inputMarkerType(); marker != "" {
			alias(marker, p.Input.Package)
		}
	}
	if p.Output.Ownership == ContractGenerated && p.Output.Package != p.Package {
		g := group(p.Output.Package)
		g.Output = p.Output
		g.Output.Fields = append([]Field(nil), p.Output.Fields...)
		alias(p.Output.Type, p.Output.Package)
	}
	for _, v := range p.Views {
		if v.Ownership == ViewGenerated && v.Package != p.Package {
			g := group(v.Package)
			v.Fields = append([]Field(nil), v.Fields...)
			g.Views = append(g.Views, v)
			alias(v.Name, v.Package)
			if len(v.SetMarkerFields) > 0 {
				alias(v.Name+"Has", v.Package)
			}
		}
	}
	for _, helper := range p.HelperTypes {
		if helper.Package != p.Package {
			g := group(helper.Package)
			helper.Fields = append([]Field(nil), helper.Fields...)
			g.HelperTypes = append(g.HelperTypes, helper)
			alias(helper.Name, helper.Package)
		}
	}
	if err := d.relocateContractHooks(p, groups); err != nil {
		return err
	}
	if err := d.relocateEntityMethods(p, groups); err != nil {
		return err
	}
	keys := make([]string, 0, len(groups))
	for pkg := range groups {
		keys = append(keys, pkg)
	}
	sort.Strings(keys)
	for _, pkg := range keys {
		g := groups[pkg]
		rewrite := xshape.Resolver{Rewriter: func(name string) (string, error) {
			target, ok := d.byName[name]
			if !ok {
				ref, err := (xshape.Resolver{}).Reference(name)
				if err != nil {
					return "", err
				}
				if ref.Qualifier != "" {
					for _, imp := range g.Imports {
						a := imp.Alias
						if a == "" {
							a = packageAlias(imp.Package)
						}
						if a == ref.Qualifier && imp.Package == g.Package {
							return ref.Name, nil
						}
					}
					return name, nil
				}
				target = shapeDestination{name: name, pkg: p.Package}
			}
			if target.pkg == g.Package {
				return target.name, nil
			}
			a := uniqueImportAlias(g, target.pkg)
			ensureImport(g, a, target.pkg)
			return a + "." + target.name, nil
		}}
		for _, fields := range [][]Field{g.Input.Fields, g.Output.Fields} {
			for i := range fields {
				value, err := rewrite.Rewrite(fields[i].Type)
				if err != nil {
					return err
				}
				fields[i].Type = value
			}
		}
		for i := range g.HelperTypes {
			for j := range g.HelperTypes[i].Fields {
				field := &g.HelperTypes[i].Fields[j]
				value, err := rewrite.Rewrite(field.Type)
				if err != nil {
					return err
				}
				field.Type = value
			}
		}
		for i := range g.Views {
			for j := range g.Views[i].Fields {
				f := &g.Views[i].Fields[j]
				value, err := rewrite.Rewrite(f.Type)
				if err != nil {
					return err
				}
				f.Type = value
			}
		}
		p.ShapePackages = append(p.ShapePackages, g)
	}

	return nil
}

func (p *Plan) localShape(pkg string) bool { return pkg == "" || pkg == p.Package }
func (p *Plan) aliasSource() string {
	var b strings.Builder
	b.WriteString("package " + p.PackageName() + "\n\nimport (\n")
	fields := make([]Field, 0, len(p.Aliases))
	for _, a := range p.Aliases {
		fields = append(fields, Field{Type: a.Type})
	}
	for _, imp := range importsForFields(fields, p.Imports) {
		fmt.Fprintf(&b, "%s %q\n", imp.Alias, imp.Package)
	}
	b.WriteString(")\n\n")
	for _, a := range p.Aliases {
		fmt.Fprintf(&b, "type %s = %s\n", a.Name, a.Type)
	}
	return b.String()
}
