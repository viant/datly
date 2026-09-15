package generate

import (
	"fmt"
	"go/parser"
	"go/token"
	"sort"

	xshape "github.com/viant/x/shape"
)

// relocateEntityMethods moves only generated receiver behavior. Snapshot,
// capture, matching and mutation frame support remain component-owned; receiver
// methods depend on public contracts and their own shape, not that state.
func (d *shapeDestinations) relocateEntityMethods(p *Plan, groups map[string]*Plan) error {
	support := p.EntitySupport
	if support == nil {
		return nil
	}
	receivers := map[string]map[string]bool{}
	for _, method := range support.Methods {
		receiver, err := (xshape.Resolver{}).Reference(method.Receiver)
		if err != nil {
			return err
		}
		destination, ok := d.byName[receiver.Name]
		if !ok || destination.pkg == p.Package {
			continue
		}
		if groups[destination.pkg] == nil {
			return fmt.Errorf("entity receiver %s has no generated destination plan", receiver.Name)
		}
		if receivers[destination.pkg] == nil {
			receivers[destination.pkg] = map[string]bool{}
		}
		receivers[destination.pkg][receiver.Name] = true
	}
	packages := make([]string, 0, len(receivers))
	for pkg := range receivers {
		packages = append(packages, pkg)
	}
	sort.Strings(packages)
	for _, pkg := range packages {
		target := groups[pkg]
		source, err := support.source(p.PackageName())
		if err != nil {
			return err
		}
		names := make([]string, 0, len(receivers[pkg]))
		for name := range receivers[pkg] {
			names = append(names, name)
		}
		sort.Strings(names)
		projected, rewrite, err := d.projectReceiverMethods(target, []byte(source), names)
		if err != nil {
			return fmt.Errorf("entity package %s: %w", pkg, err)
		}
		methodsFile, err := parser.ParseFile(token.NewFileSet(), "entity_methods.go", projected.Selected, parser.ParseComments)
		if err != nil {
			return err
		}
		remainingFile, err := parser.ParseFile(token.NewFileSet(), "entities.go", projected.Remaining, parser.ParseComments)
		if err != nil {
			return err
		}
		moved := &EntitySupportPlan{File: methodsFile, Destination: p.Generation.File("entity_methods", "entity_methods.go")}
		remaining := make([]EntityMethod, 0, len(support.Methods))
		for _, method := range support.Methods {
			if !receivers[pkg][method.Receiver] {
				remaining = append(remaining, method)
				continue
			}
			if method.ValueType != "" {
				method.ValueType, err = rewrite.Rewrite(method.ValueType)
				if err != nil {
					return err
				}
			}
			if method.Signature != "" {
				method.Signature, err = rewrite.Rewrite(method.Signature)
				if err != nil {
					return err
				}
			}
			moved.Methods = append(moved.Methods, method)
		}
		support.File, support.Methods = remainingFile, remaining
		target.EntitySupport = moved

	}
	return nil
}

// projectReceiverMethods shares native method projection and package type
// authority between regenerable entity support and create-once contract hooks.
func (d *shapeDestinations) projectReceiverMethods(target *Plan, source []byte, names []string) (*xshape.MethodSources, xshape.Resolver, error) {
	parsed, err := (xshape.SourceParser{}).Parse(source)
	if err != nil {
		return nil, xshape.Resolver{}, err
	}
	imports := map[string]string{}
	for alias, item := range parsed.Imports {
		imports[alias] = item.Path
		ensureImport(target, alias, item.Path)
	}
	rewrite := xshape.Resolver{Rewriter: func(name string) (string, error) {
		if destination, ok := d.byName[name]; ok {
			if destination.pkg == target.Package {
				return destination.name, nil
			}
			alias := uniqueImportAlias(target, destination.pkg)
			ensureImport(target, alias, destination.pkg)
			imports[alias] = destination.pkg
			return alias + "." + destination.name, nil
		}
		reference, err := (xshape.Resolver{}).Reference(name)
		if err != nil {
			return "", err
		}
		if imported := imports[reference.Qualifier]; reference.Qualifier != "" && imported == target.Package {
			return reference.Name, nil
		}
		return name, nil
	}}

	result, err := (xshape.SourceParser{}).ProjectMethods(source, xshape.MethodProjection{Receivers: names, Package: target.PackageName(), Resolver: rewrite, Imports: imports})
	for alias, location := range imports {
		ensureImport(target, alias, location)
	}
	return result, rewrite, err
}
