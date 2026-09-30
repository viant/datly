package transcribe

import (
	"context"
	"fmt"
	"reflect"
	"sort"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	"github.com/viant/xunsafe"
)

func (d *dqlPackageDiscovery) loadCodecDependencies(ctx context.Context, component *spec.Component, scope *typecatalog.ResolutionContext, contracts ...reflect.Type) error {
	_, err := bootstrap.NormalizeCodecReferences(component, scope)
	if err != nil {
		return err
	}
	names, err := bootstrap.CodecReferences(component, scope, contracts...)
	if err != nil {
		return err
	}
	packages := map[string]map[string]bool{}
	for _, name := range names {
		existing, found, err := d.catalog.Resolve(typecatalog.PackageAuthority, name)
		if err != nil {
			return err
		}
		if found {
			if d.registry != nil {
				if explicit := d.registry.Lookup(name); explicit != nil && explicit.Type != nil && existing.Type != nil && explicit.Type != existing.Type {
					return fmt.Errorf("codec type %q is already linked to a different compiled identity", name)
				}
			}
			continue
		}
		path, typeName, err := (xshape.Resolver{}).CanonicalReference(name)
		if err != nil {
			return err
		}
		if packages[path] == nil {
			packages[path] = map[string]bool{}
		}
		packages[path][typeName] = true
	}
	paths := make([]string, 0, len(packages))
	for path := range packages {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		pkg, err := loadAvailablePackage(ctx, d.workspace, path)
		if err != nil {
			return err
		}
		if pkg == nil {
			for name := range packages[path] {
				key := path + "." + name
				var descriptor *x.Type
				if d.registry != nil {
					descriptor = d.registry.Lookup(key)
				}
				if descriptor == nil {
					typ, err := bootstrap.LinkedCodecType(key)
					if err != nil {
						return err
					}
					if typ != nil {
						descriptor = x.NewType(typ)
					}
				}
				if descriptor != nil {
					if err := d.catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
						return err
					}
				}
			}
			continue
		}
		// Do not publish sibling types: they could shadow short-name factories.
		selected := *pkg
		selected.Types = nil
		for _, declared := range pkg.Types {
			if declared != nil && packages[path][declared.Name] {
				selected.Types = append(selected.Types, declared)
			}
		}
		if err := d.linkSourcePackage(&selected, xunsafe.PackageTypes(path)); err != nil {
			return err
		}
		staged := typecatalog.NewCatalog()
		location, err := d.workspace.Package(path)
		if err != nil {
			return err
		}
		if err := (&gen.Result{}).RegisterPackage(staged, &selected, location.Dir); err != nil {
			return err
		}
		keys := make([]string, 0, len(selected.Types))
		for _, declared := range selected.Types {
			keys = append(keys, path+"."+declared.Name)
		}
		if err := d.catalog.ImportReferences(staged, keys); err != nil {
			return fmt.Errorf("codec dependencies for %s: %w", path, err)
		}
	}
	return nil
}

func (d *dqlPackageDiscovery) loadPreparedCodecs(ctx context.Context, prepared *dql.PreparedSource, scope string) error {
	if prepared.Err() != nil || prepared.Directives == nil {
		return nil
	}
	component := (&spec.Component{Parameters: prepared.Directives.Params, Views: prepared.Directives.Views}).Clone()
	return d.loadCodecDependencies(ctx, component, compileTypeContext(&Source{Scope: scope}, prepared.TypeContext))
}
