package transcribe

import (
	"context"
	"errors"
	"fmt"
	"go/build"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"

	"github.com/viant/datly/transcribe/dql"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	loaderast "github.com/viant/x/loader/ast"
	xmodule "github.com/viant/x/module"
	smodel "github.com/viant/x/syntetic/model"
	"github.com/viant/xunsafe"
)

// DQL imports can name authored Go hooks without a Go component in the source
// package. Load their existing package authority; future generated destinations
// remain unresolved until the ordinary shape planner owns them.
type dqlPackageDiscovery struct {
	workspace *xmodule.Workspace
	catalog   *typecatalog.Catalog
	registry  *x.Registry
}

func (d *dqlPackageDiscovery) load(ctx context.Context, files []xmodule.File) ([]string, error) {
	imports := map[string]bool{}
	predicates := map[string]bool{}
	for _, file := range files {
		content, err := os.ReadFile(file.Path)
		if err != nil {
			return nil, err
		}
		if header, _, err := dql.ParseHandlerSource(string(content)); err != nil {
			return nil, err
		} else if header != nil && header.Factory != "" {
			// Source-backed factories resolve their imports through Go export
			// data, not the unfiltered AST/resource discovery path used by SQL.
			continue
		}
		prepared := dql.PrepareSource(string(content))
		if err := d.loadPreparedCodecs(ctx, prepared, file.ImportPath); err != nil {
			return nil, err
		}
		d.collectImports(imports, prepared)
		if err := collectPredicatePackages(predicates, prepared, file.ImportPath); err != nil {
			return nil, fmt.Errorf("predicate dependencies for %s: %w", file.Path, err)
		}
	}
	return d.loadDependencies(ctx, imports, predicates)
}

func (d *dqlPackageDiscovery) loadSource(ctx context.Context, source, scope string) ([]string, error) {
	if header, _, err := dql.ParseHandlerSource(source); err != nil {
		return nil, err
	} else if header != nil && header.Factory != "" {
		return nil, nil
	}
	imports := map[string]bool{}
	prepared := dql.PrepareSource(source)
	if err := d.loadPreparedCodecs(ctx, prepared, scope); err != nil {
		return nil, err
	}
	d.collectImports(imports, prepared)
	predicates := map[string]bool{}
	if err := collectPredicatePackages(predicates, prepared, scope); err != nil {
		return nil, err
	}
	return d.loadDependencies(ctx, imports, predicates)
}

func (d *dqlPackageDiscovery) loadDependencies(ctx context.Context, imports, predicates map[string]bool) ([]string, error) {
	resources, err := d.loadImports(ctx, imports)
	if err != nil {
		return nil, err
	}
	for _, path := range resources {
		delete(predicates, path)
	}
	// Predicate packages supply type authority only, not resource or component
	// discovery roots. Explicit DQL imports retain their existing resource role.
	if _, err = d.loadImports(ctx, predicates); err != nil {
		return nil, err
	}
	return resources, nil
}

func (d *dqlPackageDiscovery) collectImports(imports map[string]bool, prepared *dql.PreparedSource) {
	if prepared.TypeContext == nil {
		return
	}
	for _, item := range prepared.TypeContext.Imports {
		imports[item.Package] = true
	}
}

func (d *dqlPackageDiscovery) loadImports(ctx context.Context, imports map[string]bool) ([]string, error) {
	paths := make([]string, 0, len(imports))
	for path := range imports {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	available := make([]string, 0, len(paths))
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		location, err := d.workspace.Package(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if location == nil {
			if err := d.registerLinkedPackageTypes(path); err != nil {
				return nil, err
			}
			continue
		}
		available = append(available, path)
	}
	if len(available) == 0 {
		return nil, nil
	}
	packagesByPath, err := loadAvailablePackageClosure(ctx, d.workspace, available)
	if err != nil {
		return nil, err
	}
	if len(packagesByPath) == 0 {
		return nil, nil
	}
	loaded := make([]string, 0, len(packagesByPath))
	packagePaths := make([]string, 0, len(packagesByPath))
	for path := range packagesByPath {
		packagePaths = append(packagePaths, path)
	}
	sort.Strings(packagePaths)
	for _, path := range packagePaths {
		pkg := packagesByPath[path]
		if err := d.linkSourcePackage(pkg, xunsafe.PackageTypes(path)); err != nil {
			return nil, fmt.Errorf("link DQL import %s: %w", path, err)
		}
		location, err := d.workspace.Package(path)
		if err != nil || location == nil {
			return nil, err
		}
		if err = (&gen.Result{}).RegisterPackage(d.catalog, pkg, location.Dir); err != nil {
			return nil, fmt.Errorf("register DQL import %s: %w", path, err)
		}
		loaded = append(loaded, path)
	}
	return loaded, nil
}

// Enrich declarations, rather than registering reflected packages, so source
// metadata and manifest ownership remain authoritative.
func (d *dqlPackageDiscovery) linkSourcePackage(pkg *smodel.Package, linked []reflect.Type) error {
	byName := make(map[string]reflect.Type, len(linked))
	ambiguous := map[string]bool{}
	for _, typ := range linked {
		if typ == nil || typ.PkgPath() != pkg.PkgPath || typ.Name() == "" {
			continue
		}
		if prior := byName[typ.Name()]; prior != nil && prior != typ {
			ambiguous[typ.Name()] = true
		}
		byName[typ.Name()] = typ
	}
	resolved := make(map[*smodel.Type]reflect.Type, len(pkg.Types))
	for _, declared := range pkg.Types {
		if declared == nil {
			continue
		}
		key := pkg.PkgPath + "." + declared.Name
		typ := declared.ReflectType
		if d.registry != nil {
			if explicit := d.registry.Lookup(key); explicit != nil && explicit.Type != nil {
				typ = explicit.Type
			}
		}
		if typ == nil {
			if ambiguous[declared.Name] {
				return fmt.Errorf("type %q has ambiguous linked compiled identities", key)
			}
			typ = byName[declared.Name]
		}
		existing, _, err := d.catalog.ResolveRuntimeType(typecatalog.PackageAuthority, key)
		if err != nil {
			return err
		}
		if existing != nil {
			if typ != nil && typ != existing {
				return fmt.Errorf("type %q is already linked to a different compiled identity", key)
			}
			typ = existing
		}
		resolved[declared] = typ
	}
	for declared, typ := range resolved {
		declared.ReflectType = typ
	}
	return nil
}

func (d *dqlPackageDiscovery) registerLinkedPackageTypes(path string) error {
	var descriptors []*x.Type
	for _, typeOf := range xunsafe.PackageTypes(path) {
		if typeOf != nil && typeOf.Name() != "" {
			descriptors = append(descriptors, x.NewType(typeOf))
		}
	}
	if len(descriptors) == 0 {
		return nil
	}
	if err := d.catalog.LinkRuntimeAll(typecatalog.TypeOriginPackage, descriptors...); err != nil {
		return fmt.Errorf("register linked DQL import %s: %w", path, err)
	}
	return nil
}

func loadAvailablePackageClosure(ctx context.Context, workspace *xmodule.Workspace, roots []string) (map[string]*smodel.Package, error) {
	result := map[string]*smodel.Package{}
	queued := map[string]bool{}
	queue := append([]string(nil), roots...)
	for _, path := range roots {
		queued[path] = true
	}
	for i := 0; i < len(queue); i++ {
		path := queue[i]
		if result[path] != nil {
			continue
		}
		pkg, err := loadAvailablePackage(ctx, workspace, path)
		if err != nil {
			var noGo *build.NoGoError
			if errors.As(err, &noGo) {
				continue
			}
			return nil, fmt.Errorf("load DQL import %s: %w", path, err)
		}
		if pkg == nil {
			continue
		}
		result[path] = pkg
		imports := make([]string, 0, len(pkg.Imports))
		for _, imported := range pkg.Imports {
			imports = append(imports, imported.Path)
		}
		sort.Strings(imports)
		for _, imported := range imports {
			if queued[imported] {
				continue
			}
			location, err := workspace.Package(imported)
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, err
			}
			if location == nil {
				continue
			}
			queued[imported] = true
			queue = append(queue, imported)
		}
	}
	return result, nil
}

func loadAvailablePackage(ctx context.Context, workspace *xmodule.Workspace, packagePath string) (*smodel.Package, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	location, err := workspace.Package(packagePath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil || location == nil {
		return nil, err
	}
	relative, err := filepath.Rel(location.Module.Dir, location.Dir)
	if err != nil {
		return nil, err
	}
	pkg, err := loaderast.LoadPackageFS(ctx, workspace.SourceFS(location.Module), filepath.ToSlash(relative))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return pkg, err
}
