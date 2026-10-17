package dependency

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/viant/datly/internal/packageasset"
	"go/build"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/viant/datly/transcribe/dql"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/internal/program"
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
// Snapshot retains compilation-local source authority used by the same AST load.
// Select returns build-selected filenames relative to the package directory.
type Snapshot struct {
	Packages map[string]*smodel.Package
	Files    []gen.BorrowedAuthorityFile
	Select   func(context.Context, string, string) ([]string, error)
}

type Loader struct {
	Snapshot  *Snapshot
	Workspace *xmodule.Workspace
	Catalog   *typecatalog.Catalog
	Registry  *x.Registry
}

func (d *Loader) Load(ctx context.Context, files []xmodule.File) ([]string, error) {
	imports := map[string]bool{}
	predicates := map[string]bool{}
	for _, file := range files {
		content, err := os.ReadFile(file.Path)
		if err != nil {
			return nil, err
		}
		if header, _, err := dql.ParseHandlerSource(string(content)); err != nil {
			return nil, err
		} else if header != nil && header.Factory != "" && !isGeneratedPostFactorySource(header, string(content)) {
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

func (d *Loader) LoadSource(ctx context.Context, source, scope string) ([]string, error) {
	if header, _, err := dql.ParseHandlerSource(source); err != nil {
		return nil, err
	} else if header != nil && header.Factory != "" && !isGeneratedPostFactorySource(header, source) {
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

func (d *Loader) loadDependencies(ctx context.Context, imports, predicates map[string]bool) ([]string, error) {
	resources, err := d.LoadImports(ctx, imports)
	if err != nil {
		return nil, err
	}
	for _, path := range resources {
		delete(predicates, path)
	}
	// Predicate packages supply type authority only, not resource or component
	// discovery roots. Explicit DQL imports retain their existing resource role.
	if _, err = d.LoadImports(ctx, predicates); err != nil {
		return nil, err
	}
	return resources, nil
}

func (d *Loader) collectImports(imports map[string]bool, prepared *dql.PreparedSource) {
	if prepared.TypeContext == nil {
		return
	}
	for _, item := range prepared.TypeContext.Imports {
		imports[item.Package] = true
	}
}

func (d *Loader) LoadImports(ctx context.Context, imports map[string]bool) ([]string, error) {
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
		location, err := d.Workspace.Package(path)
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
	packagesByPath, err := loadAvailablePackageClosure(ctx, d.Workspace, available, d.Snapshot)
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
		location, err := d.Workspace.Package(path)
		if err != nil || location == nil {
			return nil, err
		}
		if err = (&gen.Result{}).RegisterPackage(d.Catalog, pkg, location.Dir); err != nil {
			return nil, fmt.Errorf("register DQL import %s: %w", path, err)
		}
		loaded = append(loaded, path)
	}
	return loaded, nil
}

// Enrich declarations, rather than registering reflected packages, so source
// metadata and manifest ownership remain authoritative.
func (d *Loader) linkSourcePackage(pkg *smodel.Package, linked []reflect.Type) error {
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
		if d.Registry != nil {
			if explicit := d.Registry.Lookup(key); explicit != nil && explicit.Type != nil {
				typ = explicit.Type
			}
		}
		if typ == nil {
			if ambiguous[declared.Name] {
				return fmt.Errorf("type %q has ambiguous linked compiled identities", key)
			}
			typ = byName[declared.Name]
		}
		existing, _, err := d.Catalog.ResolveRuntimeType(typecatalog.PackageAuthority, key)
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

func (d *Loader) registerLinkedPackageTypes(path string) error {
	var descriptors []*x.Type
	for _, typeOf := range xunsafe.PackageTypes(path) {
		if typeOf != nil && typeOf.Name() != "" {
			descriptors = append(descriptors, x.NewType(typeOf))
		}
	}
	if len(descriptors) == 0 {
		return nil
	}
	if err := d.Catalog.LinkRuntimeAll(typecatalog.TypeOriginPackage, descriptors...); err != nil {
		return fmt.Errorf("register linked DQL import %s: %w", path, err)
	}
	return nil
}

func loadAvailablePackageClosure(ctx context.Context, workspace *xmodule.Workspace, roots []string, snapshots ...*Snapshot) (map[string]*smodel.Package, error) {
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
		var snapshot *Snapshot
		if len(snapshots) > 0 {
			snapshot = snapshots[0]
		}
		var pkg *smodel.Package
		var err error
		if snapshot != nil {
			pkg, err = loadSnapshotPackage(ctx, workspace, path, snapshot)
		} else {
			pkg, err = LoadPackage(ctx, workspace, path)
		}
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

func LoadPackage(ctx context.Context, workspace *xmodule.Workspace, packagePath string) (*smodel.Package, error) {
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

func isGeneratedPostFactorySource(header *dql.HandlerHeader, source string) bool {
	_, ok := program.FactoryContracts(header, dql.PrepareSource(source))
	return ok
}

func loadSnapshotPackage(ctx context.Context, workspace *xmodule.Workspace, packagePath string, snapshot *Snapshot) (*smodel.Package, error) {
	if pkg := snapshot.Packages[packagePath]; pkg != nil {
		return pkg, nil
	}
	location, err := workspace.Package(packagePath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil || location == nil {
		return nil, err
	}
	var names []string
	if snapshot.Select != nil {
		names, err = snapshot.Select(ctx, packagePath, location.Dir)
	} else {
		var selected *build.Package
		selected, err = build.Default.ImportDir(location.Dir, 0)
		if err == nil {
			names = append(append([]string(nil), selected.GoFiles...), selected.CgoFiles...)
		}
	}
	if err != nil {
		return nil, err
	}
	relative, err := filepath.Rel(location.Module.Dir, location.Dir)
	if err != nil {
		return nil, err
	}
	paths := []string{"go.mod"}
	for _, name := range names {
		if filepath.Base(name) != name || !strings.HasSuffix(name, ".go") {
			return nil, fmt.Errorf("invalid selected Go filename %q", name)
		}
		paths = append(paths, filepath.ToSlash(filepath.Join(relative, name)))
	}
	source := workspace.SourceFS(location.Module)
	frozen, err := (packageasset.Snapshotter{Source: source}).Files(ctx, paths)
	if err != nil {
		return nil, err
	}
	// Seal the same immutable bytes the parser consumes, never a second live read.
	files := make([]gen.BorrowedAuthorityFile, 0, len(paths))
	for _, name := range paths {
		data, err := fs.ReadFile(frozen, name)
		if err != nil {
			return nil, err
		}
		info, err := fs.Stat(source, name)
		if err != nil {
			return nil, err
		}
		files = append(files, gen.BorrowedAuthorityFile{Path: filepath.Join(location.Module.Dir, filepath.FromSlash(name)), Mode: info.Mode(), SHA256: fmt.Sprintf("%x", sha256.Sum256(data)), Bytes: data})
	}
	pkg, err := loaderast.LoadPackageFS(ctx, frozen, filepath.ToSlash(relative))
	if err != nil {
		return nil, err
	}
	if snapshot.Packages == nil {
		snapshot.Packages = map[string]*smodel.Package{}
	}
	existing := map[string]gen.BorrowedAuthorityFile{}
	for _, file := range snapshot.Files {
		existing[file.Path] = file
	}
	for _, file := range files {
		if prior, ok := existing[file.Path]; ok {
			if prior.SHA256 != file.SHA256 || prior.Mode != file.Mode {
				return nil, fmt.Errorf("dependency source changed during compilation: %s", file.Path)
			}
			continue
		}
		existing[file.Path] = file
	}
	snapshot.Packages[packagePath] = pkg
	snapshot.Files = snapshot.Files[:0]
	for _, file := range existing {
		snapshot.Files = append(snapshot.Files, file)
	}
	sort.Slice(snapshot.Files, func(i, j int) bool { return snapshot.Files[i].Path < snapshot.Files[j].Path })
	return pkg, nil
}
