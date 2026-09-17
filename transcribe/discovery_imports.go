package transcribe

import (
	"context"
	"errors"
	"fmt"
	"go/build"
	"io/fs"
	"os"
	"sort"

	"github.com/viant/datly/transcribe/dql"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	loaderast "github.com/viant/x/loader/ast"
	xmodule "github.com/viant/x/module"
	smodel "github.com/viant/x/syntetic/model"
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
	for _, file := range files {
		content, err := os.ReadFile(file.Path)
		if err != nil {
			return nil, err
		}
		d.collectImports(imports, string(content))
	}
	return d.loadImports(ctx, imports)
}

func (d *dqlPackageDiscovery) loadSource(ctx context.Context, source string) ([]string, error) {
	imports := map[string]bool{}
	d.collectImports(imports, source)
	return d.loadImports(ctx, imports)
}

func (d *dqlPackageDiscovery) collectImports(imports map[string]bool, source string) {
	prepared := dql.PrepareSource(source)
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
		_, err := d.workspace.Package(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		available = append(available, path)
	}
	if len(available) == 0 {
		return nil, nil
	}
	packagesByPath := map[string]*smodel.Package{}
	for _, path := range available {
		module, err := (loaderast.LocalPackageLoader{Workspace: d.workspace}).Load(ctx, path)
		if err != nil {
			var noGo *build.NoGoError
			if errors.As(err, &noGo) {
				continue
			}
			return nil, fmt.Errorf("load DQL import %s: %w", path, err)
		}
		for packagePath := range module.Packages {
			if packagesByPath[packagePath] == nil {
				packagesByPath[packagePath] = module.Packages[packagePath]
			}
		}
	}
	loaded := make([]string, 0, len(packagesByPath))
	packagePaths := make([]string, 0, len(packagesByPath))
	for path := range packagesByPath {
		packagePaths = append(packagePaths, path)
	}
	sort.Strings(packagePaths)
	for _, path := range packagePaths {
		pkg := packagesByPath[path]
		if d.registry != nil {
			for _, declared := range pkg.Types {
				if declared != nil {
					if linked := d.registry.Lookup(path + "." + declared.Name); linked != nil && linked.Type != nil {
						declared.ReflectType = linked.Type
					}
				}
			}
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
