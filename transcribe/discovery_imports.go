package transcribe

import (
	"context"
	"errors"
	"fmt"
	"go/build"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/viant/datly/transcribe/dql"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	loaderast "github.com/viant/x/loader/ast"
	xmodule "github.com/viant/x/module"
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
	var loaded []string
	for _, path := range paths {
		location, err := d.workspace.Package(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if location == nil {
			continue
		}
		relative, err := filepath.Rel(location.Module.Dir, location.Dir)
		if err != nil {
			return nil, err
		}
		pkg, err := loaderast.LoadPackageFS(ctx, d.workspace.SourceFS(location.Module), filepath.ToSlash(relative))
		if err != nil {
			var noGo *build.NoGoError
			if errors.As(err, &noGo) {
				continue
			}
			return nil, fmt.Errorf("load DQL import %s: %w", path, err)
		}
		if d.registry != nil {
			for _, declared := range pkg.Types {
				if declared != nil {
					if linked := d.registry.Lookup(path + "." + declared.Name); linked != nil && linked.Type != nil {
						declared.ReflectType = linked.Type
					}
				}
			}
		}
		if err = (&gen.Result{}).RegisterPackage(d.catalog, pkg, location.Dir); err != nil {
			return nil, fmt.Errorf("register DQL import %s: %w", path, err)
		}
		loaded = append(loaded, path)
	}
	return loaded, nil
}
