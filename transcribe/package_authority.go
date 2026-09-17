package transcribe

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/viant/datly/bootstrap"
	loaderast "github.com/viant/x/loader/ast"
	xmodule "github.com/viant/x/module"
	"github.com/viant/x/syntetic/model"
)

type scopedPackageAuthority struct {
	Packages map[string]*model.Package
}

// loadScopedPackageAuthority loads selected component packages and modules
// named explicitly by their contract imports. It never recursively promotes
// standard-library or unrelated third-party dependencies to package authority.
func loadScopedPackageAuthority(ctx context.Context, workspace *xmodule.Workspace, routes []*bootstrap.RouteSource) (*scopedPackageAuthority, error) {
	result := &scopedPackageAuthority{Packages: map[string]*model.Package{}}
	allowedModules := map[string]bool{}
	var roots []string
	for _, route := range routes {
		roots = append(roots, route.PackagePath)
		for _, imported := range route.Imports {
			roots = append(roots, imported.Package)
		}
	}
	for _, packagePath := range roots {
		location, err := workspace.Package(packagePath)
		if err == nil && location != nil && location.Module != nil {
			allowedModules[location.Module.Path] = true
		}
	}
	active := map[string]bool{}
	var load func(string) (*model.Package, error)
	load = func(packagePath string) (*model.Package, error) {
		if existing := result.Packages[packagePath]; existing != nil {
			return existing, nil
		}
		if active[packagePath] {
			return nil, fmt.Errorf("package authority import cycle at %s", packagePath)
		}
		location, err := workspace.Package(packagePath)
		if err != nil || location == nil || location.Module == nil || !allowedModules[location.Module.Path] {
			return nil, err
		}
		relative, err := filepath.Rel(location.Module.Dir, location.Dir)
		if err != nil {
			return nil, err
		}
		pkg, err := loaderast.LoadPackageFS(ctx, workspace.SourceFS(location.Module), filepath.ToSlash(relative))
		if err != nil {
			return nil, err
		}
		active[packagePath] = true
		result.Packages[packagePath] = pkg
		for _, imported := range pkg.Imports {
			dependency, err := workspace.Package(imported.Path)
			if err != nil || dependency == nil || dependency.Module == nil || !allowedModules[dependency.Module.Path] {
				continue
			}
			loaded, err := load(imported.Path)
			if err != nil {
				return nil, err
			}
			if loaded != nil {
				pkg.Dependencies = append(pkg.Dependencies, loaded)
			}
		}
		delete(active, packagePath)
		return pkg, nil
	}
	for _, packagePath := range roots {
		if _, err := load(packagePath); err != nil {
			return nil, fmt.Errorf("load package authority %q: %w", packagePath, err)
		}
	}
	return result, nil
}
