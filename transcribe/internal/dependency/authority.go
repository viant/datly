package dependency

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/viant/datly/bootstrap"
	xmodule "github.com/viant/x/module"
	"github.com/viant/x/syntetic/model"
)

type Authority struct {
	Packages map[string]*model.Package
}

// LoadAuthority loads selected component packages and their local
// contract dependency closure. Workspace.Package only resolves selected roots
// and explicit local replacements, so this never promotes module-cache or
// unrelated third-party dependencies to package authority.
func LoadAuthority(ctx context.Context, workspace *xmodule.Workspace, routes []*bootstrap.RouteSource) (*Authority, error) {
	result := &Authority{Packages: map[string]*model.Package{}}
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
		pkg, err := loadPackageFS(ctx, workspace.SourceFS(location.Module), filepath.ToSlash(relative), location.Dir)
		if err != nil {
			return nil, err
		}
		active[packagePath] = true
		result.Packages[packagePath] = pkg
		for _, imported := range pkg.Imports {
			dependency, err := workspace.Package(imported.Path)
			if err != nil || dependency == nil || dependency.Module == nil {
				continue
			}
			allowedModules[dependency.Module.Path] = true
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
