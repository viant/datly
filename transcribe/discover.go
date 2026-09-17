package transcribe

import (
	"context"
	"errors"
	"fmt"
	"github.com/viant/datly/constant"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	packageresources "github.com/viant/datly/bootstrap/resources"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	loaderast "github.com/viant/x/loader/ast"
	xmodule "github.com/viant/x/module"
)

var sourceFileExtensions = map[string]bool{
	".sql": true,
	".dql": true,
}

// Discovery compiles authored component sources selected by module-qualified
// package patterns into one canonical project. It is the local-filesystem
// frontend corresponding to the original transcribe command's source loop.
type Discovery struct {
	Const *constant.Values
	// Workspace fixes source selection to a Go build when supplied.
	Workspace  *xmodule.Workspace
	BaseDir    string
	ModuleDirs []string
	Include    []string
	// TypeInclude loads package-level type authority without selecting those
	// packages for component discovery or resource loading.
	TypeInclude   []string
	Exclude       []string
	Connector     string
	Types         *typecatalog.Catalog
	ColumnRefiner *column.Refiner
	// Registry is trusted compiled package authority. Selected source descriptors
	// retain linked types so lifecycle methods and typed factories remain executable.
	Registry *x.Registry
}

// Compile discovers and compiles each component source exactly once. Plain SQL
// files without a route directive are resources rather than components and are
// ignored; every other compile failure is returned with its source path.
func (d *Discovery) Compile(ctx context.Context) (*ProjectGeneration, error) {
	if d == nil {
		return nil, fmt.Errorf("transcribe discovery is required")
	}
	staged := *d
	if d.Registry != nil {
		var err error
		staged.Registry, err = (x.Cloner{}).Registry(d.Registry)
		if err != nil {
			return nil, err
		}
	}
	d = &staged
	catalog := typecatalog.NewCatalog()
	if d.Types != nil {
		// Discovery stages a detached generation. A failed compile must not
		// publish package descriptors into the caller's active catalog.
		var err error
		catalog, err = d.Types.Clone()
		if err != nil {
			return nil, fmt.Errorf("stage discovery types: %w", err)
		}
	}
	project := &ProjectGeneration{}
	workspace := d.Workspace
	var err error
	if workspace == nil {
		workspace, err = (xmodule.LocalWorkspace{BaseDir: d.BaseDir, ModuleDirs: d.ModuleDirs}).Resolve(ctx)
		if err != nil {
			return nil, err
		}
	}
	if _, err = d.loadTypeIncludes(ctx, catalog, workspace); err != nil {
		return nil, err
	}
	packageSources, packagePaths, err := d.packageSources(ctx, catalog, workspace)
	if err != nil {
		return nil, err
	}
	var files []xmodule.File
	err = workspace.Walk(ctx, d.Include, d.Exclude, func(file xmodule.File) error {
		packagePaths = append(packagePaths, file.ImportPath)
		if !sourceFileExtensions[strings.ToLower(filepath.Ext(file.Path))] {
			return nil
		}
		files = append(files, file)
		return nil
	})
	if err != nil {
		return nil, err
	}
	dqlImports, err := (&dqlPackageDiscovery{workspace: workspace, catalog: catalog, registry: d.Registry}).load(ctx, files)
	if err != nil {
		return nil, err
	}
	packagePaths = append(packagePaths, dqlImports...)
	assets, err := (packageresources.Loader{Workspace: workspace, Packages: packagePaths}).Load(ctx)
	if err != nil {
		return nil, err
	}
	project.Resources = assets.Store
	compilation := &discoveryCompilation{discovery: d, catalog: catalog, packages: packageSources, resources: assets.Store, defaults: map[string]*resource.Store{}}
	compiledPackages := map[string]bool{}
	for _, file := range files {
		if assets.IsAsset(file.Path) {
			continue
		}
		compiled, err := compilation.compileFile(ctx, file)
		if errors.Is(err, dql.ErrMissingRouteDirective) {
			continue
		}
		if err != nil {
			return nil, err
		}
		project.Components = append(project.Components, compiled)
		compiledPackages[packageSourceIdentity(compiled.Source.Scope, compiled.Source.Name)] = true
	}
	if err = compilation.compilePackages(ctx, project, compiledPackages); err != nil {
		return nil, err
	}
	sort.SliceStable(project.Components, func(i, j int) bool {
		left, right := project.Components[i].Source, project.Components[j].Source
		if left.Scope != right.Scope {
			return left.Scope < right.Scope
		}
		return left.Path < right.Path
	})
	return project, nil
}

// Generate compiles the discovered project and delegates all validation and
// persistence to ProjectGeneration.
func (d *Discovery) Generate(ctx context.Context, rootDir string) (*GeneratedProject, error) {
	project, err := d.Compile(ctx)
	if err != nil {
		return nil, err
	}
	return project.Generate(ctx, rootDir)
}

type discoveryCompilation struct {
	discovery *Discovery
	catalog   *typecatalog.Catalog
	packages  map[string]*bootstrap.PackageComponentSource
	resources *resource.Store
	defaults  map[string]*resource.Store
}

func (c *discoveryCompilation) compileFile(ctx context.Context, file xmodule.File) (*Result, error) {
	content, err := os.ReadFile(file.Path)
	if err != nil {
		return nil, err
	}
	resources, err := c.packageResources(ctx, file.Dir)
	if err != nil {
		return nil, err
	}
	source := &Source{
		Const:         c.discovery.Const,
		Scope:         file.ImportPath,
		Name:          strings.TrimSuffix(filepath.Base(file.Path), filepath.Ext(file.Path)),
		Path:          file.Path,
		Text:          string(content),
		Connector:     c.discovery.Connector,
		Resources:     resources,
		Types:         c.catalog,
		ColumnRefiner: c.discovery.ColumnRefiner,
	}
	var result *Result
	if packageSource := c.packages[packageSourceIdentity(file.ImportPath, source.Name)]; packageSource != nil {
		result, err = (&descriptorPackageCompilation{source: source, packageSource: packageSource, catalog: c.catalog}).compile(ctx)
	} else {
		result, err = NewCompiler().Compile(ctx, source)
	}
	if err != nil {
		return nil, fmt.Errorf("compile component source %q: %w", file.Path, err)
	}
	return result, nil
}

func (d *Discovery) loadTypeIncludes(ctx context.Context, catalog *typecatalog.Catalog, workspace *xmodule.Workspace) ([]string, error) {
	if len(d.TypeInclude) == 0 {
		return nil, nil
	}
	imports := map[string]bool{}
	for _, packagePath := range d.TypeInclude {
		packagePath = strings.TrimSpace(packagePath)
		if packagePath != "" {
			imports[packagePath] = true
		}
	}
	return (&dqlPackageDiscovery{workspace: workspace, catalog: catalog, registry: d.Registry}).loadImports(ctx, imports)
}

func (d *Discovery) packageSources(ctx context.Context, catalog *typecatalog.Catalog, workspace *xmodule.Workspace) (map[string]*bootstrap.PackageComponentSource, []string, error) {
	routes, err := (bootstrap.PackageDiscovery{Workspace: workspace, Include: d.Include, Exclude: d.Exclude}).Discover(ctx)
	if err != nil {
		return nil, nil, err
	}
	if len(routes) == 0 {
		return nil, nil, nil
	}
	imports := make([]string, 0, len(routes))
	for _, route := range routes {
		imports = append(imports, route.PackagePath)
	}
	module, err := (loaderast.LocalPackageLoader{Workspace: workspace}).Load(ctx, imports...)
	if err != nil {
		return nil, nil, fmt.Errorf("load package authority: %w", err)
	}
	authorityPaths := map[string]bool{}
	queue := make([]string, 0, len(routes))
	for _, route := range routes {
		if !authorityPaths[route.PackagePath] {
			authorityPaths[route.PackagePath] = true
			queue = append(queue, route.PackagePath)
		}
	}
	for len(queue) > 0 {
		packagePath := queue[0]
		queue = queue[1:]
		pkg := module.Packages[packagePath]
		if pkg == nil {
			return nil, nil, fmt.Errorf("package authority %q was not loaded", packagePath)
		}
		for _, imported := range pkg.Imports {
			if module.Packages[imported.Path] == nil || authorityPaths[imported.Path] {
				continue
			}
			authorityPaths[imported.Path] = true
			queue = append(queue, imported.Path)
		}
	}
	packagePaths := make([]string, 0, len(authorityPaths))
	for packagePath := range authorityPaths {
		packagePaths = append(packagePaths, packagePath)
	}
	sort.Strings(packagePaths)
	for _, packagePath := range packagePaths {
		if d.Registry != nil {
			for _, declared := range module.Packages[packagePath].Types {
				if declared == nil {
					continue
				}
				if linked := d.Registry.Lookup(packagePath + "." + declared.Name); linked != nil && linked.Type != nil {
					declared.ReflectType = linked.Type
				}
			}
		}
		if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, module.Packages[packagePath]); err != nil {
			return nil, nil, fmt.Errorf("register package authority %q: %w", packagePath, err)
		}
	}
	loadedPackagePaths := append([]string(nil), packagePaths...)
	routesByPackage := map[string][]*bootstrap.RouteSource{}
	for _, route := range routes {
		routesByPackage[route.PackagePath] = append(routesByPackage[route.PackagePath], route)
	}
	selected := map[string]*bootstrap.PackageComponentSource{}
	packagePaths = packagePaths[:0]
	for packagePath := range routesByPackage {
		packagePaths = append(packagePaths, packagePath)
	}
	sort.Strings(packagePaths)
	for _, packagePath := range packagePaths {
		packageRoutes := routesByPackage[packagePath]
		resolutionContext := &typecatalog.ResolutionContext{
			DefaultPackage: packagePath, PackagePath: packagePath,
			PackageName: packageRoutes[0].PackageName, PackageDir: packageRoutes[0].Dir,
		}
		seenImports := map[string]bool{}
		for _, route := range packageRoutes {
			for _, item := range route.Imports {
				key := item.Alias + "\x00" + item.Package
				if seenImports[key] {
					continue
				}
				seenImports[key] = true
				resolutionContext.Imports = append(resolutionContext.Imports, typecatalog.PackageImport{Alias: item.Alias, Package: item.Package})
			}
		}
		resolver, resolveErr := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, resolutionContext)
		if resolveErr != nil {
			return nil, nil, resolveErr
		}
		components, groupErr := bootstrap.GroupPackageComponentSources(packageRoutes, resolver)
		if groupErr != nil {
			return nil, nil, groupErr
		}
		for _, component := range components {
			identity := packageSourceIdentity(packagePath, component.ComponentName())
			if selected[identity] != nil {
				return nil, nil, fmt.Errorf("package component %q has more than one contract authority", identity)
			}
			selected[identity] = component
		}
	}
	return selected, loadedPackagePaths, nil
}

func packageSourceIdentity(packagePath, componentName string) string {
	return strings.TrimSpace(packagePath) + "\x00" + strings.TrimSpace(componentName)
}
