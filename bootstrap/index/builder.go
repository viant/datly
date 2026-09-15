package index

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/viant/datly/bootstrap"
	bootstraproutes "github.com/viant/datly/bootstrap/routes"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/typecatalog"
	loaderast "github.com/viant/x/loader/ast"
	xmodule "github.com/viant/x/module"
	synthetic "github.com/viant/x/syntetic/model"
)

const formatVersion = "datly-bootstrap-index-v1"

type Config struct {
	BaseDir    string
	ModuleDirs []string
	Include    []string
	Exclude    []string
	Workspace  *xmodule.Workspace
	Types      *typecatalog.Catalog
}

type Builder struct{ Config Config }

type draft struct {
	component *spec.Component
	packages  map[string]bool
	sources   []Source
	imports   []string
	overlay   bool
}

// Build scans selected packages once at bootstrap and publishes routing-only
// metadata. It creates no persistent sidecar; callers explicitly persist a
// snapshot if their deployment chooses to own that resource.
func (b Builder) Build(ctx context.Context) (*Snapshot, error) {
	if ctx == nil {
		return nil, fmt.Errorf("bootstrap index context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	config := b.Config
	workspace := config.Workspace
	if workspace == nil {
		var err error
		workspace, err = (xmodule.LocalWorkspace{BaseDir: config.BaseDir, ModuleDirs: config.ModuleDirs}).Resolve(ctx)
		if err != nil {
			return nil, err
		}
	}
	selected, sourcesByPackage, err := scanSelectedFiles(ctx, workspace, config.Include, config.Exclude)
	if err != nil {
		return nil, err
	}
	files := make([]xmodule.File, 0, len(selected))
	for _, file := range selected {
		files = append(files, file.file)
	}
	routes, err := (bootstrap.PackageDiscovery{Workspace: workspace}).DiscoverFiles(files)
	if err != nil {
		return nil, err
	}
	packageComponents, dependencySources, err := resolvePackageComponents(ctx, workspace, routes, config.Types)
	if err != nil {
		return nil, err
	}
	drafts := map[string]*draft{}
	for _, component := range packageComponents {
		identity := component.Key.String()
		drafts[identity] = &draft{component: component, packages: map[string]bool{component.Key.Scope: true}, sources: append([]Source(nil), dependencySources[component.Key.Scope]...)}
	}
	for _, file := range selected {
		if file.kind != SourceDQL {
			continue
		}
		content, readErr := os.ReadFile(file.source.Path)
		if readErr != nil {
			return nil, readErr
		}
		prepared := dql.PrepareSource(string(content))
		component, parseErr := dql.ParsePreparedComponentSource(file.source.PackagePath, strings.TrimSuffix(filepath.Base(file.source.Path), filepath.Ext(file.source.Path)), prepared)
		if errors.Is(parseErr, dql.ErrMissingRouteDirective) {
			continue
		}
		if parseErr != nil {
			return nil, fmt.Errorf("index component source %q: %w", file.source.Path, parseErr)
		}
		identity := component.Key.String()
		var componentImports []string
		if component.TypeContext != nil {
			for _, imported := range component.TypeContext.Imports {
				componentImports = append(componentImports, imported.Package)
			}
		}
		current := drafts[identity]
		if current == nil {
			current = &draft{component: component, packages: map[string]bool{file.source.PackagePath: true}, sources: []Source{file.source}, imports: componentImports, overlay: true}
			drafts[identity] = current
			continue
		}
		if current.overlay {
			return nil, fmt.Errorf("component %s has more than one DQL authority", identity)
		}
		// Package+DQL compilation uses the DQL route as the route authority.
		// Keep the package identity while replacing the pre-materialization
		// protocol surface, so the package tag route cannot remain exposed.
		current.component.Routes = cloneRoutes(component.Routes)
		current.component.Static = component.Static.Clone()
		current.component.Settings = mergeIndexSettings(current.component.Settings, component.Settings)
		current.packages[file.source.PackagePath] = true
		current.sources = append(current.sources, file.source)
		current.imports = append(current.imports, componentImports...)
		current.overlay = true
	}
	for identity, current := range drafts {
		if err := (bootstraproutes.Compiler{Component: current.component}).Compile(); err != nil {
			return nil, fmt.Errorf("index component %s routes: %w", identity, err)
		}
	}
	identities := make([]string, 0, len(drafts))
	for identity := range drafts {
		identities = append(identities, identity)
	}
	sort.Strings(identities)
	entries := make([]*Entry, 0, len(identities))
	for _, identity := range identities {
		current := drafts[identity]
		var sources []Source
		for packagePath := range current.packages {
			sources = append(sources, sourcesByPackage[packagePath]...)
		}
		sources = append(sources, current.sources...)
		importedSources, importErr := componentImportSources(ctx, workspace, current.imports)
		if importErr != nil {
			return nil, fmt.Errorf("index component %s imports: %w", identity, importErr)
		}
		sources = append(sources, importedSources...)
		declared, declaredErr := declaredResourceSources(current.component, current.packages, sources, selected)
		if declaredErr != nil {
			return nil, fmt.Errorf("index component %s resources: %w", identity, declaredErr)
		}
		sources = append(sources, declared...)
		sources = uniqueSources(sources)
		sort.Slice(sources, func(i, j int) bool { return sources[i].Path < sources[j].Path })
		entries = append(entries, &Entry{Component: current.component.Clone(), Sources: sources, Fingerprint: digestSources(sources)})
	}
	entries = expandReportEntries(entries)
	sortEntries(entries)
	selection := selectionIdentity(config)
	return newSnapshot(selection, snapshotFingerprint(selection, entries), entries)
}

func componentImportSources(ctx context.Context, workspace *xmodule.Workspace, declared []string) ([]Source, error) {
	if len(declared) == 0 {
		return nil, nil
	}
	imports := make([]string, 0, len(declared))
	seenImports := map[string]bool{}
	for _, item := range declared {
		if packagePath := strings.TrimSpace(item); packagePath != "" && !seenImports[packagePath] {
			seenImports[packagePath] = true
			imports = append(imports, packagePath)
		}
	}
	if len(imports) == 0 {
		return nil, nil
	}
	packages, err := (loaderast.LocalPackageLoader{Workspace: workspace}).Load(ctx, imports...)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(packages.Packages))
	for packagePath := range packages.Packages {
		paths = append(paths, packagePath)
	}
	sort.Strings(paths)
	var result []Source
	for _, packagePath := range paths {
		sources, err := packageGoSources(workspace, packagePath)
		if err != nil {
			return nil, err
		}
		result = append(result, sources...)
	}
	return result, nil
}

func expandReportEntries(entries []*Entry) []*Entry {
	result := append([]*Entry(nil), entries...)
	for _, entry := range entries {
		component := entry.Component
		if component == nil || component.Settings == nil || component.Settings.Report == nil || !component.Settings.Report.Enabled || component.RootView == nil || component.RootView.Groupable == nil || !*component.RootView.Groupable {
			continue
		}
		eligible := make([]*spec.Route, 0, len(component.Routes))
		for _, route := range component.Routes {
			if route != nil && strings.EqualFold(route.Method, http.MethodGet) {
				eligible = append(eligible, route)
			}
		}
		for _, route := range eligible {
			suffix := ""
			if len(eligible) > 1 {
				suffix = typecatalog.ExportedFieldName(route.Name)
			}
			name := typecatalog.ExportedFieldName(component.Key.Name + suffix + "Cube")
			cube := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: component.Key.Scope, Name: name}, Name: name, Routes: []*spec.Route{{Method: http.MethodPost, Path: strings.TrimRight(route.Path, "/") + "/cube", APIKeyHeader: route.APIKeyHeader, APIKeyValue: route.APIKeyValue}}}
			if component.Settings.Report.MCPTool == nil || *component.Settings.Report.MCPTool {
				cube.Routes[0].MCP = []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: name, Description: component.Description}}
			}
			result = append(result, &Entry{Component: cube, Owner: component.Key, Sources: append([]Source(nil), entry.Sources...), Fingerprint: digestStrings(entry.Fingerprint, cube.Key.String())})
			compose := component.Settings.Report.Compose
			if compose == nil || !compose.Enabled {
				continue
			}
			composeName := name + "Compose"
			composed := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: component.Key.Scope, Name: composeName}, Name: composeName, Routes: []*spec.Route{{Method: http.MethodPost, Path: strings.TrimRight(route.Path, "/") + "/cube/compose", APIKeyHeader: route.APIKeyHeader, APIKeyValue: route.APIKeyValue}}}
			if compose.MCPTool == nil || *compose.MCPTool {
				composed.Routes[0].MCP = []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: composeName, Description: component.Description}}
			}
			result = append(result, &Entry{Component: composed, Owner: component.Key, Sources: append([]Source(nil), entry.Sources...), Fingerprint: digestStrings(entry.Fingerprint, composed.Key.String())})
		}
	}
	return result
}

func mergeIndexSettings(base, overlay *spec.Settings) *spec.Settings {
	if overlay == nil {
		return base.Clone()
	}
	result := base.Clone()
	if result == nil {
		result = &spec.Settings{}
	}
	if len(overlay.MCPFolders) > 0 {
		result.MCPFolders = overlay.Clone().MCPFolders
	}
	if overlay.IgnoreEmptyQueryParameters != nil {
		value := *overlay.IgnoreEmptyQueryParameters
		result.IgnoreEmptyQueryParameters = &value
	}
	if overlay.DefaultConnector != "" {
		result.DefaultConnector = overlay.DefaultConnector
	}
	if overlay.Report != nil {
		result.Report = overlay.Report.Clone()
	}
	if overlay.Cache != nil {
		result.Cache = overlay.Clone().Cache
	}
	if overlay.Generation != nil {
		result.Generation = overlay.Generation.Clone()
	}
	if overlay.InputType != "" {
		result.InputType = overlay.InputType
	}
	if overlay.OutputType != "" {
		result.OutputType = overlay.OutputType
	}
	if overlay.JSONMarshalType != "" {
		result.JSONMarshalType = overlay.JSONMarshalType
	}
	if overlay.JSONUnmarshalType != "" {
		result.JSONUnmarshalType = overlay.JSONUnmarshalType
	}
	if overlay.XMLUnmarshalType != "" {
		result.XMLUnmarshalType = overlay.XMLUnmarshalType
	}
	if overlay.Format != "" {
		result.Format = overlay.Format
	}
	if overlay.DateFormat != "" {
		result.DateFormat = overlay.DateFormat
	}
	if overlay.Output != nil {
		result.Output = overlay.Clone().Output
	}
	if overlay.CaseFormat != "" {
		result.CaseFormat = overlay.CaseFormat
	}
	if len(overlay.Const) > 0 {
		result.Const = overlay.Clone().Const
	}
	return result
}

func declaredResourceSources(component *spec.Component, packages map[string]bool, authority []Source, files []selectedFile) ([]Source, error) {
	references := map[string]bool{}
	var visitView func(*spec.View)
	visitView = func(view *spec.View) {
		if view == nil {
			return
		}
		if view.Source != nil {
			if value := filepath.ToSlash(strings.TrimSpace(view.Source.URI)); value != "" {
				references[value] = true
			}
			for _, embedded := range view.Source.Embeds {
				if embedded != nil {
					value := filepath.ToSlash(strings.TrimSpace(embedded.Path))
					references[value] = true
				}
			}
		}
		for _, relation := range view.Relations {
			if relation != nil {
				visitView(relation.View)
			}
		}
	}
	visitView(component.RootView)
	for _, view := range component.Views {
		visitView(view)
	}
	if len(references) == 0 {
		return nil, nil
	}
	var result []Source
	resolved := map[string]bool{}
	for _, file := range files {
		if file.kind != SourceDQL || !packages[file.source.PackagePath] {
			continue
		}
		relative, err := filepath.Rel(file.source.Dir, file.source.Path)
		if err != nil {
			continue
		}
		relative = filepath.ToSlash(relative)
		if references[relative] || references[filepath.Base(relative)] {
			result = append(result, file.source)
			resolved[relative] = true
			resolved[filepath.Base(relative)] = true
		}
	}
	packageDirs := map[string]string{}
	for _, source := range authority {
		if source.Kind == SourceGo {
			packageDirs[source.PackagePath] = source.Dir
		}
	}
	for reference := range references {
		if resolved[reference] || reference == "" || strings.Contains(reference, ":") {
			continue
		}
		for packagePath, dir := range packageDirs {
			path := filepath.Join(dir, filepath.FromSlash(reference))
			content, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			digest := sha256.Sum256(content)
			result = append(result, Source{Kind: SourceResource, Path: path, Dir: dir, PackagePath: packagePath, Fingerprint: hex.EncodeToString(digest[:])})
			resolved[reference] = true
			break
		}
		if !resolved[reference] {
			return nil, fmt.Errorf("declared resource %q was not found", reference)
		}
	}
	return result, nil
}

func uniqueSources(sources []Source) []Source {
	seen := map[string]bool{}
	result := sources[:0]
	for _, source := range sources {
		if seen[source.Path] {
			continue
		}
		seen[source.Path] = true
		result = append(result, source)
	}
	return result
}

func resolvePackageComponents(ctx context.Context, workspace *xmodule.Workspace, routes []*bootstrap.RouteSource, seed *typecatalog.Catalog) ([]*spec.Component, map[string][]Source, error) {
	if len(routes) == 0 {
		return nil, nil, nil
	}
	imports := make([]string, 0, len(routes))
	for _, route := range routes {
		imports = append(imports, route.PackagePath)
	}
	module, err := (loaderast.LocalPackageLoader{Workspace: workspace}).Load(ctx, imports...)
	if err != nil {
		return nil, nil, fmt.Errorf("index package authority: %w", err)
	}
	catalog := typecatalog.NewCatalog()
	if seed != nil {
		catalog, err = seed.Clone()
		if err != nil {
			return nil, nil, err
		}
	}
	packagePaths := make([]string, 0, len(module.Packages))
	for packagePath := range module.Packages {
		packagePaths = append(packagePaths, packagePath)
	}
	sort.Strings(packagePaths)
	for _, packagePath := range packagePaths {
		if err := catalog.RegisterPackage(typecatalog.TypeOriginPackage, module.Packages[packagePath]); err != nil {
			return nil, nil, fmt.Errorf("index package authority %q: %w", packagePath, err)
		}
	}
	routesByPackage := map[string][]*bootstrap.RouteSource{}
	for _, route := range routes {
		routesByPackage[route.PackagePath] = append(routesByPackage[route.PackagePath], route)
	}
	packagePaths = packagePaths[:0]
	for packagePath := range routesByPackage {
		packagePaths = append(packagePaths, packagePath)
	}
	sort.Strings(packagePaths)
	var result []*spec.Component
	for _, packagePath := range packagePaths {
		packageRoutes := routesByPackage[packagePath]
		resolution := &typecatalog.ResolutionContext{DefaultPackage: packagePath, PackagePath: packagePath, PackageName: packageRoutes[0].PackageName, PackageDir: packageRoutes[0].Dir}
		seen := map[string]bool{}
		for _, route := range packageRoutes {
			for _, imported := range route.Imports {
				key := imported.Alias + "\x00" + imported.Package
				if seen[key] {
					continue
				}
				seen[key] = true
				resolution.Imports = append(resolution.Imports, typecatalog.PackageImport{Alias: imported.Alias, Package: imported.Package})
			}
		}
		resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, resolution)
		if err != nil {
			return nil, nil, err
		}
		groups, err := bootstrap.GroupPackageComponentSources(packageRoutes, resolver)
		if err != nil {
			return nil, nil, err
		}
		for _, group := range groups {
			component, err := group.ResolveDescriptors(resolver)
			if err != nil {
				return nil, nil, err
			}
			result = append(result, component)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key.String() < result[j].Key.String() })
	dependencies := map[string][]Source{}
	for packagePath := range routesByPackage {
		pkg := module.Packages[packagePath]
		if pkg == nil {
			continue
		}
		seenDependencies := map[string]bool{packagePath: true}
		var visit func(*synthetic.Package) error
		visit = func(candidate *synthetic.Package) error {
			if candidate == nil || seenDependencies[candidate.PkgPath] {
				return nil
			}
			seenDependencies[candidate.PkgPath] = true
			sources, sourceErr := packageGoSources(workspace, candidate.PkgPath)
			if sourceErr != nil {
				return sourceErr
			}
			dependencies[packagePath] = append(dependencies[packagePath], sources...)
			for _, nested := range candidate.Dependencies {
				if err := visit(nested); err != nil {
					return err
				}
			}
			return nil
		}
		for _, dependency := range pkg.Dependencies {
			if err := visit(dependency); err != nil {
				return nil, nil, err
			}
		}
	}
	return result, dependencies, nil
}

func packageGoSources(workspace *xmodule.Workspace, packagePath string) ([]Source, error) {
	location, err := workspace.Package(packagePath)
	if err != nil || location == nil {
		return nil, err
	}
	entries, err := os.ReadDir(location.Dir)
	if err != nil {
		return nil, err
	}
	var result []Source
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(location.Dir, entry.Name())
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		digest := sha256.Sum256(content)
		result = append(result, Source{Kind: SourceGo, Path: path, Dir: location.Dir, PackagePath: packagePath, Fingerprint: hex.EncodeToString(digest[:])})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Path < result[j].Path })
	return result, nil
}

type selectedFile struct {
	source Source
	kind   SourceKind
	file   xmodule.File
}

func scanSelectedFiles(ctx context.Context, workspace *xmodule.Workspace, include, exclude []string) ([]selectedFile, map[string][]Source, error) {
	if len(include) == 0 {
		return nil, nil, fmt.Errorf("bootstrap package include patterns are required")
	}
	var files []xmodule.File
	if err := workspace.Walk(ctx, include, exclude, func(file xmodule.File) error {
		if strings.HasSuffix(strings.ToLower(file.Path), "_test.go") {
			return nil
		}
		if kind := sourceKind(file.Path); kind != SourceGo && kind != SourceDQL {
			return nil
		}
		files = append(files, file)
		return nil
	}); err != nil {
		return nil, nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	selected := make([]selectedFile, 0, len(files))
	byPackage := map[string][]Source{}
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		kind := sourceKind(file.Path)
		if kind == "" {
			kind = SourceResource
		}
		content, err := os.ReadFile(file.Path)
		if err != nil {
			return nil, nil, err
		}
		digest := sha256.Sum256(content)
		source := Source{Kind: kind, Path: filepath.Clean(file.Path), Dir: filepath.Clean(file.Dir), PackagePath: file.ImportPath, Fingerprint: hex.EncodeToString(digest[:])}
		selected = append(selected, selectedFile{source: source, kind: kind, file: file})
		if kind == SourceGo || kind == SourceDQL {
			byPackage[file.ImportPath] = append(byPackage[file.ImportPath], source)
		}
	}
	return selected, byPackage, nil
}

func sourceKind(path string) SourceKind {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".go":
		return SourceGo
	case ".dql", ".sql":
		return SourceDQL
	default:
		return SourceResource
	}
}

func cloneRoutes(routes []*spec.Route) []*spec.Route {
	result := make([]*spec.Route, 0, len(routes))
	for _, endpoint := range routes {
		if endpoint != nil {
			result = append(result, endpoint.Clone())
		}
	}
	return result
}

func selectionIdentity(config Config) string {
	values := []string{formatVersion, filepath.Clean(config.BaseDir)}
	modules := append([]string(nil), config.ModuleDirs...)
	for index := range modules {
		modules[index] = filepath.Clean(modules[index])
	}
	sort.Strings(modules)
	include := append([]string(nil), config.Include...)
	exclude := append([]string(nil), config.Exclude...)
	sort.Strings(include)
	sort.Strings(exclude)
	values = append(values, modules...)
	values = append(values, "include")
	values = append(values, include...)
	values = append(values, "exclude")
	values = append(values, exclude...)
	return digestStrings(values...)
}

func digestSources(sources []Source) string {
	values := make([]string, 0, len(sources)*4)
	for _, source := range sources {
		values = append(values, string(source.Kind), source.PackagePath, source.Path, source.Fingerprint)
	}
	return digestStrings(values...)
}

func snapshotFingerprint(selection string, entries []*Entry) string {
	values := []string{selection}
	for _, entry := range entries {
		values = append(values, entry.Component.Key.String(), entry.Fingerprint)
	}
	return digestStrings(values...)
}

func digestStrings(values ...string) string {
	hash := sha256.New()
	for _, value := range values {
		hash.Write([]byte{0})
		hash.Write([]byte(value))
	}
	return hex.EncodeToString(hash.Sum(nil))
}
