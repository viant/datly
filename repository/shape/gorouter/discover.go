package gorouter

import (
	"bufio"
	"context"
	"fmt"
	"go/ast"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/componenttag"
	"github.com/viant/datly/view/extension"
	tagtags "github.com/viant/tagly/tags"
	"github.com/viant/x"
	"github.com/viant/xreflect"
	"golang.org/x/tools/go/packages"
)

// Discover scans Go packages for router holders and returns one route source per component-tagged field.
func Discover(ctx context.Context, baseDir string, include, exclude []string) ([]*RouteSource, error) {
	baseDir = strings.TrimSpace(baseDir)
	if baseDir == "" {
		return nil, fmt.Errorf("go router discovery: base dir was empty")
	}
	if len(include) == 0 {
		return nil, fmt.Errorf("go router discovery: include package patterns were empty")
	}
	include, err := expandPackagePatterns(ctx, baseDir, include)
	if err != nil {
		return nil, err
	}
	if len(include) == 0 {
		return nil, fmt.Errorf("go router discovery: no packages matched include patterns")
	}
	loadCfg := &packages.Config{
		Context: ctx,
		Dir:     baseDir,
		Mode:    packages.NeedName | packages.NeedFiles | packages.NeedSyntax,
	}
	pkgs, err := packages.Load(loadCfg, include...)
	if err != nil {
		return nil, fmt.Errorf("go router discovery: failed to load packages: %w", err)
	}
	index, err := newPackageIndex(ctx, baseDir)
	if err != nil {
		return nil, err
	}
	var result []*RouteSource
	for _, pkg := range pkgs {
		if pkg == nil || pkg.PkgPath == "" || matchesPackagePatternList(pkg.PkgPath, exclude) {
			continue
		}
		dir := firstPackageDir(pkg)
		if dir == "" {
			continue
		}
		for i, file := range pkg.Syntax {
			if file == nil || i >= len(pkg.GoFiles) {
				continue
			}
			filePath := pkg.GoFiles[i]
			imports := importMap(file)
			discovered, err := index.routesInFile(pkg.PkgPath, pkg.Name, dir, filePath, file, imports)
			if err != nil {
				return nil, err
			}
			result = append(result, discovered...)
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].PackagePath == result[j].PackagePath {
			if result[i].FilePath == result[j].FilePath {
				return result[i].FieldName < result[j].FieldName
			}
			return result[i].FilePath < result[j].FilePath
		}
		return result[i].PackagePath < result[j].PackagePath
	})
	return result, nil
}

func expandPackagePatterns(ctx context.Context, baseDir string, patterns []string) ([]string, error) {
	unique := map[string]bool{}
	var result []string
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		expanded, err := expandPackagePattern(ctx, baseDir, pattern)
		if err != nil {
			return nil, err
		}
		for _, item := range expanded {
			item = strings.TrimSpace(item)
			if item == "" || unique[item] {
				continue
			}
			unique[item] = true
			result = append(result, item)
		}
	}
	sort.Strings(result)
	return result, nil
}

func expandPackagePattern(ctx context.Context, baseDir, pattern string) ([]string, error) {
	if !strings.HasSuffix(pattern, "/...") {
		return []string{pattern}, nil
	}
	moduleDir, modulePath, err := locateModule(baseDir)
	if err == nil {
		if packages, ok, expandErr := expandModuleWildcardPattern(moduleDir, modulePath, pattern); expandErr != nil {
			return nil, expandErr
		} else if ok {
			return packages, nil
		}
	}
	cfg := &packages.Config{
		Context: ctx,
		Dir:     baseDir,
		Mode:    packages.NeedName | packages.NeedFiles,
	}
	pkgs, err := packages.Load(cfg, pattern)
	if err != nil {
		return nil, fmt.Errorf("go router discovery: failed to expand package pattern %s: %w", pattern, err)
	}
	var result []string
	unique := map[string]bool{}
	for _, pkg := range pkgs {
		if pkg == nil || pkg.PkgPath == "" || unique[pkg.PkgPath] {
			continue
		}
		unique[pkg.PkgPath] = true
		result = append(result, pkg.PkgPath)
	}
	sort.Strings(result)
	return result, nil
}

func expandModuleWildcardPattern(moduleDir, modulePath, pattern string) ([]string, bool, error) {
	prefix := strings.TrimSuffix(strings.TrimSpace(pattern), "/...")
	if prefix == "" || moduleDir == "" || modulePath == "" {
		return nil, false, nil
	}
	if prefix != modulePath && !strings.HasPrefix(prefix, modulePath+"/") {
		return nil, false, nil
	}
	rel := strings.TrimPrefix(prefix, modulePath)
	rel = strings.TrimPrefix(rel, "/")
	rootDir := moduleDir
	if rel != "" {
		rootDir = filepath.Join(moduleDir, filepath.FromSlash(rel))
	}
	info, err := os.Stat(rootDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, true, nil
		}
		return nil, true, err
	}
	if !info.IsDir() {
		return nil, true, nil
	}
	unique := map[string]bool{}
	var result []string
	err = filepath.WalkDir(rootDir, func(current string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() {
			return nil
		}
		name := d.Name()
		if strings.HasPrefix(name, ".") || name == "testdata" {
			if current != rootDir {
				return filepath.SkipDir
			}
			return nil
		}
		hasGo, err := containsPackageGoFiles(current)
		if err != nil {
			return err
		}
		if !hasGo {
			return nil
		}
		relDir, err := filepath.Rel(moduleDir, current)
		if err != nil {
			return err
		}
		importPath := modulePath
		if relDir != "." {
			importPath += "/" + filepath.ToSlash(relDir)
		}
		if !unique[importPath] {
			unique[importPath] = true
			result = append(result, importPath)
		}
		return nil
	})
	if err != nil {
		return nil, true, err
	}
	sort.Strings(result)
	return result, true, nil
}

func containsPackageGoFiles(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		return true, nil
	}
	return false, nil
}

func locateModule(baseDir string) (string, string, error) {
	dir := filepath.Clean(baseDir)
	for {
		goModPath := filepath.Join(dir, "go.mod")
		data, err := os.ReadFile(goModPath)
		if err == nil {
			modulePath := parseModulePath(data)
			if modulePath == "" {
				return "", "", fmt.Errorf("go router discovery: module path not found in %s", goModPath)
			}
			return dir, modulePath, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", "", fmt.Errorf("go router discovery: go.mod not found from %s", baseDir)
}

func parseModulePath(data []byte) string {
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}
		if strings.HasPrefix(line, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "module "))
		}
	}
	return ""
}

type packageIndex struct {
	ctx      context.Context
	baseDir  string
	pkgs     map[string]*packageMeta
	dirTypes map[string]*xreflect.DirTypes
}

type packageMeta struct {
	importPath string
	name       string
	dir        string
}

func newPackageIndex(ctx context.Context, baseDir string) (*packageIndex, error) {
	return &packageIndex{
		ctx:      ctx,
		baseDir:  baseDir,
		pkgs:     map[string]*packageMeta{},
		dirTypes: map[string]*xreflect.DirTypes{},
	}, nil
}

func (p *packageIndex) routesInFile(pkgPath, pkgName, pkgDir, filePath string, file *ast.File, imports map[string]string) ([]*RouteSource, error) {
	var result []*RouteSource
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}
		for _, spec := range gen.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				continue
			}
			for _, field := range structType.Fields.List {
				route, err := p.routeFromField(pkgPath, pkgName, pkgDir, filePath, field, imports)
				if err != nil {
					return nil, err
				}
				if route != nil {
					result = append(result, route)
				}
			}
		}
	}
	return result, nil
}

func (p *packageIndex) routeFromField(pkgPath, pkgName, pkgDir, filePath string, field *ast.Field, imports map[string]string) (*RouteSource, error) {
	if field == nil || field.Tag == nil || len(field.Names) == 0 {
		return nil, nil
	}
	tagLiteral, err := strconv.Unquote(field.Tag.Value)
	if err != nil {
		return nil, fmt.Errorf("go router discovery: invalid struct tag in %s: %w", filePath, err)
	}
	parsed, err := componenttag.Parse(reflect.StructTag(tagLiteral))
	if err != nil {
		return nil, fmt.Errorf("go router discovery: invalid component tag in %s: %w", filePath, err)
	}
	if parsed == nil || parsed.Component == nil {
		return nil, nil
	}
	fieldName := strings.TrimSpace(field.Names[0].Name)
	if fieldName == "" {
		return nil, nil
	}
	inputRef := normalizeTypeRef(strings.TrimSpace(parsed.Component.Input), pkgPath)
	outputRef := normalizeTypeRef(strings.TrimSpace(parsed.Component.Output), pkgPath)
	viewRef := normalizeTypeRef(strings.TrimSpace(parsed.Component.View), pkgPath)
	handlerRef := normalizeTypeRef(strings.TrimSpace(parsed.Component.Handler), pkgPath)
	sourceURL := strings.TrimSpace(parsed.Component.Source)
	summaryURL := strings.TrimSpace(parsed.Component.Summary)
	if inputRef == "" || outputRef == "" {
		inferredInput, inferredOutput := inferComponentTypeRefs(field.Type, pkgPath, imports)
		if inputRef == "" {
			inputRef = inferredInput
		}
		if outputRef == "" {
			outputRef = inferredOutput
		}
	}
	if inputRef == "" && outputRef == "" {
		if viewRef == "" && sourceURL == "" {
			return nil, nil
		}
	}
	if inputRef == "" && outputRef == "" && viewRef == "" {
		return nil, nil
	}
	registry := x.NewRegistry()
	tagCopy := *parsed.Component
	if inputRef != "" {
		rType, err := p.resolveType(inputRef)
		if err != nil {
			return nil, fmt.Errorf("go router discovery: failed to resolve %s input %s: %w", fieldName, inputRef, err)
		}
		registerType(registry, inputRef, rType)
		tagCopy.Input = inputRef
	}
	if outputRef != "" {
		rType, err := p.resolveType(outputRef)
		if err != nil {
			return nil, fmt.Errorf("go router discovery: failed to resolve %s output %s: %w", fieldName, outputRef, err)
		}
		registerType(registry, outputRef, rType)
		tagCopy.Output = outputRef
	}
	if viewRef != "" {
		rType, err := p.resolveType(viewRef)
		if err != nil {
			return nil, fmt.Errorf("go router discovery: failed to resolve %s view %s: %w", fieldName, viewRef, err)
		}
		registerType(registry, viewRef, rType)
		tagCopy.View = viewRef
	}
	if handlerRef != "" {
		rType, err := p.resolveType(handlerRef)
		if err != nil {
			return nil, fmt.Errorf("go router discovery: failed to resolve %s handler %s: %w", fieldName, handlerRef, err)
		}
		registerType(registry, handlerRef, rType)
		tagCopy.Handler = handlerRef
	}
	if sourceURL != "" {
		tagCopy.Source = sourceURL
	}
	if summaryURL != "" {
		tagCopy.Summary = summaryURL
	}
	componentTag := tagCopy.Tag()
	rootType := reflect.StructOf([]reflect.StructField{{
		Name: exportName(fieldName),
		Type: reflect.TypeOf(struct{}{}),
		Tag:  reflect.StructTag(tagtags.Tags{componentTag}.Stringify()),
	}})
	name := strings.TrimSpace(tagCopy.Name)
	if name == "" {
		name = exportName(fieldName)
	}
	return &RouteSource{
		Name:        name,
		FieldName:   fieldName,
		FilePath:    filePath,
		PackageName: pkgName,
		PackagePath: pkgPath,
		PackageDir:  pkgDir,
		RoutePath:   strings.TrimSpace(tagCopy.Path),
		Method:      strings.TrimSpace(tagCopy.Method),
		Connector:   strings.TrimSpace(tagCopy.Connector),
		InputRef:    inputRef,
		OutputRef:   outputRef,
		ViewRef:     viewRef,
		SourceURL:   sourceURL,
		SummaryURL:  summaryURL,
		Source: &shape.Source{
			Name:         name,
			Path:         filePath,
			Type:         rootType,
			TypeRegistry: registry,
		},
	}, nil
}

func (p *packageIndex) resolveType(ref string) (reflect.Type, error) {
	pkgPath, typeName := splitTypeRef(ref)
	if pkgPath == "" || typeName == "" {
		return nil, fmt.Errorf("invalid type reference %q", ref)
	}
	if extension.Config != nil && extension.Config.Types != nil {
		if linked, err := extension.Config.Types.Lookup(typeName, xreflect.WithPackage(pkgPath)); err == nil && linked != nil {
			return linked, nil
		}
	}
	meta, err := p.packageMeta(pkgPath)
	if err != nil {
		return nil, err
	}
	dirTypes, err := p.dirTypesFor(meta.dir)
	if err != nil {
		return nil, err
	}
	rType, err := dirTypes.Type(typeName)
	if err != nil {
		return nil, err
	}
	return rType, nil
}

func (p *packageIndex) packageMeta(importPath string) (*packageMeta, error) {
	if meta, ok := p.pkgs[importPath]; ok {
		return meta, nil
	}
	cfg := &packages.Config{
		Context: p.ctx,
		Dir:     p.baseDir,
		Mode:    packages.NeedName | packages.NeedFiles,
	}
	pkgs, err := packages.Load(cfg, importPath)
	if err != nil {
		return nil, fmt.Errorf("go router discovery: failed to load package %s: %w", importPath, err)
	}
	for _, pkg := range pkgs {
		if pkg == nil || pkg.PkgPath == "" {
			continue
		}
		dir := firstPackageDir(pkg)
		if dir == "" {
			continue
		}
		meta := &packageMeta{importPath: pkg.PkgPath, name: pkg.Name, dir: dir}
		p.pkgs[pkg.PkgPath] = meta
		if pkg.PkgPath == importPath {
			return meta, nil
		}
	}
	return nil, fmt.Errorf("go router discovery: package %s not resolved", importPath)
}

func (p *packageIndex) dirTypesFor(dir string) (*xreflect.DirTypes, error) {
	if cached, ok := p.dirTypes[dir]; ok {
		return cached, nil
	}
	options := []xreflect.Option{}
	if extension.Config != nil && extension.Config.Types != nil {
		options = append(options, xreflect.WithTypeLookup(extension.Config.Types.Lookup))
	}
	parsed, err := xreflect.ParseTypes(dir, options...)
	if err != nil {
		return nil, fmt.Errorf("go router discovery: failed to parse package dir %s: %w", dir, err)
	}
	p.dirTypes[dir] = parsed
	return parsed, nil
}

func inferComponentTypeRefs(expr ast.Expr, pkgPath string, imports map[string]string) (string, string) {
	args := componentTypeArgs(expr)
	if len(args) < 2 {
		return "", ""
	}
	return qualifyTypeExpr(args[0], pkgPath, imports), qualifyTypeExpr(args[1], pkgPath, imports)
}

func componentTypeArgs(expr ast.Expr) []ast.Expr {
	switch actual := expr.(type) {
	case *ast.IndexListExpr:
		if !isComponentSelector(actual.X) {
			return nil
		}
		return actual.Indices
	case *ast.IndexExpr:
		if !isComponentSelector(actual.X) {
			return nil
		}
		return []ast.Expr{actual.Index}
	default:
		return nil
	}
}

func isComponentSelector(expr ast.Expr) bool {
	switch actual := expr.(type) {
	case *ast.SelectorExpr:
		return actual.Sel != nil && actual.Sel.Name == "Component"
	case *ast.Ident:
		return actual.Name == "Component"
	default:
		return false
	}
}

func qualifyTypeExpr(expr ast.Expr, pkgPath string, imports map[string]string) string {
	switch actual := expr.(type) {
	case *ast.Ident:
		if pkgPath == "" || actual.Name == "" {
			return ""
		}
		return pkgPath + "." + actual.Name
	case *ast.SelectorExpr:
		ident, ok := actual.X.(*ast.Ident)
		if !ok || ident.Name == "" || actual.Sel == nil || actual.Sel.Name == "" {
			return ""
		}
		importPath := imports[ident.Name]
		if importPath == "" {
			return ""
		}
		return importPath + "." + actual.Sel.Name
	default:
		return ""
	}
}

func importMap(file *ast.File) map[string]string {
	result := map[string]string{}
	if file == nil {
		return result
	}
	for _, item := range file.Imports {
		if item == nil || item.Path == nil {
			continue
		}
		importPath, err := strconv.Unquote(item.Path.Value)
		if err != nil || importPath == "" {
			continue
		}
		alias := path.Base(importPath)
		if item.Name != nil && strings.TrimSpace(item.Name.Name) != "" {
			alias = strings.TrimSpace(item.Name.Name)
		}
		result[alias] = importPath
	}
	return result
}

func splitTypeRef(ref string) (string, string) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", ""
	}
	index := strings.LastIndex(ref, ".")
	if index == -1 || index+1 >= len(ref) {
		return "", ""
	}
	return strings.TrimSpace(ref[:index]), strings.TrimSpace(ref[index+1:])
}

func normalizeTypeRef(ref, pkgPath string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ""
	}
	if strings.Contains(ref, ".") {
		return ref
	}
	if pkgPath == "" {
		return ref
	}
	return pkgPath + "." + ref
}

func registerType(registry *x.Registry, ref string, rType reflect.Type) {
	if registry == nil || rType == nil {
		return
	}
	pkgPath, typeName := splitTypeRef(ref)
	registry.Register(x.NewType(rType, x.WithPkgPath(pkgPath), x.WithName(typeName)))
}

func firstPackageDir(pkg *packages.Package) string {
	if pkg == nil {
		return ""
	}
	for _, filePath := range pkg.GoFiles {
		if filePath == "" {
			continue
		}
		return filepath.Dir(filePath)
	}
	return ""
}

func exportName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "Route"
	}
	runes := []rune(name)
	if len(runes) == 0 {
		return "Route"
	}
	if runes[0] >= 'a' && runes[0] <= 'z' {
		runes[0] = runes[0] - 32
	}
	return string(runes)
}

func matchesPackagePatternList(pkg string, patterns []string) bool {
	for _, pattern := range patterns {
		if matchesPackagePattern(pkg, pattern) {
			return true
		}
	}
	return false
}

func matchesPackagePattern(pkg, pattern string) bool {
	pkg = strings.TrimSpace(pkg)
	pattern = strings.TrimSpace(pattern)
	if pkg == "" || pattern == "" {
		return false
	}
	if strings.HasSuffix(pattern, "/...") {
		prefix := strings.TrimSuffix(pattern, "/...")
		return pkg == prefix || strings.HasPrefix(pkg, prefix+"/")
	}
	return pkg == pattern
}
