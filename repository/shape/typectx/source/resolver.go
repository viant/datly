package source

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"golang.org/x/mod/modfile"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type Config struct {
	ProjectDir         string
	AllowedSourceRoots []string
	UseGoModuleResolve bool
	UseGOPATHFallback  bool
}

type Resolver struct {
	projectDir   string
	modulePath   string
	replacements map[string]string
	roots        []string
	useModule    bool
	useGOPATH    bool
}

func New(cfg Config) (*Resolver, error) {
	projectDir := strings.TrimSpace(cfg.ProjectDir)
	if projectDir == "" {
		return nil, fmt.Errorf("typectx source: project dir was empty")
	}
	projectDir, err := filepath.Abs(projectDir)
	if err != nil {
		return nil, err
	}
	modulePath, replacements := loadModuleConfig(projectDir)
	roots := NormalizeRoots(projectDir, cfg.AllowedSourceRoots)
	return &Resolver{
		projectDir:   projectDir,
		modulePath:   modulePath,
		replacements: replacements,
		roots:        roots,
		useModule:    cfg.UseGoModuleResolve,
		useGOPATH:    cfg.UseGOPATHFallback,
	}, nil
}

func (r *Resolver) ResolvePackageDir(importPath string) (string, error) {
	importPath = strings.TrimSpace(importPath)
	if importPath == "" {
		return "", fmt.Errorf("typectx source: empty import path")
	}
	if r.useModule {
		if resolved := r.resolveReplace(importPath); resolved != "" {
			return filepath.Clean(resolved), nil
		}
		if resolved := r.resolveProjectModule(importPath); resolved != "" {
			return filepath.Clean(resolved), nil
		}
		if resolved := r.resolveModuleCache(importPath); resolved != "" {
			return filepath.Clean(resolved), nil
		}
	}
	if r.useGOPATH {
		if resolved := resolveGOPATH(importPath); resolved != "" {
			return filepath.Clean(resolved), nil
		}
	}
	return "", fmt.Errorf("typectx source: package %s not resolved", importPath)
}

func (r *Resolver) ResolveTypeFile(importPath, typeName string) (string, error) {
	dir, err := r.ResolvePackageDir(importPath)
	if err != nil {
		return "", err
	}
	ok, err := IsWithinAnyRoot(dir, r.roots)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("typectx source: package dir %s outside trusted roots", dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	fset := token.NewFileSet()
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		filePath := filepath.Join(dir, name)
		parsed, parseErr := parser.ParseFile(fset, filePath, nil, parser.PackageClauseOnly|parser.ParseComments)
		if parseErr != nil || parsed == nil {
			continue
		}
		// Reparse full declaration only when package clause parsing succeeds.
		parsed, parseErr = parser.ParseFile(fset, filePath, nil, 0)
		if parseErr != nil || parsed == nil {
			continue
		}
		for _, decl := range parsed.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if ok && ts.Name != nil && ts.Name.Name == typeName {
					return filePath, nil
				}
			}
		}
	}
	return "", fmt.Errorf("typectx source: type %s not found in %s", typeName, importPath)
}

func (r *Resolver) Roots() []string {
	return append([]string(nil), r.roots...)
}

func (r *Resolver) resolveReplace(importPath string) string {
	oldPaths := make([]string, 0, len(r.replacements))
	for old := range r.replacements {
		oldPaths = append(oldPaths, old)
	}
	sort.SliceStable(oldPaths, func(i, j int) bool { return len(oldPaths[i]) > len(oldPaths[j]) })
	for _, old := range oldPaths {
		if importPath != old && !strings.HasPrefix(importPath, old+"/") {
			continue
		}
		mapped := r.replacements[old]
		suffix := strings.TrimPrefix(importPath, old)
		suffix = strings.TrimPrefix(suffix, "/")
		if suffix == "" {
			return mapped
		}
		return filepath.Join(mapped, filepath.FromSlash(suffix))
	}
	return ""
}

func (r *Resolver) resolveProjectModule(importPath string) string {
	if r.modulePath == "" {
		return ""
	}
	if importPath != r.modulePath && !strings.HasPrefix(importPath, r.modulePath+"/") {
		return ""
	}
	suffix := strings.TrimPrefix(importPath, r.modulePath)
	suffix = strings.TrimPrefix(suffix, "/")
	if suffix == "" {
		return r.projectDir
	}
	return filepath.Join(r.projectDir, filepath.FromSlash(suffix))
}

func (r *Resolver) resolveModuleCache(importPath string) string {
	modCache := strings.TrimSpace(os.Getenv("GOMODCACHE"))
	if modCache == "" {
		if out, err := os.UserCacheDir(); err == nil && out != "" {
			modCache = filepath.Join(filepath.Dir(out), "pkg", "mod")
		}
	}
	if modCache == "" {
		return ""
	}
	pattern := filepath.Join(modCache, filepath.FromSlash(importPath)+"@*")
	matches, _ := filepath.Glob(pattern)
	if len(matches) == 0 {
		return ""
	}
	sort.Strings(matches)
	return matches[len(matches)-1]
}

func resolveGOPATH(importPath string) string {
	gopath := strings.TrimSpace(os.Getenv("GOPATH"))
	if gopath == "" {
		gopath = strings.TrimSpace(build.Default.GOPATH)
	}
	if gopath == "" {
		return ""
	}
	for _, root := range filepath.SplitList(gopath) {
		candidate := filepath.Join(root, "src", filepath.FromSlash(importPath))
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate
		}
	}
	return ""
}

func loadModuleConfig(projectDir string) (string, map[string]string) {
	result := map[string]string{}
	goModPath := filepath.Join(projectDir, "go.mod")
	data, err := os.ReadFile(goModPath)
	if err != nil {
		return "", result
	}
	parsed, err := modfile.Parse(goModPath, data, nil)
	if err != nil || parsed == nil {
		return "", result
	}
	modulePath := ""
	if parsed.Module != nil {
		modulePath = strings.TrimSpace(parsed.Module.Mod.Path)
	}
	for _, replace := range parsed.Replace {
		if replace == nil {
			continue
		}
		oldPath := strings.TrimSpace(replace.Old.Path)
		newPath := strings.TrimSpace(replace.New.Path)
		if oldPath == "" || newPath == "" || replace.New.Version != "" {
			continue
		}
		if !filepath.IsAbs(newPath) {
			newPath = filepath.Join(projectDir, newPath)
		}
		result[oldPath] = filepath.Clean(newPath)
	}
	return modulePath, result
}

func NormalizeRoots(projectDir string, allowed []string) []string {
	seen := map[string]bool{}
	var result []string
	appendRoot := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		if !filepath.IsAbs(value) {
			value = filepath.Join(projectDir, value)
		}
		value = filepath.Clean(value)
		if seen[value] {
			return
		}
		seen[value] = true
		result = append(result, value)
	}
	appendRoot(projectDir)
	for _, item := range allowed {
		appendRoot(item)
	}
	sort.Strings(result)
	return result
}

func IsWithinAnyRoot(candidate string, roots []string) (bool, error) {
	candidate, err := filepath.Abs(candidate)
	if err != nil {
		return false, err
	}
	candidate = filepath.Clean(candidate)
	for _, root := range roots {
		root = filepath.Clean(root)
		rel, err := filepath.Rel(root, candidate)
		if err != nil {
			return false, err
		}
		if rel == "." {
			return true, nil
		}
		rel = filepath.ToSlash(rel)
		if !strings.HasPrefix(rel, "../") {
			return true, nil
		}
	}
	return false, nil
}
