package xgen

import (
	"bufio"
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func resolvePaths(projectDir, packageDir string) (string, string, error) {
	if strings.TrimSpace(projectDir) == "" {
		return "", "", fmt.Errorf("shape xgen: project dir was empty")
	}
	projectDir = filepath.Clean(projectDir)
	if strings.TrimSpace(packageDir) == "" {
		packageDir = projectDir
	} else if !filepath.IsAbs(packageDir) {
		packageDir = filepath.Join(projectDir, packageDir)
	}
	packageDir = filepath.Clean(packageDir)
	return projectDir, packageDir, nil
}

func resolvePackageName(name string, packageDir string) string {
	name = strings.TrimSpace(name)
	if name != "" {
		return name
	}
	base := filepath.Base(packageDir)
	if base == "." || base == string(filepath.Separator) || base == "" {
		return "generated"
	}
	return sanitizePkg(base)
}

func resolvePackagePath(packagePath, projectDir, packageDir string) (string, error) {
	packagePath = strings.TrimSpace(packagePath)
	if packagePath != "" {
		return packagePath, nil
	}
	modulePath, err := readModulePath(filepath.Join(projectDir, "go.mod"))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(projectDir, packageDir)
	if err != nil {
		return "", err
	}
	rel = filepath.ToSlash(rel)
	if rel == "." {
		return modulePath, nil
	}
	return strings.TrimRight(modulePath, "/") + "/" + strings.TrimLeft(rel, "/"), nil
}

func readModulePath(goModPath string) (string, error) {
	file, err := os.Open(goModPath)
	if err != nil {
		return "", fmt.Errorf("shape xgen: open go.mod failed: %w", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "module ") {
			continue
		}
		modulePath := strings.TrimSpace(strings.TrimPrefix(line, "module "))
		if modulePath != "" {
			return modulePath, nil
		}
	}
	if err = scanner.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("shape xgen: module path not found in %s", goModPath)
}

func sanitizePkg(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" {
		return "generated"
	}
	var out strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			out.WriteRune(r)
		}
	}
	if out.Len() == 0 {
		return "generated"
	}
	result := out.String()
	if result[0] >= '0' && result[0] <= '9' {
		return "p" + result
	}
	return result
}

func writeAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, ".tmp-shape-xgen-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	cleanup := func() {
		_ = os.Remove(tempPath)
	}
	if _, err = temp.Write(data); err != nil {
		_ = temp.Close()
		cleanup()
		return err
	}
	if err = temp.Chmod(perm); err != nil {
		_ = temp.Close()
		cleanup()
		return err
	}
	if err = temp.Close(); err != nil {
		cleanup()
		return err
	}
	if err = os.Rename(tempPath, path); err != nil {
		cleanup()
		return err
	}
	return nil
}

func fileExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err == nil {
		return !info.IsDir(), nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func mergeGeneratedShapes(dest string, generated []byte, typeNames []string) ([]byte, error) {
	existing, err := os.ReadFile(dest)
	if err != nil {
		return nil, err
	}
	if len(existing) == 0 {
		return generated, nil
	}
	if len(typeNames) == 0 {
		return existing, nil
	}

	fset := token.NewFileSet()
	existingFile, err := parser.ParseFile(fset, dest, existing, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("shape xgen: parse existing file failed: %w", err)
	}
	generatedFile, err := parser.ParseFile(token.NewFileSet(), "", generated, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("shape xgen: parse generated file failed: %w", err)
	}
	typeNameSet := map[string]bool{}
	for _, name := range typeNames {
		typeNameSet[name] = true
	}

	shapeDecls := generatedShapeDecls(generatedFile, typeNameSet)
	if len(shapeDecls) == 0 {
		return generated, nil
	}
	mergedImports := mergeImports(existingFile.Imports, generatedFile.Imports)

	newDecls := make([]ast.Decl, 0, len(existingFile.Decls)+len(shapeDecls)+1)
	if len(mergedImports) > 0 {
		newDecls = append(newDecls, &ast.GenDecl{
			Tok:   token.IMPORT,
			Specs: mergedImports,
		})
	}

	for _, decl := range existingFile.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok {
			newDecls = append(newDecls, decl)
			continue
		}
		switch gen.Tok {
		case token.IMPORT:
			continue
		case token.TYPE:
			filtered := make([]ast.Spec, 0, len(gen.Specs))
			for _, spec := range gen.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if !ok || !typeNameSet[ts.Name.Name] {
					filtered = append(filtered, spec)
				}
			}
			if len(filtered) == 0 {
				continue
			}
			gen.Specs = filtered
			newDecls = append(newDecls, gen)
		default:
			newDecls = append(newDecls, decl)
		}
	}
	newDecls = append(newDecls, shapeDecls...)
	existingFile.Decls = newDecls
	existingFile.Imports = importSpecsToImportNodes(mergedImports)

	var out bytes.Buffer
	if err = format.Node(&out, fset, existingFile); err != nil {
		return nil, fmt.Errorf("shape xgen: format merged file failed: %w", err)
	}
	return out.Bytes(), nil
}

func generatedShapeDecls(file *ast.File, typeNameSet map[string]bool) []ast.Decl {
	var result []ast.Decl
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}
		filtered := make([]ast.Spec, 0, len(gen.Specs))
		for _, spec := range gen.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok || !typeNameSet[ts.Name.Name] {
				continue
			}
			filtered = append(filtered, spec)
		}
		if len(filtered) == 0 {
			continue
		}
		result = append(result, &ast.GenDecl{
			Tok:   token.TYPE,
			Specs: filtered,
		})
	}
	return result
}

func mergeImports(existing []*ast.ImportSpec, generated []*ast.ImportSpec) []ast.Spec {
	merged := map[string]*ast.ImportSpec{}
	add := func(item *ast.ImportSpec) {
		if item == nil || item.Path == nil {
			return
		}
		key := item.Path.Value + "|" + importAlias(item)
		if _, ok := merged[key]; ok {
			return
		}
		merged[key] = item
	}
	for _, item := range existing {
		add(item)
	}
	for _, item := range generated {
		add(item)
	}
	keys := make([]string, 0, len(merged))
	for key := range merged {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]ast.Spec, 0, len(keys))
	for _, key := range keys {
		result = append(result, merged[key])
	}
	return result
}

func importAlias(item *ast.ImportSpec) string {
	if item == nil || item.Name == nil {
		return ""
	}
	return item.Name.Name
}

func importSpecsToImportNodes(specs []ast.Spec) []*ast.ImportSpec {
	result := make([]*ast.ImportSpec, 0, len(specs))
	for _, spec := range specs {
		if item, ok := spec.(*ast.ImportSpec); ok {
			result = append(result, item)
		}
	}
	return result
}
