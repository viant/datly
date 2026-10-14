package generate

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type entityMethodOwnership struct {
	expected                   map[string]EntityMethod
	claimed                    map[string]string
	generated                  map[string]string
	imports                    map[string]string
	packageName                string
	allowAuthoredAccessorTypes bool
	convenienceAccessor        bool
}

func (p *scaffoldPersistence) preserveEntityMethods(target, existing string) error {
	if p.plan == nil || p.plan.EntitySupport == nil || len(p.plan.EntitySupport.Methods) == 0 {
		return nil
	}
	p.plan.accessorValidationNeeded = false
	p.plan.accessorSourceValidationNeeded = false
	p.plan.handlerGoFiles = nil
	proposed := map[string]bool{}
	for _, file := range p.files {
		relative, err := managedPath(target, file.Path)
		if err != nil {
			return err
		}
		proposed[relative] = true
	}
	index := -1
	for i, file := range p.files {
		relative, err := managedPath(target, file.Path)
		if err != nil {
			return err
		}
		if relative == p.plan.EntitySupport.Destination {
			index = i
			break
		}
	}
	if index < 0 {
		return fmt.Errorf("entity support artifact is missing")
	}
	file, err := parser.ParseFile(token.NewFileSet(), "entities.go", strings.TrimPrefix(p.files[index].Content, generatedHeader(p.owner)), parser.ParseComments)
	if err != nil {
		return err
	}
	imports, err := handlerImports(file, "")
	if err != nil {
		return err
	}
	policy := &entityMethodOwnership{expected: map[string]EntityMethod{}, claimed: map[string]string{}, generated: map[string]string{}, imports: imports, packageName: file.Name.Name, allowAuthoredAccessorTypes: (p.plan.ExternalHandler != nil && p.plan.ExternalHandler.GeneratedContracts && p.plan.ExternalHandler.Build != nil) || p.plan.accessorBuild != nil}
	for _, method := range p.plan.EntitySupport.Methods {
		policy.expected[method.Receiver+"."+method.Name] = method
	}
	for _, declaration := range file.Decls {
		method, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if key, _ := policy.methodKey(method); key != "" {
			policy.generated[key] = entityMethodSource(method)
		}
	}
	entries, err := os.ReadDir(existing)
	existingMissing := os.IsNotExist(err)
	if existingMissing {
		err = nil
	}
	if err != nil {
		return err
	}
	build := p.plan.accessorBuild
	if p.plan.ExternalHandler != nil && p.plan.ExternalHandler.Build != nil {
		build = p.plan.ExternalHandler.Build
	}
	if existingMissing && build == nil {
		return nil
	}
	if existingMissing && filepath.Clean(target) != filepath.Clean(existing) {
		if _, err := os.Stat(target); err == nil {
			return fmt.Errorf("accessor projection %q is missing", existing)
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	if existingMissing && p.plan.HookScaffold == nil && len(p.userFiles) == 0 {
		return nil // An empty initial projection has no authored ownership to select.
	}
	selected := map[string]bool{}
	var projection map[string][]byte
	if build != nil {
		projection = map[string][]byte{}
		if !existingMissing {
			projection, err = scaffoldBuildFiles(target, existing)
			if err != nil {
				return err
			}
		}
		for _, artifact := range append(append([]EmittedFile(nil), p.files...), p.userFiles...) {
			relative, err := managedPath(target, artifact.Path)
			if err != nil {
				return err
			}
			if projection[filepath.Join(target, relative)] != nil && !proposed[relative] {
				continue // Create-once source retains the current projected bytes.
			}
			projection[filepath.Join(target, relative)] = []byte(artifact.Content)
		}
		for relative := range p.renames {
			projection[filepath.Join(target, relative)] = nil
		}
		goFiles, err := build.GoFiles(p.plan.Package, projection)
		if err != nil {
			return err
		}
		for _, name := range goFiles {
			if projection[filepath.Join(target, name)] == nil {
				return fmt.Errorf("selected accessor source %q has no projected package content", name)
			}
			selected[name] = true
		}
		// Selecting generated source alone must retain ordinary split-package
		// generation. Authored method candidates (including excluded or invalid
		// source) still require the final compiler gate.
		validationNeeded := false
		for path, content := range projection {
			name := filepath.Base(path)
			if content == nil || filepath.Clean(filepath.Dir(path)) != filepath.Clean(target) || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || proposed[name] || p.renames[name] || generatedOwner(content) != "" {
				continue
			}
			if policy.hasCandidateSource(path, content) {
				validationNeeded = true
				break
			}
		}
		if p.plan.HookScaffold != nil && policy.hasCandidate(p.plan.HookScaffold.File) {
			validationNeeded = true
		}
		selection := *p.plan
		selection.handlerGoFiles = goFiles
		if len(entries) > 0 {
			entries, err = fs.ReadDir(selection.PackageFS(existing, "."), ".")
			if err != nil {
				return err
			}
		}
		p.plan.accessorSourceValidationNeeded = validationNeeded
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(existing, name)
		if proposed[name] || p.renames[name] {
			continue
		}
		content := projection[filepath.Join(target, name)]
		if build == nil {
			content, err = os.ReadFile(path)
			if err != nil {
				return err
			}
		}
		if generatedOwner(content) != "" {
			continue
		}
		source, err := parser.ParseFile(token.NewFileSet(), path, content, parser.ParseComments)
		if err != nil {
			return err
		}
		if err = policy.inspect(source, name); err != nil {
			return err
		}
	}
	if p.plan.HookScaffold != nil && (build == nil || selected[p.plan.HookScaffold.Destination]) {
		if _, err := os.Stat(filepath.Join(existing, p.plan.HookScaffold.Destination)); os.IsNotExist(err) {
			if err = policy.inspect(p.plan.HookScaffold.File, p.plan.HookScaffold.Destination); err != nil {
				return err
			}
		}
	}
	if policy.convenienceAccessor && p.plan.accessorBuild != nil {
		p.plan.accessorValidationNeeded = true
	}
	if len(policy.claimed) == 0 {
		return nil
	}
	declarations := file.Decls[:0]
	for _, declaration := range file.Decls {
		method, ok := declaration.(*ast.FuncDecl)
		if ok {
			key, _ := policy.methodKey(method)
			if policy.claimed[key] != "" {
				continue
			}
		}
		declarations = append(declarations, declaration)
	}
	file.Decls = declarations
	used := map[string]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		if selected, ok := node.(*ast.SelectorExpr); ok {
			if qualifier, ok := selected.X.(*ast.Ident); ok {
				used[qualifier.Name] = true
			}
		}
		return true
	})
	for _, declaration := range file.Decls {
		group, ok := declaration.(*ast.GenDecl)
		if !ok || group.Tok != token.IMPORT {
			continue
		}
		kept := group.Specs[:0]
		for _, item := range group.Specs {
			spec := item.(*ast.ImportSpec)
			if spec.Name == nil || spec.Name.Name == "_" || used[spec.Name.Name] {
				kept = append(kept, item)
			}
		}
		group.Specs = kept
	}
	var result bytes.Buffer
	if err = format.Node(&result, token.NewFileSet(), file); err != nil {
		return err
	}
	p.files[index].Content = generatedHeader(p.owner) + result.String() + "\n"
	return nil
}

func (p *entityMethodOwnership) methodKey(method *ast.FuncDecl) (string, bool) {
	if method == nil || method.Recv == nil || len(method.Recv.List) != 1 {
		return "", false
	}
	expression := method.Recv.List[0].Type
	pointer := false
	if ptr, ok := expression.(*ast.StarExpr); ok {
		pointer = true
		expression = ptr.X
	}
	name, ok := expression.(*ast.Ident)
	if !ok {
		return "", pointer
	}
	return name.Name + "." + method.Name.Name, pointer
}

func (p *entityMethodOwnership) hasCandidateSource(path string, content []byte) bool {
	source, err := parser.ParseFile(token.NewFileSet(), path, content, parser.AllErrors)
	return err != nil || p.hasCandidate(source)
}

func (p *entityMethodOwnership) hasCandidate(file *ast.File) bool {
	if file == nil {
		return false
	}
	for _, declaration := range file.Decls {
		method, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		key, _ := p.methodKey(method)
		if _, ok := p.expected[key]; ok {
			return true
		}
	}
	return false
}

func (p *entityMethodOwnership) inspect(file *ast.File, path string) error {
	if file == nil {
		return nil
	}
	imports, err := handlerImports(file, "")
	if err != nil {
		return err
	}
	for _, declaration := range file.Decls {
		method, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		key, pointer := p.methodKey(method)
		expected, ok := p.expected[key]
		if !ok {
			continue
		}
		if generated := p.generated[key]; generated != "" && generated == entityMethodSource(method) {
			continue
		}
		if file.Name.Name != p.packageName {
			return fmt.Errorf("authored accessor %s belongs to a different package", key)
		}
		if prior := p.claimed[key]; prior != "" {
			return fmt.Errorf("authored accessor %s is duplicated in %s and %s", key, prior, path)
		}
		if expected.Signature != "" {
			if !pointer {
				return fmt.Errorf("authored entity helper %s requires a pointer receiver", key)
			}
			wantedExpr, err := parser.ParseExpr(expected.Signature)
			if err != nil {
				return err
			}
			wanted, ok := wantedExpr.(*ast.FuncType)
			if !ok {
				return fmt.Errorf("entity helper %s expected signature is not a function", key)
			}
			for _, pair := range [][2]*ast.FieldList{{method.Type.Params, wanted.Params}, {method.Type.Results, wanted.Results}} {
				actualTypes, wantedTypes := expandedFieldTypes(pair[0]), expandedFieldTypes(pair[1])
				if len(actualTypes) != len(wantedTypes) {
					return fmt.Errorf("authored entity helper %s signature differs from %s", key, expected.Signature)
				}
				for index, actualType := range actualTypes {
					actual, err := canonicalType(actualType, imports, p.packageName)
					if err != nil {
						return err
					}
					wanted, err := canonicalType(wantedTypes[index], p.imports, p.packageName)
					if err != nil {
						return err
					}
					if actual != wanted {
						return fmt.Errorf("authored entity helper %s type %s differs from %s", key, actual, wanted)
					}
				}
			}
			p.claimed[key] = path
			continue
		}
		parameters, results := expandedFieldTypes(method.Type.Params), expandedFieldTypes(method.Type.Results)
		if expected.Getter {
			if len(parameters) != 0 || len(results) != 1 {
				return fmt.Errorf("authored getter %s must have no parameters and one result", key)
			}
		} else if !pointer || len(parameters) != 1 || len(results) > 1 {
			return fmt.Errorf("authored setter %s must have a pointer receiver, one value and optional fluent result", key)
		}
		if !expected.Getter {
			if _, variadic := parameters[0].(*ast.Ellipsis); variadic {
				return fmt.Errorf("authored setter %s must have a nonvariadic value", key)
			}
		}
		valueType := results
		if !expected.Getter {
			valueType = parameters
		}
		actual, err := canonicalType(valueType[0], imports, p.packageName)
		if err != nil {
			return err
		}
		wantedExpr, err := parser.ParseExpr(expected.ValueType)
		if err != nil {
			return err
		}
		wanted, err := canonicalType(wantedExpr, p.imports, p.packageName)
		if err != nil {
			return err
		}
		// Application field accessors may expose domain/convenience types. Only
		// a path armed with the staged Go build can validate their bodies and
		// target-package callers before publishing generated files.
		if actual != wanted && !(expected.FieldAccessor && p.allowAuthoredAccessorTypes) {
			return fmt.Errorf("authored accessor %s value type %s differs from %s", key, actual, wanted)
		}
		if actual != wanted {
			p.convenienceAccessor = true
		}
		if !expected.Getter && len(results) == 1 {
			actual, err = canonicalType(results[0], imports, p.packageName)
			if err != nil {
				return err
			}
			if actual != "*"+p.packageName+"."+expected.Receiver {
				return fmt.Errorf("authored setter %s fluent result must be *%s", key, expected.Receiver)
			}
		}
		p.claimed[key] = path
	}
	return nil
}

func entityMethodSource(method *ast.FuncDecl) string {
	if method == nil {
		return ""
	}
	var output bytes.Buffer
	if err := format.Node(&output, token.NewFileSet(), method); err != nil {
		return ""
	}
	return output.String()
}
