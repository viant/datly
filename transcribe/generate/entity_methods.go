package generate

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

type entityMethodOwnership struct {
	expected    map[string]EntityMethod
	claimed     map[string]string
	generated   map[string]string
	imports     map[string]string
	packageName string
}

func (p *scaffoldPersistence) preserveEntityMethods(target, existing string, manifest *scaffoldManifest) error {
	if p.plan == nil || p.plan.EntitySupport == nil || len(p.plan.EntitySupport.Methods) == 0 {
		return nil
	}
	owned := map[string]bool{}
	for _, file := range manifest.Files {
		owned[file] = true
	}
	proposed := map[string]string{}
	for _, file := range p.files {
		relative, err := managedPath(target, file.Path)
		if err != nil {
			return err
		}
		proposed[relative] = scaffoldFingerprint([]byte(file.Content))
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
	file, err := parser.ParseFile(token.NewFileSet(), "entities.go", p.files[index].Content, parser.ParseComments)
	if err != nil {
		return err
	}
	imports, err := handlerImports(file, "")
	if err != nil {
		return err
	}
	policy := &entityMethodOwnership{expected: map[string]EntityMethod{}, claimed: map[string]string{}, generated: map[string]string{}, imports: imports, packageName: file.Name.Name}
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
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(existing, name)
		if fingerprint := proposed[name]; fingerprint != "" {
			content, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			if fingerprint == scaffoldFingerprint(content) {
				continue
			}
		}
		if owned[name] {
			if manifest.Roles[name] != "shape" && !p.shapeDestinations()[name] {
				continue
			}
			// An unchanged generated shape is Datly-owned output, not an
			// authored implementation of the methods being regenerated. Only
			// inspect a shape after its trusted fingerprint diverges.
			if fingerprint := manifest.Fingerprints[name]; fingerprint != "" {
				content, readErr := os.ReadFile(path)
				if readErr != nil {
					return readErr
				}
				if fingerprint == scaffoldFingerprint(content) {
					continue
				}
			}
		}
		source, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		if err = policy.inspect(source, name); err != nil {
			return err
		}
	}
	if p.plan.HookScaffold != nil {
		if _, err := os.Stat(filepath.Join(existing, p.plan.HookScaffold.Destination)); os.IsNotExist(err) {
			if err = policy.inspect(p.plan.HookScaffold.File, p.plan.HookScaffold.Destination); err != nil {
				return err
			}
		}
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
	p.files[index].Content = result.String() + "\n"
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
		if actual != wanted {
			return fmt.Errorf("authored accessor %s value type %s differs from %s", key, actual, wanted)
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
