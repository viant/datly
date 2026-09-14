package generate

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	xshape "github.com/viant/x/shape"
)

// GoHandlerAsset is accepted custom Go source supplied by transcription.
// Canonical component metadata stores only Entry; the AST remains a
// generation concern and is cloned at every ownership boundary.
type GoHandlerAsset struct {
	Destination string
	Entry       string
	File        *ast.File
}

// Clone returns an isolated parser-backed copy of the handler asset.
func (a *GoHandlerAsset) Clone() (*GoHandlerAsset, error) {
	if a == nil {
		return nil, nil
	}
	result := &GoHandlerAsset{Destination: a.Destination, Entry: a.Entry}
	file, err := cloneGoFile(a.File)
	if err != nil {
		return nil, fmt.Errorf("clone custom handler AST: %w", err)
	}
	result.File = file
	return result, nil
}

// GoHandlerPlan is the validated custom handler source emitted with a package.
type GoHandlerPlan struct {
	Destination string
	Entry       string
	File        *ast.File
}

func resolveGoHandler(plan *Plan, asset *GoHandlerAsset, targetPackage string) error {
	if asset == nil {
		return nil
	}
	if asset.File == nil {
		return fmt.Errorf("custom handler AST is required")
	}
	if strings.TrimSpace(targetPackage) == "" {
		return fmt.Errorf("custom handler target package is required")
	}
	entry := strings.TrimSpace(asset.Entry)
	if !token.IsIdentifier(entry) || !token.IsExported(entry) {
		return fmt.Errorf("custom handler entry %q must be an exported Go identifier", entry)
	}
	destination := strings.TrimSpace(asset.Destination)
	if destination == "" {
		destination = lowerSnake(plan.ComponentName) + "_handler.go"
	}
	relative, err := managedRelativePath(destination)
	if err != nil {
		return fmt.Errorf("custom handler destination: %w", err)
	}
	if filepath.Base(relative) != relative || filepath.Ext(relative) != ".go" {
		return fmt.Errorf("custom handler destination %q must be a package-local .go file", destination)
	}
	imports, err := handlerImports(asset.File, targetPackage)
	if err != nil {
		return err
	}
	if err = validateHandlerComments(asset.File); err != nil {
		return err
	}
	function, err := handlerFunction(asset.File, entry)
	if err != nil {
		return err
	}
	if err = validateHandlerSignature(function, plan, imports, targetPackage); err != nil {
		return fmt.Errorf("custom handler %s: %w", entry, err)
	}
	if err = validateHandlerDeclarations(asset.File, entry, plan); err != nil {
		return err
	}
	if plan.Handler != "" && plan.Handler != entry {
		return fmt.Errorf("route handler %q conflicts with custom handler entry %q", plan.Handler, entry)
	}
	plan.Handler = entry
	plan.GoHandler = &GoHandlerPlan{Destination: relative, Entry: entry, File: asset.File}
	return nil
}

func validateHandlerComments(file *ast.File) error {
	if file == nil {
		return nil
	}
	for _, group := range file.Comments {
		for _, comment := range group.List {
			text := strings.TrimSpace(comment.Text)
			if strings.HasPrefix(text, "//go:build") || strings.HasPrefix(text, "// +build") {
				return fmt.Errorf("handler source cannot contain build constraints")
			}
		}
	}
	return nil
}

func handlerImports(file *ast.File, targetPackage string) (map[string]string, error) {
	result := map[string]string{}
	for _, item := range file.Imports {
		packagePath, err := strconv.Unquote(item.Path.Value)
		if err != nil {
			return nil, fmt.Errorf("custom handler import %q: %w", item.Path.Value, err)
		}
		if strings.TrimSpace(packagePath) == strings.TrimSpace(targetPackage) {
			return nil, fmt.Errorf("custom handler cannot import its generated package %q", packagePath)
		}
		alias := path.Base(packagePath)
		if item.Name != nil {
			alias = item.Name.Name
		}
		if alias == "." || alias == "_" {
			return nil, fmt.Errorf("custom handler import %q cannot use alias %q", packagePath, alias)
		}
		if alias == "error" {
			return nil, fmt.Errorf("custom handler import %q cannot shadow the predeclared error type", packagePath)
		}
		if previous := result[alias]; previous != "" && previous != packagePath {
			return nil, fmt.Errorf("custom handler import alias %q is ambiguous", alias)
		}
		result[alias] = packagePath
	}
	return result, nil
}

func handlerFunction(file *ast.File, entry string) (*ast.FuncDecl, error) {
	var result *ast.FuncDecl
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name == nil || function.Name.Name != entry {
			continue
		}
		if result != nil {
			return nil, fmt.Errorf("custom handler entry %q is declared more than once", entry)
		}
		result = function
	}
	if result == nil {
		return nil, fmt.Errorf("custom handler entry %q was not found", entry)
	}
	if result.Recv != nil {
		return nil, fmt.Errorf("custom handler entry %q must be a function, not a method", entry)
	}
	if result.Type.TypeParams != nil && len(result.Type.TypeParams.List) > 0 {
		return nil, fmt.Errorf("custom handler entry %q cannot declare type parameters", entry)
	}
	if result.Body == nil {
		return nil, fmt.Errorf("custom handler entry %q requires a body", entry)
	}
	return result, nil
}

func validateHandlerSignature(function *ast.FuncDecl, plan *Plan, imports map[string]string, targetPackage string) error {
	params := expandedFieldTypes(function.Type.Params)
	results := expandedFieldTypes(function.Type.Results)
	if len(params) != 2 || len(results) != 2 {
		return fmt.Errorf("signature must be func(context.Context, *%s) (*%s, error)", plan.Input.Type, plan.Output.Type)
	}
	for _, expression := range append(append([]ast.Expr(nil), params...), results...) {
		if err := rejectImportedTypeIdentifier(expression, imports); err != nil {
			return err
		}
	}
	actualContext, err := canonicalType(params[0], imports, targetPackage)
	if err != nil {
		return err
	}
	if actualContext != "context.Context" {
		return fmt.Errorf("first parameter must be context.Context")
	}
	expectedImports := plan.importMap()
	input, err := parser.ParseExpr("*(" + plan.Input.Type + ")")
	if err != nil {
		return fmt.Errorf("parse input contract type: %w", err)
	}
	output, err := parser.ParseExpr("*(" + plan.Output.Type + ")")
	if err != nil {
		return fmt.Errorf("parse output contract type: %w", err)
	}
	comparisons := []struct {
		label    string
		actual   ast.Expr
		expected ast.Expr
	}{
		{"input parameter", params[1], input},
		{"first result", results[0], output},
	}
	for _, comparison := range comparisons {
		actual, actualErr := canonicalType(comparison.actual, imports, targetPackage)
		expected, expectedErr := canonicalType(comparison.expected, expectedImports, targetPackage)
		if actualErr != nil {
			return fmt.Errorf("%s: %w", comparison.label, actualErr)
		}
		if expectedErr != nil {
			return fmt.Errorf("%s contract: %w", comparison.label, expectedErr)
		}
		if actual != expected {
			return fmt.Errorf("%s type %q does not match %q", comparison.label, actual, expected)
		}
	}
	last, err := canonicalType(results[1], imports, targetPackage)
	if err != nil || last != "error" {
		return fmt.Errorf("second result must be error")
	}
	return nil
}

func rejectImportedTypeIdentifier(expression ast.Expr, imports map[string]string) error {
	switch actual := expression.(type) {
	case *ast.Ident:
		if packagePath := imports[actual.Name]; packagePath != "" {
			return fmt.Errorf("type identifier %q is shadowed by import %q", actual.Name, packagePath)
		}
	case *ast.SelectorExpr:
		return nil
	case *ast.ParenExpr:
		return rejectImportedTypeIdentifier(actual.X, imports)
	case *ast.StarExpr:
		return rejectImportedTypeIdentifier(actual.X, imports)
	case *ast.ArrayType:
		return rejectImportedTypeIdentifier(actual.Elt, imports)
	case *ast.MapType:
		if err := rejectImportedTypeIdentifier(actual.Key, imports); err != nil {
			return err
		}
		return rejectImportedTypeIdentifier(actual.Value, imports)
	case *ast.ChanType:
		return rejectImportedTypeIdentifier(actual.Value, imports)
	case *ast.Ellipsis:
		return rejectImportedTypeIdentifier(actual.Elt, imports)
	case *ast.IndexExpr:
		if err := rejectImportedTypeIdentifier(actual.X, imports); err != nil {
			return err
		}
		return rejectImportedTypeIdentifier(actual.Index, imports)
	case *ast.IndexListExpr:
		if err := rejectImportedTypeIdentifier(actual.X, imports); err != nil {
			return err
		}
		for _, index := range actual.Indices {
			if err := rejectImportedTypeIdentifier(index, imports); err != nil {
				return err
			}
		}
	}
	return nil
}

func expandedFieldTypes(fields *ast.FieldList) []ast.Expr {
	if fields == nil {
		return nil
	}
	var result []ast.Expr
	for _, field := range fields.List {
		count := len(field.Names)
		if count == 0 {
			count = 1
		}
		for i := 0; i < count; i++ {
			result = append(result, field.Type)
		}
	}
	return result
}

func (plan *Plan) importMap() map[string]string {
	result := map[string]string{"context": "context"}
	for _, item := range plan.Imports {
		alias := strings.TrimSpace(item.Alias)
		if alias == "" {
			alias = path.Base(item.Package)
		}
		result[alias] = strings.TrimSpace(item.Package)
	}
	return result
}

func canonicalType(expression ast.Expr, imports map[string]string, localPackage string) (string, error) {
	return (xshape.Resolver{Imports: imports, Package: localPackage}).Canonical(expression)
}

func validateHandlerDeclarations(file *ast.File, entry string, plan *Plan) error {
	reserved := map[string]bool{"Component": true}
	for _, name := range []string{localTypeName(plan.Input.Type), localTypeName(plan.Output.Type)} {
		if name != "" {
			reserved[name] = true
		}
	}
	if name := plan.inputMarkerType(); name != "" {
		reserved[name] = true
	}
	for _, view := range plan.Views {
		name := view.Name
		if view.Ownership == ViewLinked {
			name = localTypeName(view.Type)
		}
		if name != "" {
			reserved[name] = true
		}
	}
	for _, helper := range plan.HelperTypes {
		reserved[helper.Name] = true
	}
	for _, name := range plan.referencedPlaceholderTypes() {
		reserved[name] = true
	}
	seen := map[string]bool{}
	for _, declaration := range file.Decls {
		for _, name := range declarationNames(declaration) {
			if name == "init" {
				return fmt.Errorf("custom handler cannot declare init")
			}
			if name == "error" {
				return fmt.Errorf("custom handler cannot shadow the predeclared error type")
			}
			if seen[name] {
				return fmt.Errorf("custom handler declaration %q is duplicated", name)
			}
			seen[name] = true
			if reserved[name] {
				return fmt.Errorf("custom handler declaration %q conflicts with generated package ownership", name)
			}
		}
	}
	return nil
}

func declarationNames(declaration ast.Decl) []string {
	switch actual := declaration.(type) {
	case *ast.FuncDecl:
		if actual.Recv == nil && actual.Name != nil && actual.Name.Name != "_" {
			return []string{actual.Name.Name}
		}
	case *ast.GenDecl:
		var result []string
		for _, item := range actual.Specs {
			switch spec := item.(type) {
			case *ast.TypeSpec:
				if spec.Name.Name != "_" {
					result = append(result, spec.Name.Name)
				}
			case *ast.ValueSpec:
				for _, name := range spec.Names {
					if name.Name != "_" {
						result = append(result, name.Name)
					}
				}
			}
		}
		return result
	}
	return nil
}

func goHandlerFileText(packageName string, plan *GoHandlerPlan) (string, error) {
	if plan == nil || plan.File == nil {
		return "", fmt.Errorf("validated custom handler plan is required")
	}
	file, err := cloneGoFile(plan.File)
	if err != nil {
		return "", err
	}
	if file == nil || file.Name == nil {
		return "", fmt.Errorf("validated custom handler AST is required")
	}
	file.Name = ast.NewIdent(packageName)
	var result bytes.Buffer
	if err := format.Node(&result, token.NewFileSet(), file); err != nil {
		return "", fmt.Errorf("format custom handler: %w", err)
	}
	return result.String(), nil
}

func cloneGoFile(file *ast.File) (*ast.File, error) {
	if file == nil {
		return nil, nil
	}
	var source bytes.Buffer
	if err := format.Node(&source, token.NewFileSet(), file); err != nil {
		return nil, err
	}
	return parser.ParseFile(token.NewFileSet(), "handler.go", source.Bytes(), parser.ParseComments)
}
