package generate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// RetainsReadIndexCache proves that the selected native asset already owns the
// cached receiver methods and that the retained input declares its storage.
// Source ownership alone never grants authority over linked declarations.
func RetainsReadIndexCache(directory, destination, inputDestination, owner, input, cache, indexType string, proposal *ast.File) bool {
	content, err := os.ReadFile(filepath.Join(directory, destination))
	if err != nil || !SourceOwnedBy(content, owner) || !readIndexMethodsMatch(content, proposal, input) {
		return false
	}

	if inputDestination == "" || filepath.Ext(inputDestination) != ".go" || strings.HasSuffix(inputDestination, "_test.go") {
		return false
	}
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(directory, inputDestination), nil, 0)
	if err != nil || file.Name.Name != proposal.Name.Name {
		return false
	}
	for _, decl := range file.Decls {
		group, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, item := range group.Specs {
			typ, ok := item.(*ast.TypeSpec)
			if !ok || typ.Name.Name != input {
				continue
			}
			shape, ok := typ.Type.(*ast.StructType)
			if !ok {
				continue
			}
			for _, field := range shape.Fields.List {
				for _, name := range field.Names {
					if name.Name == cache {
						ptr, ok := field.Type.(*ast.StarExpr)
						if !ok {
							return false
						}
						named, ok := ptr.X.(*ast.Ident)
						return ok && named.Name == indexType
					}
				}
			}
		}
	}

	return false
}

func readIndexMethodsMatch(content []byte, proposal *ast.File, input string) bool {
	existing, err := parser.ParseFile(token.NewFileSet(), "read-index-source.go", content, 0)
	if err != nil || proposal == nil || existing.Name.Name != proposal.Name.Name {
		return false
	}
	expected := map[string]string{}
	for _, decl := range proposal.Decls {
		if method, ok := decl.(*ast.FuncDecl); ok && readIndexReceiver(method, input) {
			expected[method.Name.Name] = entityMethodSource(method)
		}
	}
	if len(expected) == 0 {
		return false
	}
	found := 0
	for _, decl := range existing.Decls {
		if group, ok := decl.(*ast.GenDecl); ok {
			for _, item := range group.Specs {
				if typ, ok := item.(*ast.TypeSpec); ok && typ.Name.Name == input {
					return false
				}
			}
		}
		if method, ok := decl.(*ast.FuncDecl); ok && readIndexReceiver(method, input) {
			want, ok := expected[method.Name.Name]
			if !ok || want != entityMethodSource(method) {
				return false
			}
			found++
		}
	}
	return found == len(expected)
}
func readIndexReceiver(method *ast.FuncDecl, input string) bool {
	if method.Recv == nil || len(method.Recv.List) != 1 {
		return false
	}
	receiver := method.Recv.List[0].Type
	if ptr, ok := receiver.(*ast.StarExpr); ok {
		receiver = ptr.X
	}
	name, ok := receiver.(*ast.Ident)
	return ok && name.Name == input
}
func (p *scaffoldPersistence) ownedReadIndexDestination(name string, content []byte) bool {
	if p.plan == nil || p.plan.ReadIndexes == nil || !SourceOwnedBy(content, p.owner) {
		return false
	}
	indexes := p.plan.ReadIndexes
	if indexes.CacheField == "" || indexes.Source.Destination != name {
		return false
	}
	if !readIndexMethodsMatch(content, indexes.Source.File, p.plan.Input.Type) {
		return false
	}
	// Every other linked declaration still has its ordinary protection.
	file, err := parser.ParseFile(token.NewFileSet(), name, content, 0)
	if err != nil {
		return false
	}
	var declarations []ast.Decl
	for _, decl := range file.Decls {
		if method, ok := decl.(*ast.FuncDecl); ok && readIndexReceiver(method, p.plan.Input.Type) {
			continue
		}
		declarations = append(declarations, decl)
	}
	file.Decls = declarations
	rendered, err := (MutationSource{File: file}).source(file.Name.Name)
	return err == nil && !p.linkedSource([]byte(rendered))
}
