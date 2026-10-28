package generate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
)

// A generated companion can mix entity accessors with generated methods on a
// retained Input. Permit regeneration only when those Input methods are exactly
// the current generator proposal. This never grants ownership of the Input type
// or an authored/modified receiver method.
func (p *scaffoldPersistence) ownedUnchangedInputMethods(name string, content []byte) bool {
	if p.plan == nil || p.plan.Input.Ownership != ContractLinked || !SourceOwnedBy(content, p.owner) {
		return false
	}
	var proposal *ast.File
	for _, file := range p.files {
		if filepath.Base(file.Path) != name {
			continue
		}
		var err error
		proposal, err = parser.ParseFile(token.NewFileSet(), name, file.Content, 0)
		if err != nil {
			return false
		}
		break
	}
	if proposal == nil || !readIndexMethodsMatch(content, proposal, p.plan.Input.Type) {
		return false
	}
	existing, err := parser.ParseFile(token.NewFileSet(), name, content, 0)
	if err != nil {
		return false
	}
	// Import bindings used by identical method syntax must remain identical too.
	imports := map[string]bool{}
	for _, item := range proposal.Imports {
		key := item.Path.Value
		if item.Name != nil {
			key = item.Name.Name + " " + key
		}
		imports[key] = true
	}
	for _, item := range existing.Imports {
		key := item.Path.Value
		if item.Name != nil {
			key = item.Name.Name + " " + key
		}
		if !imports[key] {
			return false
		}
	}
	var declarations []ast.Decl
	for _, decl := range existing.Decls {
		if method, ok := decl.(*ast.FuncDecl); ok && readIndexReceiver(method, p.plan.Input.Type) {
			continue
		}
		declarations = append(declarations, decl)
	}
	existing.Decls = declarations
	rendered, err := (MutationSource{File: existing}).source(existing.Name.Name)
	return err == nil && !p.linkedSource([]byte(rendered))
}
