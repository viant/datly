package generate

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
)

func (p *Plan) exactSourceInput() bool {
	return p != nil && p.Generation != nil && p.Generation.SourceInput != "" && p.Generation.SourceInput == p.Input.Type
}
func (p *Plan) inputSource(pkg, comment, name string, fields []Field, imports []spec.ImportSpec) string {
	if p.exactSourceInput() {
		return structFileWithImports(pkg, comment, name, fields, imports)
	}
	return inputStructFile(pkg, comment, name, fields, imports)
}
func (p *Plan) inputSetters(pkg string) string {
	if p.exactSourceInput() {
		return ""
	}
	return inputSetterFile(pkg, p.Input.Type, p.Input.Fields, p.Imports)
}
func (p *Plan) sourceMemberSelections() (map[string][]spec.SourceMember, error) {
	result := map[string][]spec.SourceMember{}
	if p.Generation == nil {
		return result, nil
	}
	if len(p.Generation.SourceMembers) == 0 && p.Generation.SourceInput == "" {
		return result, nil
	}
	if p.ExternalHandler == nil || !p.ExternalHandler.GeneratedContracts || p.ExternalHandler.Build == nil {
		return nil, fmt.Errorf("source members require a source-authored generated factory")
	}
	destinations := map[string]string{}
	if p.Input.Ownership == ContractGenerated && p.localShape(p.Input.Package) {
		destinations[p.Input.Type] = p.Input.Destination
	}
	for _, view := range p.Views {
		if view.Ownership == ViewGenerated && p.localShape(view.Package) {
			destinations[view.Name] = view.Destination
		}
	}
	for _, selection := range p.Generation.SourceMembers {
		if !token.IsIdentifier(selection.Owner) || !token.IsIdentifier(selection.Name) || selection.Owner == "_" || selection.Name == "_" || selection.Name == "Has" {
			return nil, fmt.Errorf("source members require direct non-marker declarations")
		}
		destination, ok := destinations[selection.Owner]
		if !ok {
			return nil, fmt.Errorf("source member owner %s does not match a local generated destination", selection.Owner)
		}
		snapshot, ok := p.ExternalHandler.SourceMemberFiles[destination]
		if !ok || snapshot.File == nil {
			return nil, fmt.Errorf("source member owner %s has no build-selected source at generated destination %s", selection.Owner, destination)
		}
		if _, err := packageGoDestination(destination, "source member"); err != nil {
			return nil, err
		}
		result[destination] = append(result[destination], selection)
	}
	if p.Generation.SourceInput != "" {
		if !p.exactSourceInput() {
			return nil, fmt.Errorf("source_input must select the local generated input")
		}
		found := false
		for _, item := range p.Generation.SourceMembers {
			if item.Owner == p.Input.Type {
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("source_input requires explicit input source members")
		}
	}
	return result, nil
}
func (p *Plan) sharedSourceMemberDestination(destination, previous, owner string) bool {
	if !p.exactSourceInput() || filepath.Clean(destination) != filepath.Clean(p.Input.Destination) {
		return false
	}
	if !(previous == "input contract" && strings.HasPrefix(owner, "view ") || owner == "input contract" && strings.HasPrefix(previous, "view ")) {
		return false
	}
	input, view := false, false
	for _, selection := range p.Generation.SourceMembers {
		if selection.Owner == p.Input.Type {
			input = true
		}
		if "view "+selection.Owner == previous || "view "+selection.Owner == owner {
			for _, candidate := range p.Views {
				if candidate.Ownership == ViewGenerated && candidate.Name == selection.Owner && filepath.Clean(candidate.Destination) == filepath.Clean(destination) {
					view = true
				}
			}
		}
	}
	return input && view
}

// combineSourceMemberArtifacts assembles explicitly co-located generated
// declarations before publication. It does not change destination ownership for
// unrelated artifacts or admit duplicate package-level declarations.
func (p *Plan) combineSourceMemberArtifacts(files []EmittedFile) ([]EmittedFile, error) {
	selections, err := p.sourceMemberSelections()
	if err != nil {
		return nil, err
	}
	if len(selections) == 0 {
		return files, nil
	}
	p.sourceMemberSyntax = map[string]*ast.File{}
	indexes := map[string]int{}
	var result []EmittedFile
	for _, file := range files {
		index, exists := indexes[file.Path]
		var syntax *ast.File
		relevant := ""
		for destination := range selections {
			if filepath.Base(file.Path) == destination {
				relevant = destination
				break
			}
		}
		if relevant != "" {
			syntax, err = parser.ParseFile(token.NewFileSet(), file.Path, file.Content, 0)
			if err != nil {
				return nil, err
			}
		}
		if !exists {
			indexes[file.Path] = len(result)
			result = append(result, file)
			if syntax != nil {
				p.sourceMemberSyntax[relevant] = syntax
			}
			continue
		}
		if relevant == "" || !p.sharedSourceMemberDestination(relevant, "input contract", "view "+p.sourceViewAt(relevant)) {
			return nil, fmt.Errorf("duplicate generated artifact %s", file.Path)
		}
		syntax, err = combineSourceMemberDeclarations(p.sourceMemberSyntax[relevant], syntax, false)
		if err != nil {
			return nil, err
		}
		p.sourceMemberSyntax[relevant] = syntax
		rendered, err := (xshape.SourceParser{}).FormatFile(syntax)
		if err != nil {
			return nil, err
		}
		result[index].Content = generatedHeader(p.ComponentName) + string(rendered)
	}
	return result, nil
}
func (p *Plan) sourceViewAt(destination string) string {
	for _, view := range p.Views {
		if view.Ownership == ViewGenerated && view.Destination == destination {
			return view.Name
		}
	}
	return ""
}

// Assembly copies only changed nodes; current build-selected source and fresh
// generated AST remain shared immutable authority throughout staged generation.
func combineSourceMemberDeclarations(left, right *ast.File, importsOnly bool) (*ast.File, error) {
	if left.Name.Name != right.Name.Name {
		return nil, fmt.Errorf("combined source package differs")
	}
	result := *left
	result.Decls = append([]ast.Decl(nil), left.Decls...)
	names := map[string]bool{}
	imports := map[string]string{}
	for _, declaration := range left.Decls {
		for _, name := range declarationNames(declaration) {
			names[name] = true
		}
		if gen, ok := declaration.(*ast.GenDecl); ok && gen.Tok == token.IMPORT {
			for _, entry := range gen.Specs {
				item := entry.(*ast.ImportSpec)
				alias := ""
				if item.Name != nil {
					alias = item.Name.Name
				}
				imports[item.Path.Value] = alias
			}
		}
	}
	for _, declaration := range right.Decls {
		if gen, ok := declaration.(*ast.GenDecl); ok && gen.Tok == token.IMPORT {
			var specs []ast.Spec
			for _, entry := range gen.Specs {
				item := entry.(*ast.ImportSpec)
				alias := ""
				if item.Name != nil {
					alias = item.Name.Name
				}
				if prior, ok := imports[item.Path.Value]; ok {
					if prior != alias {
						return nil, fmt.Errorf("combined import aliases differ for %s", item.Path.Value)
					}
					continue
				}
				imports[item.Path.Value] = alias
				specs = append(specs, entry)
			}
			if len(specs) > 0 {
				copied := *gen
				copied.Specs = specs
				result.Decls = append(result.Decls, &copied)
			}
			continue
		}
		if importsOnly {
			continue
		}
		for _, name := range declarationNames(declaration) {
			if names[name] {
				return nil, fmt.Errorf("duplicate combined declaration %s", name)
			}
			names[name] = true
		}
		result.Decls = append(result.Decls, declaration)
	}
	var first, rest []ast.Decl
	for _, declaration := range result.Decls {
		if gen, ok := declaration.(*ast.GenDecl); ok && gen.Tok == token.IMPORT {
			first = append(first, declaration)
		} else {
			rest = append(rest, declaration)
		}
	}
	result.Decls = append(first, rest...)
	return &result, nil
}
func (p *scaffoldPersistence) prepareSourceMembers(target, readRoot string) (map[string]bool, error) {
	admitted := map[string]bool{}
	if p.plan == nil {
		return admitted, nil
	}
	selections, err := p.plan.sourceMemberSelections()
	if err != nil {
		return nil, err
	}
	for destination, members := range selections {
		snapshot := p.plan.ExternalHandler.SourceMemberFiles[destination]
		original, err := os.ReadFile(filepath.Join(readRoot, destination))
		if err != nil {
			return nil, err
		}
		if string(original) != snapshot.Source {
			return nil, fmt.Errorf("source member declaration %s changed after build-selected parsing", destination)
		}
		index := -1
		for i, file := range p.files {
			if filepath.Clean(file.Path) == filepath.Join(target, destination) {
				index = i
				break
			}
		}
		if index < 0 {
			return nil, fmt.Errorf("source member destination %s has no staged artifact", destination)
		}
		generated := p.plan.sourceMemberSyntax[destination]
		if generated == nil {
			return nil, fmt.Errorf("source member destination %s has no current generated AST", destination)
		}
		owner := generatedOwner(original)
		if owner != "" && owner != p.owner {
			return nil, fmt.Errorf("source members cannot claim foreign owner %s", owner)
		}
		retained, err := retainSourceMemberAST(snapshot.File, generated, members, owner == "", func(file *ast.File, field *ast.Field) (string, error) {
			return sourceFieldSemanticSignature(file, field, p.plan.Package, snapshot.CanonicalType)
		})
		if err != nil {
			return nil, err
		}
		retained = sourceMemberUsedGeneratedImports(retained)
		retained, err = combineSourceMemberDeclarations(retained, snapshot.File, true)
		if err != nil {
			return nil, err
		}
		rendered, err := (xshape.SourceParser{}).FormatFile(retained)
		if err != nil {
			return nil, err
		}
		functions, err := sourceMemberFunctions(snapshot)
		if err != nil {
			return nil, err
		}
		p.files[index].Content = generatedHeader(p.owner) + string(rendered) + functions
		admitted[destination] = true
	}
	return admitted, nil
}

// Preserve original function/method bytes, including inline comments and compiler
// directives, from the same sealed build-selected AST. No reparsing or printer
// reconstruction of application behavior is needed.
func sourceMemberFunctions(snapshot SourceMemberFile) (string, error) {
	var result strings.Builder
	base := snapshot.File.FileStart
	if !base.IsValid() {
		return "", fmt.Errorf("source member snapshot has no file position authority")
	}
	for _, declaration := range snapshot.File.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		start := fn.Pos()
		if fn.Doc != nil && fn.Doc.Pos() < start {
			start = fn.Doc.Pos()
		}
		begin, end := int(start-base), int(fn.End()-base)
		if begin < 0 || end < begin || end > len(snapshot.Source) {
			return "", fmt.Errorf("source function %s has invalid source positions", fn.Name.Name)
		}
		result.WriteString("\n")
		result.WriteString(snapshot.Source[begin:end])
		result.WriteString("\n")
	}
	return result.String(), nil
}

// Initial semantic admission can preserve original field aliases. Keep only
// imports used by the independently generated declaration AST before combining
// all original imports required by byte-retained application functions.
func sourceMemberUsedGeneratedImports(file *ast.File) *ast.File {
	used := map[string]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok {
			if name, ok := selector.X.(*ast.Ident); ok {
				used[name.Name] = true
			}
		}
		return true
	})
	result := *file
	result.Decls = nil
	result.Imports = nil
	for _, decl := range file.Decls {
		group, ok := decl.(*ast.GenDecl)
		if !ok || group.Tok != token.IMPORT {
			result.Decls = append(result.Decls, decl)
			continue
		}
		copyGroup := *group
		copyGroup.Specs = nil
		for _, entry := range group.Specs {
			item := entry.(*ast.ImportSpec)
			pkg, err := strconv.Unquote(item.Path.Value)
			if err != nil {
				continue
			}
			name := path.Base(pkg)
			if item.Name != nil {
				name = item.Name.Name
			}
			if name == "_" || name == "." || used[name] {
				copyGroup.Specs = append(copyGroup.Specs, item)
				result.Imports = append(result.Imports, item)
			}
		}
		if len(copyGroup.Specs) > 0 {
			result.Decls = append(result.Decls, &copyGroup)
		}
	}
	return &result
}
