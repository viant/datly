package generate

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"

	"github.com/viant/datly/transcribe/presencegen"
)

// resolvePreviousPresence reuses the existing presence generator on planned
// shape source. Methods join ordinary entity support so ownership validation,
// split-package relocation and transactional publication remain authoritative.
func (r *planResolver) resolvePreviousPresence() error {
	if !r.input.nativeMutationTargetSelected {
		return nil
	}
	var entries []string
	seen := map[string]bool{}
	canonical, err := r.canonicalViewIndex()
	if err != nil {
		return err
	}
	for _, view := range r.plan.Views {
		auxiliaryOnly := canonical[view.Identity] != nil && canonical[view.Identity].Auxiliary
		for _, identity := range view.RoleIdentities {
			if role := canonical[identity]; role != nil && !role.Auxiliary {
				auxiliaryOnly = false
			}
		}
		if auxiliaryOnly {
			continue
		}
		if view.Ownership == ViewGenerated && len(view.SetMarkerFields) != 0 && !seen[view.Name] {
			entries = append(entries, view.Name)
			seen[view.Name] = true
		}
	}
	if len(entries) == 0 {
		return nil
	}
	source, err := presencegen.Generate(r.plan.PackageName(), map[string][]byte{"views.go": []byte(viewFile(r.plan.PackageName(), r.plan))}, entries, presencegen.Options{RowOnly: true, TypeResolver: r.input.TypeResolver})
	if err != nil {
		return fmt.Errorf("generate Previous presence: %w", err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "previous_presence.go", source, 0)
	if err != nil {
		return err
	}
	support := r.plan.EntitySupport
	if support == nil {
		support = &EntitySupportPlan{File: &ast.File{Name: ast.NewIdent(r.plan.PackageName())}, Role: "setters", Destination: r.plan.Generation.File("setters", "setters.go")}
		r.plan.EntitySupport = support
	}
	imports, err := handlerImports(support.File, "")
	if err != nil {
		return err
	}
	// The row-local renderer imports only standard scalar helpers. Reuse an
	// existing import alias, or choose a free alias before appending the methods.
	for _, item := range file.Imports {
		path, err := strconv.Unquote(item.Path.Value)
		if err != nil {
			return err
		}
		old := item.Name.Name
		alias := old
		found := false
		for candidate, imported := range imports {
			if imported == path {
				alias, found = candidate, true
				break
			}
		}
		if !found {
			for suffix := 2; imports[alias] != ""; suffix++ {
				alias = fmt.Sprintf("%s%d", old, suffix)
			}
			item.Name = ast.NewIdent(alias)
			support.File.Decls = append([]ast.Decl{&ast.GenDecl{Tok: token.IMPORT, Specs: []ast.Spec{item}}}, support.File.Decls...)
			support.File.Imports = append(support.File.Imports, item)
			imports[alias] = path
		}
		if alias != old {
			avoidPresenceImportShadow(file, alias)
			ast.Inspect(file, func(node ast.Node) bool {
				if selector, ok := node.(*ast.SelectorExpr); ok {
					if qualifier, ok := selector.X.(*ast.Ident); ok && qualifier.Name == old {
						qualifier.Name = alias
					}
				}
				return true
			})
		}
	}
	for _, declaration := range file.Decls {
		method, ok := declaration.(*ast.FuncDecl)
		if !ok || method.Recv == nil {
			continue
		}
		receiver := method.Recv.List[0].Type.(*ast.StarExpr).X.(*ast.Ident).Name
		for _, existing := range support.Methods {
			if existing.Receiver == receiver && existing.Name == method.Name.Name {
				return fmt.Errorf("duplicate generated Previous presence owner for %s.%s", receiver, method.Name.Name)
			}
		}
		support.File.Decls = append(support.File.Decls, method)
		support.Methods = append(support.Methods, EntityMethod{Receiver: receiver, Name: method.Name.Name, Signature: "func(*" + receiver + ")"})
	}
	// Normalize positions before the existing entity-method projection runs.
	support.File, err = cloneGoFile(support.File)
	return err
}

// Existing entity support may import slices as r, prev, present or has. Rename
// only resolved local variables in the generated methods before reusing that
// alias; field selectors and authored support remain unchanged.
func avoidPresenceImportShadow(file *ast.File, alias string) {
	used := map[string]bool{}
	var shadowed []*ast.Object
	seen := map[*ast.Object]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok {
			used[id.Name] = true
			if id.Obj != nil && id.Obj.Kind == ast.Var && id.Obj.Name == alias && !seen[id.Obj] {
				seen[id.Obj] = true
				shadowed = append(shadowed, id.Obj)
			}
		}
		return true
	})
	for _, object := range shadowed {
		name := alias + "Value"
		for suffix := 2; used[name]; suffix++ {
			name = fmt.Sprintf("%sValue%d", alias, suffix)
		}
		used[name] = true
		ast.Inspect(file, func(node ast.Node) bool {
			if id, ok := node.(*ast.Ident); ok && id.Obj == object {
				id.Name = name
			}
			return true
		})
		object.Name = name
	}
}
