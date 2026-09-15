package generate

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"strconv"

	"github.com/viant/datly/spec"
)

// FactoryLinkPlan is compiled-Go registration wiring, separate from the
// runtime-free public policy source. It registers only in a supplied x.Registry.
type FactoryLinkPlan struct{ Name, Destination, PackagePath, Factory, Adapter string }

func (r *planResolver) resolveFactoryLink() {
	factory, adapter := "", ""
	if r.plan.ContractHandler != nil {
		factory = r.plan.ContractHandler.Factory
		adapter = "custom"
	}
	if r.plan.MutationHandler != nil {
		factory = r.plan.MutationHandler.Factory
		adapter = "mutation"
	}
	if factory == "" {
		return
	}
	r.plan.FactoryLink = &FactoryLinkPlan{Name: "Register" + exportedName(r.plan.ComponentName) + "Factories", Destination: r.plan.Generation.File("links", "links.go"), PackagePath: r.input.TargetPackage, Factory: factory, Adapter: adapter}
}

func (p *FactoryLinkPlan) source(packageName string, plan *Plan) (string, error) {
	imports := importsForFields([]Field{{Type: plan.Input.Type}, {Type: plan.Output.Type}}, plan.Imports)
	xAlias, adapterAlias := "", ""
	for _, target := range []struct {
		path, preferred string
		alias           *string
	}{{"github.com/viant/x", "x", &xAlias}, {"github.com/viant/datly/runtime/handler/" + p.Adapter, "handleradapter", &adapterAlias}} {
		for _, item := range imports {
			if item.Package == target.path {
				*target.alias = item.Alias
				if *target.alias == "" {
					*target.alias = packageAlias(item.Package)
				}
				break
			}
		}
		if *target.alias == "" {
			*target.alias = availableImportAlias(imports, target.preferred, p.Name, p.Factory)
			imports = append(imports, spec.ImportSpec{Alias: *target.alias, Package: target.path})
		}
	}
	registryName := availableImportAlias(imports, "registry", p.Name, p.Factory)
	input, err := parser.ParseExpr(plan.Input.Type)
	if err != nil {
		return "", err
	}
	output, err := parser.ParseExpr(plan.Output.Type)
	if err != nil {
		return "", err
	}
	file := &ast.File{Name: ast.NewIdent(packageName)}
	declaration := &ast.GenDecl{Tok: token.IMPORT, Lparen: 1}
	for _, item := range imports {
		imported := &ast.ImportSpec{Path: &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(item.Package)}}
		if item.Alias != "" {
			imported.Name = ast.NewIdent(item.Alias)
		}
		declaration.Specs = append(declaration.Specs, imported)
	}
	bridge := &ast.CallExpr{Fun: &ast.IndexListExpr{X: &ast.SelectorExpr{X: ast.NewIdent(adapterAlias), Sel: ast.NewIdent("Factory")}, Indices: []ast.Expr{input, output}}, Args: []ast.Expr{ast.NewIdent(p.Factory)}}
	function := &ast.FuncDecl{Name: ast.NewIdent(p.Name), Type: &ast.FuncType{
		Params:  &ast.FieldList{List: []*ast.Field{{Names: []*ast.Ident{ast.NewIdent(registryName)}, Type: &ast.StarExpr{X: &ast.SelectorExpr{X: ast.NewIdent(xAlias), Sel: ast.NewIdent("Registry")}}}}},
		Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}},
	}, Body: &ast.BlockStmt{List: []ast.Stmt{
		&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("function"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{&ast.CallExpr{Fun: &ast.SelectorExpr{X: ast.NewIdent(xAlias), Sel: ast.NewIdent("NewFunction")}, Args: []ast.Expr{&ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(p.PackagePath)}, &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(p.Factory)}, bridge}}}},
		&ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{&ast.ReturnStmt{Results: []ast.Expr{ast.NewIdent("err")}}}}},
		&ast.ReturnStmt{Results: []ast.Expr{&ast.CallExpr{Fun: &ast.SelectorExpr{X: ast.NewIdent(registryName), Sel: ast.NewIdent("RegisterFunctions")}, Args: []ast.Expr{ast.NewIdent("function")}}}},
	}}}
	file.Decls = []ast.Decl{declaration, function}
	var source bytes.Buffer
	if err := format.Node(&source, token.NewFileSet(), file); err != nil {
		return "", err
	}
	return source.String(), nil
}
