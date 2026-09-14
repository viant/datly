package golang

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
)

// ScaffoldHooks creates the optional user-owned input/output lifecycle file.
// The generator persists it create-once and never adds it to its manifest.
func ScaffoldHooks(config Config) (*ast.File, error) {
	packageName := strings.TrimSpace(config.Package)
	if !token.IsIdentifier(packageName) || token.Lookup(packageName).IsKeyword() {
		return nil, fmt.Errorf("hook scaffold package %q is invalid", config.Package)
	}
	input, err := localContractType(config.InputType, "input")
	if err != nil {
		return nil, err
	}
	output, err := localContractType(config.OutputType, "output")
	if err != nil {
		return nil, err
	}
	reserved := map[string]bool{input.Name: true, output.Name: true}
	contextAlias := hookImportAlias("context", reserved)
	reserved[contextAlias] = true
	handlerAlias := hookImportAlias("xhandler", reserved)
	declarations := []ast.Decl{
		&ast.GenDecl{Tok: token.IMPORT, Lparen: 1, Specs: []ast.Spec{
			&ast.ImportSpec{Name: ast.NewIdent(contextAlias), Path: &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote("context")}},
			&ast.ImportSpec{Name: ast.NewIdent(handlerAlias), Path: &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(handlerPackage)}},
		}},
		hookAssertion(handlerAlias, "Initializer", input),
		hookAssertion(handlerAlias, "ErrorFinalizer", output),
		initializerMethod(contextAlias, input),
		errorFinalizerMethod(contextAlias, output),
	}
	return &ast.File{Name: ast.NewIdent(packageName), Decls: declarations}, nil
}

func localContractType(source, label string) (*ast.Ident, error) {
	expression, err := parser.ParseExpr(strings.TrimSpace(source))
	if err != nil {
		return nil, fmt.Errorf("parse hook scaffold %s contract %q: %w", label, source, err)
	}
	identifier, ok := expression.(*ast.Ident)
	if !ok {
		return nil, fmt.Errorf("hook scaffold %s contract %q must be a direct package-local type", label, source)
	}
	return identifier, nil
}

func hookImportAlias(preferred string, reserved map[string]bool) string {
	if !reserved[preferred] {
		return preferred
	}
	for suffix := 2; ; suffix++ {
		candidate := preferred + strconv.Itoa(suffix)
		if !reserved[candidate] {
			return candidate
		}
	}
}

func hookAssertion(handlerAlias, interfaceName string, contract *ast.Ident) ast.Decl {
	return &ast.GenDecl{Tok: token.VAR, Specs: []ast.Spec{&ast.ValueSpec{
		Names: []*ast.Ident{ast.NewIdent("_")},
		Type:  selectExpr(ast.NewIdent(handlerAlias), interfaceName),
		Values: []ast.Expr{&ast.CallExpr{
			Fun: ast.NewIdent("new"), Args: []ast.Expr{ast.NewIdent(contract.Name)},
		}},
	}}}
}

func initializerMethod(contextAlias string, contract *ast.Ident) ast.Decl {
	return &ast.FuncDecl{
		Recv: &ast.FieldList{List: []*ast.Field{{Names: []*ast.Ident{ast.NewIdent("input")}, Type: &ast.StarExpr{X: ast.NewIdent(contract.Name)}}}},
		Name: ast.NewIdent("Init"),
		Type: &ast.FuncType{
			Params:  &ast.FieldList{List: []*ast.Field{{Names: []*ast.Ident{ast.NewIdent("ctx")}, Type: selectExpr(ast.NewIdent(contextAlias), "Context")}}},
			Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}},
		},
		Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("nil"))}},
	}
}

func errorFinalizerMethod(contextAlias string, contract *ast.Ident) ast.Decl {
	return &ast.FuncDecl{
		Recv: &ast.FieldList{List: []*ast.Field{{Names: []*ast.Ident{ast.NewIdent("output")}, Type: &ast.StarExpr{X: ast.NewIdent(contract.Name)}}}},
		Name: ast.NewIdent("Finalize"),
		Type: &ast.FuncType{
			Params: &ast.FieldList{List: []*ast.Field{
				{Names: []*ast.Ident{ast.NewIdent("ctx")}, Type: selectExpr(ast.NewIdent(contextAlias), "Context")},
				{Names: []*ast.Ident{ast.NewIdent("handlerErr")}, Type: ast.NewIdent("error")},
			}},
			Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}},
		},
		Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("nil"))}},
	}
}
