package golang

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strconv"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// StatementQueue lowers the already resolved statement into a typed queued
// operation. The surrounding native program retains binding, lifecycle and
// transaction ownership; this function never obtains a connection or drains.
func StatementQueue(value *plan.StatementPlan, config Config) (*ast.File, error) {
	if value == nil || value.SQL == "" {
		return nil, fmt.Errorf("canonical statement plan is required")
	}
	if !token.IsIdentifier(config.Package) || token.Lookup(config.Package).IsKeyword() || !token.IsIdentifier(config.Factory) {
		return nil, fmt.Errorf("statement target package and factory are required")
	}
	for _, typ := range []string{config.InputType, config.OutputType} {
		if typ == "" {
			return nil, fmt.Errorf("statement contract types are required")
		}
		if _, err := parser.ParseExpr(typ); err != nil {
			return nil, err
		}
	}
	l := &lowerer{config: config, reservedNames: map[string]bool{config.Factory: true}, pathsByAlias: map[string]string{}, importsByPath: map[string]string{}, usedImports: map[string]bool{}}
	if err := l.indexImports(); err != nil {
		return nil, err
	}
	for _, typ := range []string{config.InputType, config.OutputType} {
		if err := l.markExpressionImports(typ); err != nil {
			return nil, err
		}
	}
	fmtAlias := l.importsByPath["fmt"]
	if fmtAlias == "" {
		fmtAlias = l.availableAlias("fmt")
		l.pathsByAlias[fmtAlias] = "fmt"
		l.importsByPath["fmt"] = fmtAlias
	}
	l.usedImports["fmt"] = true
	selector := func(field plan.StatementSelector) (ast.Expr, error) {
		if len(field.Path) < 2 || (field.Path[0] != "Input" && field.Path[0] != "Output") {
			return nil, fmt.Errorf("statement selector requires canonical contract root")
		}
		root := "input"
		if field.Path[0] == "Output" {
			root = "output"
		}
		var result ast.Expr = ast.NewIdent(root)
		for _, part := range field.Path[1:] {
			if !token.IsIdentifier(part) || token.Lookup(part).IsKeyword() {
				return nil, fmt.Errorf("invalid statement field path")
			}
			result = selectExpr(result, part)
		}
		return result, nil
	}
	dest, err := selector(value.LastInsertID)
	if err != nil {
		return nil, err
	}
	if !value.LastInsertID.Addressable {
		return nil, fmt.Errorf("statement result destination is not addressable")
	}
	args := []ast.Expr{&ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(value.SQL)}, &ast.UnaryExpr{Op: token.AND, X: dest}}
	for _, field := range value.Arguments {
		arg, err := selector(field)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	nilExpr := ast.NewIdent("nil")
	fail := func(message string) ast.Stmt {
		return returnStmt(callExpr(selectExpr(ast.NewIdent(fmtAlias), "Errorf"), &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(message)}))
	}
	capability := &ast.InterfaceType{Methods: &ast.FieldList{List: []*ast.Field{{Names: []*ast.Ident{ast.NewIdent("ExecuteWithResult")}, Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("string")}, {Type: ast.NewIdent("any")}, {Type: &ast.Ellipsis{Elt: ast.NewIdent("any")}}}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}}}}}}}
	body := []ast.Stmt{
		&ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.BinaryExpr{X: ast.NewIdent("input"), Op: token.EQL, Y: nilExpr}, Op: token.LOR, Y: &ast.BinaryExpr{X: ast.NewIdent("output"), Op: token.EQL, Y: nilExpr}}, Body: &ast.BlockStmt{List: []ast.Stmt{fail("statement contracts are required")}}},
		&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("capability"), ast.NewIdent("ok")}, Tok: token.DEFINE, Rhs: []ast.Expr{&ast.TypeAssertExpr{X: ast.NewIdent("data"), Type: capability}}},
		&ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: ast.NewIdent("ok")}, Body: &ast.BlockStmt{List: []ast.Stmt{fail("native buffered statement result capability unavailable")}}},
		returnStmt(callExpr(selectExpr(ast.NewIdent("capability"), "ExecuteWithResult"), args...)),
	}
	function := &ast.FuncDecl{Name: ast.NewIdent(config.Factory + "QueueStatement"), Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("data", ast.NewIdent("any")), namedField("input", &ast.StarExpr{X: parseExpr(config.InputType)}), namedField("output", &ast.StarExpr{X: parseExpr(config.OutputType)})}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}
	paths := make([]string, 0, len(l.usedImports))
	for path := range l.usedImports {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	var imports []ast.Spec
	for _, path := range paths {
		imports = append(imports, &ast.ImportSpec{Name: ast.NewIdent(l.importsByPath[path]), Path: &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(path)}})
	}
	return &ast.File{Name: ast.NewIdent(config.Package), Decls: []ast.Decl{&ast.GenDecl{Tok: token.IMPORT, Specs: imports}, function}}, nil
}
