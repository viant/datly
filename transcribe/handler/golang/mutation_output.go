package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// MutationOutputAsset projects the working request body into the declared
// output contract. Call before returning output, not from the error-free getter.
type MutationOutputAsset struct {
	File     *ast.File
	Function string
}

func MutationOutputSupport(value *plan.Plan, config Config) (*MutationOutputAsset, error) {
	l := &lowerer{plan: value, config: config}
	if err := l.prepare(); err != nil {
		return nil, err
	}
	if value.Output == nil {
		return nil, fmt.Errorf("generic mutation output requires a declared body destination")
	}
	input, output := value.Root.InputPath, value.Output.Path
	if len(input) < 2 || input[0] != "Input" || len(output) < 2 || output[0] != "Output" {
		return nil, fmt.Errorf("mutation output requires canonical input and output field paths")
	}
	e := &previousEmitter{l: l}
	e.shape = l.availableAlias("xshape")
	l.pathsByAlias[e.shape] = "github.com/viant/x/shape"
	e.reflect = l.availableAlias("reflect")
	l.pathsByAlias[e.reflect] = "reflect"
	l.pathsByAlias[l.fmtAlias] = "fmt"
	id := ast.NewIdent
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.BinaryExpr{X: id("input"), Op: token.EQL, Y: id("nil")}, Op: token.LOR, Y: &ast.BinaryExpr{X: id("output"), Op: token.EQL, Y: id("nil")}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation output projection requires input and output"))}}}, e.accessorAssignment(parseExpr(config.InputType), strings.Join(input[1:], "."), "source"), e.visitError(), e.accessorAssignment(parseExpr(config.OutputType), strings.Join(output[1:], "."), "target"), e.visitError(), &ast.AssignStmt{Lhs: []ast.Expr{id("body"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(id("source"), "Get"), id("input"))}}, e.visitError(), returnStmt(callExpr(selectExpr(id("target"), "Set"), id("output"), callExpr(selectExpr(id("body"), "Interface"))))}
	name := "_" + lowerInitial(l.factory) + "ProjectOutput"
	if err := l.reserveDeclaration(name); err != nil {
		return nil, err
	}
	file := &ast.File{Name: id(config.Package), Decls: []ast.Decl{e.function(name, []*ast.Field{namedField("input", &ast.StarExpr{X: parseExpr(config.InputType)}), namedField("output", &ast.StarExpr{X: parseExpr(config.OutputType)})}, []*ast.Field{{Type: id("error")}}, body)}}
	file.Decls = append([]ast.Decl{(&entityEmitter{l: l}).importDeclaration(file)}, file.Decls...)
	return &MutationOutputAsset{File: file, Function: name}, nil
}
