package golang

import (
	"go/ast"
	"go/token"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

func (e *readIndexEmitter) captureRead(read plan.ReadCollection, row, slice string) ([]ast.Stmt, error) {
	id := ast.NewIdent
	nilExpr := id("nil")
	path := strings.Join(read.InputPath[1:], ".")
	body := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{id("projection"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(id("metadata"), "Projection"), stringExpr(path))}}, e.guardError(nilExpr), &ast.IfStmt{Cond: e.isNil(id("projection")), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, e.errorExpr("application read projection is unavailable: "+path))}}}}
	body = append(body, e.accessor(parseExpr(e.l.config.InputType), path, "inputAccess")...)
	body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{id("carrier"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(id("inputAccess"), "Get"), id("input"))}}, e.guardError(nilExpr), defineStmt("value", callExpr(selectExpr(id("carrier"), "Interface"))))
	f := &frameEmitter{previousEmitter: e.previousEmitter}
	record := &recordLowering{current: recordShape{base: row, pointer: true, many: true}}
	body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: callExpr(selectExpr(id("projection"), "DirectOutput")), Op: token.LAND, Y: &ast.BinaryExpr{X: callExpr(selectExpr(id("projection"), "RootHolder")), Op: token.NEQ, Y: stringExpr("")}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, e.errorExpr("direct application read cannot declare a root holder"))}}})
	body = append(body, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(id("projection"), "DirectOutput"))}, Body: &ast.BlockStmt{List: f.unwrapCarrier(record)}})
	collection := &ast.ParenExpr{X: &ast.CompositeLit{Type: &ast.IndexExpr{X: selectExpr(id(e.shape), "Collection"), Index: parseExpr(row)}}}
	body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{id("rows"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(collection, "Pointers"), id("value"))}}, e.guardError(nilExpr))
	// Read projection evidence is checked before exposing any scalar lookups.
	loop := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("row"), Op: token.EQL, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{&ast.BranchStmt{Tok: token.CONTINUE}}}}, &ast.AssignStmt{Lhs: []ast.Expr{id("loaded"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(id("projection"), "Fields"), id("ordinal"))}}, e.guardError(nilExpr)}
	for _, field := range read.Fields {
		loop = append(loop, &ast.IfStmt{Cond: &ast.BinaryExpr{X: e.isNil(id("loaded")), Op: token.LOR, Y: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(id("loaded"), "Has"), stringExpr(field.Field))}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, e.errorExpr("application index field was not loaded: "+path+"."+field.Field))}}})
	}
	body = append(body, &ast.RangeStmt{Key: id("ordinal"), Value: id("row"), Tok: token.DEFINE, X: id("rows"), Body: &ast.BlockStmt{List: loop}})
	runtime := &ast.ParenExpr{X: &ast.CompositeLit{Type: selectExpr(id(e.shape), "Runtime")}}
	body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{id("cloned"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(runtime, "CloneValue"), id("rows"), &ast.CompositeLit{Type: selectExpr(id(e.shape), "CloneOptions")})}}, e.guardError(nilExpr), assignStmt(selectExpr(id("result"), read.Name), callExpr(id(slice), &ast.TypeAssertExpr{X: id("cloned"), Type: &ast.ArrayType{Elt: &ast.StarExpr{X: parseExpr(row)}}})))
	return body, nil
}
