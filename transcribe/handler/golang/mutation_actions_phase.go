package golang

import (
	"go/ast"
	"go/token"
	"strconv"
)

func (e *actionEmitter) phase(name string, stage int, body []ast.Stmt) ast.Decl {
	actions, frames, nilExpr := ast.NewIdent("actions"), ast.NewIdent("frames"), ast.NewIdent("nil")
	guard := []ast.Stmt{
		&ast.IfStmt{Cond: &ast.BinaryExpr{X: actions, Op: token.EQL, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation action state is required"))}}},
		&ast.IfStmt{Cond: &ast.BinaryExpr{X: selectExpr(actions, "failed"), Op: token.LOR, Y: &ast.BinaryExpr{X: selectExpr(actions, "stage"), Op: token.NEQ, Y: &ast.BasicLit{Kind: token.INT, Value: strconv.Itoa(stage)}}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation phase " + name + " is out of order or already failed"))}}},
		assignStmt(selectExpr(actions, "failed"), ast.NewIdent("true")),
		&ast.IfStmt{Cond: &ast.BinaryExpr{X: frames, Op: token.EQL, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation frames are required"))}}},
		&ast.IfStmt{Cond: e.isNil(ast.NewIdent("ctx")), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation context is required"))}}},
		errorGuard(callExpr(selectExpr(ast.NewIdent("ctx"), "Err"))),
	}
	if stage == 1 {
		guard = append(guard, assignStmt(selectExpr(actions, "frames"), frames))
	} else {
		guard = append(guard, &ast.IfStmt{Cond: &ast.BinaryExpr{X: selectExpr(actions, "frames"), Op: token.NEQ, Y: frames}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation phases require the same prepared frames"))}}})
	}
	guard = append(guard, errorGuard(callExpr(selectExpr(frames, e.verify), selectExpr(actions, "Input"))))
	guard = append(guard, body...)
	guard = append(guard, assignStmt(selectExpr(actions, "stage"), &ast.BasicLit{Kind: token.INT, Value: strconv.Itoa(stage + 1)}), assignStmt(selectExpr(actions, "failed"), ast.NewIdent("false")), returnStmt(nilExpr))
	return e.method(name, []*ast.Field{namedField("ctx", selectExpr(ast.NewIdent(e.l.contextAlias), "Context")), namedField("frames", &ast.StarExpr{X: ast.NewIdent(e.frames.TypeName)})}, []*ast.Field{{Type: ast.NewIdent("error")}}, guard)
}

func (e *actionEmitter) original(role actionRole) []ast.Stmt {
	frame, nilExpr := ast.NewIdent("frame"), ast.NewIdent("nil")
	invalid := &ast.BinaryExpr{X: &ast.BinaryExpr{X: frame, Op: token.EQL, Y: nilExpr}, Op: token.LOR, Y: &ast.BinaryExpr{X: selectExpr(frame, "Entity"), Op: token.EQL, Y: nilExpr}}
	return []ast.Stmt{
		&ast.IfStmt{Cond: invalid, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation requires non-nil frame and entity"))}}},
		&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("original"), ast.NewIdent("ok")}, Tok: token.DEFINE, Rhs: []ast.Expr{&ast.TypeAssertExpr{X: selectExpr(selectExpr(frame, "State"), "Original"), Type: &ast.StarExpr{X: ast.NewIdent(role.association.StateType)}}}},
		&ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.UnaryExpr{Op: token.NOT, X: ast.NewIdent("ok")}, Op: token.LOR, Y: &ast.BinaryExpr{X: ast.NewIdent("original"), Op: token.EQL, Y: nilExpr}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation requires the captured role original state"))}}},
		&ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(ast.NewIdent("original"), "Available"))}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("original presence is unavailable for mutation"))}}},
	}
}

func (e *actionEmitter) originalKey(role actionRole, key, supplied string) []ast.Stmt {
	adapter := &ast.ParenExpr{X: &ast.CompositeLit{Type: ast.NewIdent(role.association.KeyAdapterType)}}
	method := "Key"
	lhs := []ast.Expr{ast.NewIdent(key), ast.NewIdent(supplied)}
	if e.entities.identity != nil {
		method = "Identity"
		lhs = append(lhs, ast.NewIdent("insertOnly"))
	}
	lhs = append(lhs, ast.NewIdent("err"))
	return []ast.Stmt{&ast.AssignStmt{Lhs: lhs, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(adapter, method), ast.NewIdent("original"))}}, errorGuard(ast.NewIdent("err"))}
}

func (e *actionEmitter) frameLoop(role actionRole, body []ast.Stmt) ast.Stmt {
	return &ast.RangeStmt{Key: ast.NewIdent("_"), Value: ast.NewIdent("frame"), Tok: token.DEFINE, X: selectExpr(ast.NewIdent("frames"), role.frame.Field), Body: &ast.BlockStmt{List: body}}
}
