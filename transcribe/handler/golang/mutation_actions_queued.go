package golang

import (
	"go/ast"
	"go/token"
)

// verifyQueued is observational: it compares queued working state, not the
// UPDATE DTO whose identity marker bits were intentionally cleared.
func (e *actionEmitter) verifyQueued() ast.Decl {
	actions, frames, nilExpr := ast.NewIdent("actions"), ast.NewIdent("frames"), ast.NewIdent("nil")
	body := []ast.Stmt{
		&ast.IfStmt{Cond: &ast.BinaryExpr{X: actions, Op: token.EQL, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation action state is required"))}}},
		&ast.IfStmt{Cond: &ast.BinaryExpr{X: selectExpr(actions, "failed"), Op: token.LOR, Y: &ast.BinaryExpr{X: selectExpr(actions, "stage"), Op: token.NEQ, Y: &ast.BasicLit{Kind: token.INT, Value: "5"}}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("queued verification requires a successful queue phase"))}}},
		&ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.BinaryExpr{X: frames, Op: token.EQL, Y: nilExpr}, Op: token.LOR, Y: &ast.BinaryExpr{X: frames, Op: token.NEQ, Y: selectExpr(actions, "frames")}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("queued verification requires the prepared frames"))}}},
		&ast.IfStmt{Cond: e.isNil(ast.NewIdent("ctx")), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation context is required"))}}},
		errorGuard(callExpr(selectExpr(ast.NewIdent("ctx"), "Err"))),
		errorGuard(callExpr(selectExpr(frames, e.verify), selectExpr(actions, "Input"))),
	}
	for _, role := range e.roles {
		block := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("options"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(ast.NewIdent(role.entry + "PayloadOptions"))}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}}}
		current := selectExpr(ast.NewIdent("frame"), "Entity")
		loop := []ast.Stmt{defineStmt("baseline", &ast.IndexExpr{X: selectExpr(actions, role.field+"QueuedState"), Index: current}), &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("baseline"), Op: token.EQL, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("queued working snapshot is missing"))}}}, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("cloned"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{e.runtimeCall("CloneValue", current, ast.NewIdent("options"))}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}}}
		equal := callExpr(selectExpr(ast.NewIdent(e.l.importsByPath["reflect"]), "DeepEqual"), ast.NewIdent("cloned"), ast.NewIdent("baseline"))
		loop = append(loop, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: equal}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("entity values or markers changed after queue"))}}})
		block = append(block, e.frameLoop(role, loop))
		body = append(body, &ast.BlockStmt{List: block})
	}
	body = append(body, returnStmt(nilExpr))
	return e.method("VerifyQueued", []*ast.Field{namedField("ctx", selectExpr(ast.NewIdent(e.l.contextAlias), "Context")), namedField("frames", &ast.StarExpr{X: ast.NewIdent(e.frames.TypeName)})}, []*ast.Field{{Type: ast.NewIdent("error")}}, body)
}
