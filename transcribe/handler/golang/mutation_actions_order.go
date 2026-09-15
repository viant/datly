package golang

import (
	"go/ast"
	"go/token"
	"strconv"
)

// verifyOrder validates the complete typed decision log before the first queue
// operation. A malformed order can never publish a valid prefix of writes.
func (e *actionEmitter) verifyOrder() []ast.Stmt {
	body := []ast.Stmt{defineStmt("expected", &ast.BasicLit{Kind: token.INT, Value: "0"})}
	cases := []ast.Stmt{}
	for _, role := range e.roles {
		entries := selectExpr(ast.NewIdent("actions"), role.field)
		body = append(body, defineStmt(role.field+"Seen", callExpr(ast.NewIdent("make"), &ast.ArrayType{Elt: ast.NewIdent("bool")}, callExpr(ast.NewIdent("len"), entries))), &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("expected")}, Tok: token.ADD_ASSIGN, Rhs: []ast.Expr{callExpr(ast.NewIdent("len"), entries)}})
		index := selectExpr(ast.NewIdent("visit"), "Index")
		invalid := &ast.BinaryExpr{X: &ast.BinaryExpr{X: index, Op: token.LSS, Y: &ast.BasicLit{Kind: token.INT, Value: "0"}}, Op: token.LOR, Y: &ast.BinaryExpr{X: index, Op: token.GEQ, Y: callExpr(ast.NewIdent("len"), entries)}}
		seen := &ast.IndexExpr{X: ast.NewIdent(role.field + "Seen"), Index: index}
		entry := &ast.IndexExpr{X: entries, Index: index}
		statements := []ast.Stmt{&ast.IfStmt{Cond: invalid, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation order has an invalid row index"))}}}, &ast.IfStmt{Cond: seen, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation order repeats a row"))}}}, assignStmt(seen, ast.NewIdent("true")), &ast.IfStmt{Cond: &ast.BinaryExpr{X: entry, Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation decision is missing"))}}}}
		validAction := &ast.BinaryExpr{X: &ast.BinaryExpr{X: selectExpr(entry, "Action"), Op: token.EQL, Y: selectExpr(ast.NewIdent(e.l.handlerAlias), "WriteInsert")}, Op: token.LOR, Y: &ast.BinaryExpr{X: selectExpr(entry, "Action"), Op: token.EQL, Y: selectExpr(ast.NewIdent(e.l.handlerAlias), "WriteUpdate")}}
		validAction = &ast.BinaryExpr{X: validAction, Op: token.LOR, Y: &ast.BinaryExpr{X: selectExpr(entry, "Action"), Op: token.EQL, Y: selectExpr(ast.NewIdent(e.l.handlerAlias), "WriteDelete")}}
		statements = append(statements, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: &ast.ParenExpr{X: validAction}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation decision has an invalid action"))}}})
		cases = append(cases, &ast.CaseClause{List: []ast.Expr{&ast.BasicLit{Kind: token.INT, Value: strconv.Itoa(role.record.order)}}, Body: statements})
	}
	cases = append(cases, &ast.CaseClause{Body: []ast.Stmt{returnStmt(e.errorExpr("mutation order has an unknown role"))}})
	order := selectExpr(ast.NewIdent("frames"), e.frames.OrderField)
	body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: callExpr(ast.NewIdent("len"), order), Op: token.NEQ, Y: ast.NewIdent("expected")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation order does not cover every decision"))}}}, &ast.RangeStmt{Key: ast.NewIdent("_"), Value: ast.NewIdent("visit"), Tok: token.DEFINE, X: order, Body: &ast.BlockStmt{List: []ast.Stmt{&ast.SwitchStmt{Tag: selectExpr(ast.NewIdent("visit"), "Role"), Body: &ast.BlockStmt{List: cases}}}}})
	return body
}
