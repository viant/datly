package golang

import (
	"go/ast"
	"go/token"
)

func (e *validationEmitter) producerPolicy(role actionRole, final bool) []ast.Stmt {
	a, id := e.action, ast.NewIdent
	slot := &ast.IndexExpr{X: e.member(role.field + "Policies"), Index: id("frame")}
	if !final {
		var body []ast.Stmt
		if a.entities.identity != nil {
			insert := &ast.BinaryExpr{X: selectExpr(id("options"), "Action"), Op: token.EQL, Y: selectExpr(id(a.l.handlerAlias), "WriteInsert")}
			collect := []ast.Stmt{defineStmt("deferred", &ast.CompositeLit{Type: id(e.fields)})}
			for _, field := range role.record.plan.Entity.Fields {
				if field.Relation {
					continue
				}
				absent := &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(id("original"), "Has"), stringExpr(field.Name))}
				collect = append(collect, &ast.IfStmt{Cond: absent, Body: &ast.BlockStmt{List: []ast.Stmt{
					&ast.AssignStmt{Lhs: []ast.Expr{id("produced"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(id("original"), "Produced"), stringExpr(field.Name))}}, errorGuard(id("err")),
					&ast.IfStmt{Cond: id("produced"), Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(&ast.IndexExpr{X: id("deferred"), Index: stringExpr(field.Name)}, id("true"))}}},
				}}})
			}
			collect = append(collect, &ast.IfStmt{Cond: &ast.BinaryExpr{X: callExpr(id("len"), id("deferred")), Op: token.GTR, Y: &ast.BasicLit{Kind: token.INT, Value: "0"}}, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(selectExpr(id("options"), "DeferredFields"), id("deferred"))}}})
			body = append(body, &ast.IfStmt{Cond: insert, Body: &ast.BlockStmt{List: collect}})
		}
		return append(body, assignStmt(slot, id("options")))
	}
	body := []ast.Stmt{
		&ast.AssignStmt{Lhs: []ast.Expr{id("before"), id("found")}, Tok: token.DEFINE, Rhs: []ast.Expr{slot}},
		e.guard(&ast.UnaryExpr{Op: token.NOT, X: id("found")}, "final validation frame was not business validated"),
		e.guard(&ast.BinaryExpr{X: selectExpr(id("before"), "Action"), Op: token.NEQ, Y: selectExpr(id("options"), "Action")}, "mutation action changed after business validation"),
		assignStmt(selectExpr(id("options"), "Previous"), selectExpr(id("before"), "Previous")),
		assignStmt(selectExpr(id("options"), "PreviousFields"), selectExpr(id("before"), "PreviousFields")),
		assignStmt(selectExpr(id("options"), "Location"), selectExpr(id("before"), "Location")),
	}
	// Final coverage adds reconciled marks without losing the business pass's
	// effective coverage. Availability is intentionally not carried forward.
	var union []ast.Stmt
	for _, field := range role.record.plan.Entity.Fields {
		if field.Relation {
			continue
		}
		at := &ast.IndexExpr{X: id("coverage"), Index: stringExpr(field.Name)}
		union = append(union, assignStmt(at, &ast.BinaryExpr{X: at, Op: token.LOR, Y: callExpr(selectExpr(selectExpr(id("before"), "Fields"), "Has"), stringExpr(field.Name))}))
	}
	body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: selectExpr(id("options"), "Action"), Op: token.EQL, Y: selectExpr(id(a.l.handlerAlias), "WriteUpdate")}, Body: &ast.BlockStmt{List: union}})
	return body
}
