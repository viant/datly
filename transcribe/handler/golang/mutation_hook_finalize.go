package golang

import (
	"go/ast"
	"go/token"
)

func (e *mutationHookEmitter) finalize() ast.Decl {
	id := ast.NewIdent
	hooks := id(e.receiver)
	body := []ast.Stmt{e.guard(&ast.BinaryExpr{X: hooks, Op: token.EQL, Y: id("nil")}, "mutation hook set is required"), e.guard(selectExpr(hooks, "finalizeAttempted"), "mutation hooks were already finalized"), assignStmt(selectExpr(hooks, "finalizeAttempted"), id("true"))}
	// No context cancellation guard: failed/cancelled operations also finalize.
	body = append(body, e.guard(&ast.BinaryExpr{X: id(e.context), Op: token.EQL, Y: id("nil")}, "mutation finalization context is required"), &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: selectExpr(hooks, "prepared")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("nil"))}}})
	for _, role := range e.roles {
		if role.record.plan != e.l.plan.Root {
			continue
		}
		alias := e.l.availableAlias("mutation")
		e.l.pathsByAlias[alias] = "github.com/viant/xdatly/handler/mutation"
		capability := &ast.IndexListExpr{X: selectExpr(id(alias), "Finalizer"), Indices: []ast.Expr{parseExpr(e.l.config.InputType), parseExpr(e.l.config.OutputType)}}
		assertion := &ast.TypeAssertExpr{X: callExpr(id("any"), selectExpr(hooks, role.hookField)), Type: capability}
		invoke := callExpr(selectExpr(id("finalizer"), "Finalize"), id(e.context), id("input"), id("output"), callExpr(selectExpr(id("outcome"), "Clone")))
		body = append(body, &ast.IfStmt{Init: &ast.AssignStmt{Lhs: []ast.Expr{id("finalizer"), id("ok")}, Tok: token.DEFINE, Rhs: []ast.Expr{assertion}}, Cond: id("ok"), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(invoke)}}})
		break
	}
	body = append(body, returnStmt(id("nil")))
	return e.method("Finalize", []*ast.Field{namedField(e.context, selectExpr(id(e.l.contextAlias), "Context")), namedField("input", &ast.StarExpr{X: parseExpr(e.l.config.InputType)}), namedField("output", &ast.StarExpr{X: parseExpr(e.l.config.OutputType)}), namedField("outcome", selectExpr(id(e.l.handlerAlias), "Outcome"))}, body)
}
