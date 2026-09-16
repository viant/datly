package golang

import (
	"go/ast"
	"go/token"
)

// optionalPhase delegates through focused public capabilities without creating
// another hook instance. The surrounding Program owns the intervening actions.
func (e *mutationHookEmitter) optionalPhase(name, contract, prerequisite, attempted, completed string) ast.Decl {
	id := ast.NewIdent
	hooks, frames, frame := id(e.receiver), id(e.frames), id(e.frame)
	body := e.commonGuards()
	body = append(body, e.guard(&ast.UnaryExpr{Op: token.NOT, X: selectExpr(hooks, prerequisite)}, name+" requires the prior hook phase to complete"), e.guard(&ast.BinaryExpr{X: frames, Op: token.EQL, Y: id("nil")}, "mutation hook frames are required"), e.guard(selectExpr(hooks, attempted), name+" was already attempted"), assignStmt(selectExpr(hooks, attempted), id("true")))
	for _, role := range e.roles {
		var parent ast.Expr = selectExpr(id(e.l.handlerAlias), "NoParent")
		if role.parent != nil {
			parent = parseExpr(role.parent.value.base)
		}
		capability := &ast.IndexListExpr{X: selectExpr(id(e.l.handlerAlias), contract), Indices: []ast.Expr{parseExpr(role.record.value.base), parent, parseExpr(e.l.config.OutputType)}}
		assertion := &ast.TypeAssertExpr{X: callExpr(id("any"), selectExpr(hooks, role.hookField)), Type: capability}
		missing := &ast.BinaryExpr{X: &ast.BinaryExpr{X: frame, Op: token.EQL, Y: id("nil")}, Op: token.LOR, Y: &ast.BinaryExpr{X: selectExpr(frame, "Entity"), Op: token.EQL, Y: id("nil")}}
		lifecycleType := &ast.IndexListExpr{X: selectExpr(id(e.l.handlerAlias), "LifecycleContext"), Indices: []ast.Expr{parseExpr(role.record.value.base), parent, parseExpr(e.l.config.OutputType)}}
		lifecycle := &ast.CompositeLit{Type: lifecycleType, Elts: []ast.Expr{&ast.KeyValueExpr{Key: id("EntityState"), Value: selectExpr(frame, "State")}, &ast.KeyValueExpr{Key: id("Output"), Value: selectExpr(hooks, "output")}}}
		loop := []ast.Stmt{e.guard(missing, "entity hook "+name+" requires an entity frame"), errorGuard(callExpr(selectExpr(id(e.context), "Err"))), errorGuard(callExpr(selectExpr(id("callback"), name), id(e.context), selectExpr(frame, "Entity"), lifecycle))}
		body = append(body, &ast.IfStmt{Init: &ast.AssignStmt{Lhs: []ast.Expr{id("callback"), id("ok")}, Tok: token.DEFINE, Rhs: []ast.Expr{assertion}}, Cond: id("ok"), Body: &ast.BlockStmt{List: []ast.Stmt{&ast.RangeStmt{Key: id("_"), Value: frame, Tok: token.DEFINE, X: selectExpr(frames, role.field), Body: &ast.BlockStmt{List: loop}}}}})
	}
	if completed != "" {
		body = append(body, assignStmt(selectExpr(hooks, completed), id("true")))
	}
	body = append(body, returnStmt(id("nil")))
	return e.method(name, []*ast.Field{namedField(e.context, selectExpr(id(e.l.contextAlias), "Context")), namedField(e.frames, &ast.StarExpr{X: id(e.asset.FramesType)})}, body)
}
