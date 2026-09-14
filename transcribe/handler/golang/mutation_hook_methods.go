package golang

import (
	"go/ast"
	"go/token"
)

func (e *mutationHookEmitter) method(name string, parameters []*ast.Field, body []ast.Stmt) ast.Decl {
	return &ast.FuncDecl{Name: ast.NewIdent(name), Recv: &ast.FieldList{List: []*ast.Field{namedField(e.receiver, &ast.StarExpr{X: ast.NewIdent(e.asset.TypeName)})}}, Type: &ast.FuncType{Params: &ast.FieldList{List: parameters}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}
}
func (e *mutationHookEmitter) failure(message string) ast.Expr {
	return callExpr(selectExpr(ast.NewIdent(e.l.fmtAlias), "Errorf"), stringExpr(message))
}
func (e *mutationHookEmitter) guard(condition ast.Expr, message string) ast.Stmt {
	return &ast.IfStmt{Cond: condition, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.failure(message))}}}
}
func (e *mutationHookEmitter) commonGuards() []ast.Stmt {
	return []ast.Stmt{
		e.guard(&ast.BinaryExpr{X: ast.NewIdent(e.receiver), Op: token.EQL, Y: ast.NewIdent("nil")}, "mutation hook set is required"),
		e.guard(&ast.BinaryExpr{X: ast.NewIdent(e.context), Op: token.EQL, Y: ast.NewIdent("nil")}, "mutation hook context is required"),
		errorGuard(callExpr(selectExpr(ast.NewIdent(e.context), "Err"))),
	}
}

func (e *mutationHookEmitter) prepare() ast.Decl {
	hooks := ast.NewIdent(e.receiver)
	body := e.commonGuards()
	for _, role := range e.roles {
		if role.bind {
			body = append(body, e.guard(&ast.BinaryExpr{X: ast.NewIdent(e.binder), Op: token.EQL, Y: ast.NewIdent("nil")}, "mutation hook binder is required"))
			break
		}
	}
	body = append(body, e.guard(selectExpr(hooks, "prepareAttempted"), "mutation hooks were already prepared"), assignStmt(selectExpr(hooks, "prepareAttempted"), ast.NewIdent("true")))
	for _, role := range e.roles {
		body = append(body, assignStmt(selectExpr(hooks, role.hookField), callExpr(ast.NewIdent("new"), role.hook)))
		if role.bind {
			body = append(body, e.bindGuard(role))
		}
	}
	body = append(body, assignStmt(selectExpr(hooks, "prepared"), ast.NewIdent("true")), returnStmt(ast.NewIdent("nil")))
	return e.method("Prepare", []*ast.Field{namedField(e.context, selectExpr(ast.NewIdent(e.l.contextAlias), "Context")), namedField(e.binder, selectExpr(ast.NewIdent(e.l.handlerAlias), "Binder"))}, body)
}

func (e *mutationHookEmitter) bindGuard(role mutationHookRole) ast.Stmt {
	call := callExpr(selectExpr(ast.NewIdent(e.binder), "Bind"), ast.NewIdent(e.context), selectExpr(ast.NewIdent(e.receiver), role.hookField))
	return &ast.IfStmt{Init: &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{call}}, Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(callExpr(selectExpr(ast.NewIdent(e.l.fmtAlias), "Errorf"), stringExpr("bind entity hooks "+role.record.plan.Identity+": %w"), ast.NewIdent("err")))}}}
}

func (e *mutationHookEmitter) phase(name string) ast.Decl {
	hooks, frames, frame := ast.NewIdent(e.receiver), ast.NewIdent(e.frames), ast.NewIdent(e.frame)
	body := e.commonGuards()
	body = append(body, e.guard(&ast.UnaryExpr{Op: token.NOT, X: selectExpr(hooks, "prepared")}, "mutation hooks require successful Prepare"), e.guard(&ast.BinaryExpr{X: frames, Op: token.EQL, Y: ast.NewIdent("nil")}, "mutation hook frames are required"))
	if name == "Init" {
		body = append(body, e.guard(selectExpr(hooks, "initAttempted"), "entity Init was already attempted"), assignStmt(selectExpr(hooks, "initAttempted"), ast.NewIdent("true")))
	} else {
		body = append(body, e.guard(&ast.UnaryExpr{Op: token.NOT, X: selectExpr(hooks, "initialized")}, "entity Validate requires successful Init"), e.guard(selectExpr(hooks, "validateAttempted"), "entity Validate was already attempted"), assignStmt(selectExpr(hooks, "validateAttempted"), ast.NewIdent("true")))
	}
	for _, role := range e.roles {
		missing := &ast.BinaryExpr{X: &ast.BinaryExpr{X: frame, Op: token.EQL, Y: ast.NewIdent("nil")}, Op: token.LOR, Y: &ast.BinaryExpr{X: selectExpr(frame, "Entity"), Op: token.EQL, Y: ast.NewIdent("nil")}}
		call := callExpr(selectExpr(selectExpr(hooks, role.hookField), name), ast.NewIdent(e.context), selectExpr(frame, "Entity"), selectExpr(frame, "State"))
		loop := []ast.Stmt{e.guard(missing, "entity hook "+role.record.plan.Identity+" "+name+" requires a non-nil frame and entity"), errorGuard(callExpr(selectExpr(ast.NewIdent(e.context), "Err"))), errorGuard(call)}
		body = append(body, &ast.RangeStmt{Key: ast.NewIdent("_"), Value: frame, Tok: token.DEFINE, X: selectExpr(frames, role.field), Body: &ast.BlockStmt{List: loop}})
	}
	if name == "Init" {
		body = append(body, assignStmt(selectExpr(hooks, "initialized"), ast.NewIdent("true")))
	} else {
		body = append(body, assignStmt(selectExpr(hooks, "validated"), ast.NewIdent("true")))
	}
	body = append(body, returnStmt(ast.NewIdent("nil")))
	return e.method(name, []*ast.Field{namedField(e.context, selectExpr(ast.NewIdent(e.l.contextAlias), "Context")), namedField(e.frames, &ast.StarExpr{X: ast.NewIdent(e.asset.FramesType)})}, body)
}
