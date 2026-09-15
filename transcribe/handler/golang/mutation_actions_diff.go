package golang

import (
	"fmt"
	"go/ast"
	"go/token"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

func (e *actionEmitter) actionValue(role actionRole, action plan.Action) (ast.Expr, error) {
	found := false
	for _, allowed := range role.record.plan.Write.Allowed {
		found = found || allowed == action
	}
	if !found {
		return nil, fmt.Errorf("action %s is not allowed for %s", action, role.record.plan.Identity)
	}
	switch action {
	case plan.ActionInsert:
		return selectExpr(ast.NewIdent(e.l.handlerAlias), "WriteInsert"), nil
	case plan.ActionUpdate:
		return selectExpr(ast.NewIdent(e.l.handlerAlias), "WriteUpdate"), nil
	}
	return nil, fmt.Errorf("unsupported mutation action %s", action)
}

// actionSelection is shared by pre-write validation and later diffing. It uses
// the immutable operation policy and the already identity-matched Previous row,
// never current identifier values or zero tests.
func (e *actionEmitter) actionSelection(role actionRole) ([]ast.Stmt, ast.Expr, error) {
	body, action, err := e.normalActionSelection(role)
	if err != nil {
		return nil, nil, err
	}
	for _, field := range role.record.plan.Entity.Fields {
		if !field.DeleteMarker {
			continue
		}
		body = append(body, defineStmt("selectedAction", action))
		action = ast.NewIdent("selectedAction")
		selection, err := e.deleteSelection(role, action)
		if err != nil {
			return nil, nil, err
		}
		body = append(body, selection...)
	}
	return body, action, nil
}

func (e *actionEmitter) normalActionSelection(role actionRole) ([]ast.Stmt, ast.Expr, error) {
	switch e.l.plan.Operation {
	case plan.OperationPost:
		action, err := e.actionValue(role, role.record.plan.Write.Missing)
		return e.entities.identity.verifyAction(e, action), action, err
	case plan.OperationPut:
		action, err := e.actionValue(role, role.record.plan.Write.Existing)
		return e.entities.identity.verifyAction(e, action), action, err
	case plan.OperationPatch:
		missing, err := e.actionValue(role, role.record.plan.Write.Missing)
		if err != nil {
			return nil, nil, err
		}
		existing, err := e.actionValue(role, role.record.plan.Write.Existing)
		if err != nil {
			return nil, nil, err
		}
		body := []ast.Stmt{defineStmt("action", missing), &ast.IfStmt{Cond: &ast.BinaryExpr{X: selectExpr(selectExpr(ast.NewIdent("frame"), "State"), "Previous"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{&ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: ast.NewIdent("supplied")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("database match requires a complete resolved identity"))}}}, assignStmt(ast.NewIdent("action"), existing)}}}}
		body = append(body, e.entities.identity.verifyAction(e, ast.NewIdent("action"))...)
		return body, ast.NewIdent("action"), nil
	default:
		return nil, nil, fmt.Errorf("unsupported mutation operation %s", e.l.plan.Operation)
	}
}

func (e *actionEmitter) diff() (ast.Decl, error) {
	body, err := e.verifyBusiness()
	if err != nil {
		return nil, err
	}
	for _, role := range e.roles {
		entries := ast.NewIdent(role.field + "Decisions")
		body = append(body, defineStmt(entries.Name, &ast.CompositeLit{Type: &ast.ArrayType{Elt: &ast.StarExpr{X: ast.NewIdent(role.entry)}}}))
		loop := e.original(role)
		loop = append(loop, e.decisionIdentity(role, "key", "supplied")...)
		selection, action, err := e.actionSelection(role)
		if err != nil {
			return nil, err
		}
		loop = append(loop, selection...)
		ready, err := e.insertReadiness(role)
		if err != nil {
			return nil, err
		}
		if len(ready) > 0 {
			loop = append(loop, &ast.IfStmt{Cond: &ast.BinaryExpr{X: action, Op: token.EQL, Y: selectExpr(ast.NewIdent(e.l.handlerAlias), "WriteInsert")}, Body: &ast.BlockStmt{List: ready}})
		}
		entry := &ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: ast.NewIdent(role.entry), Elts: []ast.Expr{
			&ast.KeyValueExpr{Key: ast.NewIdent("Frame"), Value: ast.NewIdent("frame")},
			&ast.KeyValueExpr{Key: ast.NewIdent("IdentityKey"), Value: ast.NewIdent("key")},
			&ast.KeyValueExpr{Key: ast.NewIdent("IdentityAssigned"), Value: ast.NewIdent("supplied")},
			&ast.KeyValueExpr{Key: ast.NewIdent("Action"), Value: action},
		}}}
		loop = append(loop, assignStmt(entries, callExpr(ast.NewIdent("append"), entries, entry)))
		body = append(body, e.frameLoop(role, loop))
	}
	for _, role := range e.roles {
		body = append(body, assignStmt(selectExpr(ast.NewIdent("actions"), role.field), ast.NewIdent(role.field+"Decisions")))
	}
	return e.phase("Diff", 2, body), nil
}
