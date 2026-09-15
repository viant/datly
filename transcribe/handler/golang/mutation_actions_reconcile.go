package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

func (e *actionEmitter) runtimeCall(method string, args ...ast.Expr) *ast.CallExpr {
	return callExpr(selectExpr(&ast.ParenExpr{X: &ast.CompositeLit{Type: selectExpr(ast.NewIdent(e.shape), "Runtime")}}, method), args...)
}
func (e *actionEmitter) accessor(record *recordLowering, path, name string) []ast.Stmt {
	reflectAlias := e.l.importsByPath["reflect"]
	rowType := callExpr(selectExpr(callExpr(selectExpr(ast.NewIdent(reflectAlias), "TypeOf"), callExpr(record.value.pointerExpr(), ast.NewIdent("nil"))), "Elem"))
	return []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent(name), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(callExpr(selectExpr(ast.NewIdent(e.shape), "Linked"), rowType), "Accessor"), stringExpr(path))}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}}}
}
func (e *actionEmitter) assignment(accessor, value ast.Expr) ast.Expr {
	return &ast.CompositeLit{Type: selectExpr(ast.NewIdent(e.shape), "FieldValue"), Elts: []ast.Expr{&ast.KeyValueExpr{Key: ast.NewIdent("Accessor"), Value: accessor}, &ast.KeyValueExpr{Key: ast.NewIdent("Value"), Value: value}}}
}
func (e *actionEmitter) fieldPath(role actionRole, name string) (string, error) {
	for _, field := range role.record.plan.Entity.Fields {
		if field.Name == name {
			return strings.Join((&entityEmitter{l: e.l}).entityFieldPath(role.record, name), "."), nil
		}
	}
	return "", fmt.Errorf("mutation field %s is absent from entity role %s", name, role.record.plan.Identity)
}
func (e *actionEmitter) parent(role actionRole) (*actionRole, *plan.RelationPlan) {
	for index := range e.roles {
		for _, relation := range e.roles[index].record.plan.Relations {
			if relation.Child == role.record.plan {
				return &e.roles[index], relation
			}
		}
	}
	return nil, nil
}

func (e *actionEmitter) reconcile() (ast.Decl, error) {
	body, err := e.verifyBusiness()
	if err != nil {
		return nil, err
	}
	for _, role := range e.roles {
		if role.association.CurrentKeyFunction == "" {
			return nil, fmt.Errorf("mutation role %s requires captured current-key accessor metadata", role.record.plan.Identity)
		}
		indexType := &ast.MapType{Key: role.record.value.pointerExpr(), Value: role.record.value.pointerExpr()}
		body = append(body, defineStmt(role.field+"Payloads", callExpr(ast.NewIdent("make"), indexType)))
		decisions := selectExpr(ast.NewIdent("actions"), role.field+"ByEntity")
		body = append(body, assignStmt(decisions, callExpr(ast.NewIdent("make"), &ast.MapType{Key: role.record.value.pointerExpr(), Value: &ast.StarExpr{X: ast.NewIdent(role.entry)}})))
		block := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("options"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(ast.NewIdent(role.entry + "PayloadOptions"))}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}}}
		clone := e.runtimeCall("CloneValue", selectExpr(selectExpr(ast.NewIdent("entry"), "Frame"), "Entity"), ast.NewIdent("options"))
		loop := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("cloned"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{clone}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}}, assignStmt(selectExpr(ast.NewIdent("entry"), "Payload"), &ast.TypeAssertExpr{X: ast.NewIdent("cloned"), Type: role.record.value.pointerExpr()}), assignStmt(&ast.IndexExpr{X: ast.NewIdent(role.field + "Payloads"), Index: selectExpr(selectExpr(ast.NewIdent("entry"), "Frame"), "Entity")}, selectExpr(ast.NewIdent("entry"), "Payload"))}
		restore, err := e.restoreIdentity(role)
		if err != nil {
			return nil, err
		}
		loop = append(loop, restore...)
		loop = append(loop, assignStmt(&ast.IndexExpr{X: decisions, Index: selectExpr(selectExpr(ast.NewIdent("entry"), "Frame"), "Entity")}, ast.NewIdent("entry")))
		block = append(block, e.decisionLoop(role, loop))
		body = append(body, &ast.BlockStmt{List: block})
	}
	for _, role := range e.roles {
		links, err := e.reconcileLinks(role)
		if err != nil {
			return nil, err
		}
		if len(links) > 0 {
			body = append(body, e.decisionLoop(role, links))
		}
	}
	identityOwners := map[mutationEntityKey]actionRole{}
	for _, role := range e.roles {
		if len(role.record.plan.IdentityKeys()) == 0 {
			continue
		}
		owner, shared := identityOwners[role.entityKey()]
		if !shared {
			owner = role
			identityOwners[role.entityKey()] = role
			body = append(body, defineStmt(owner.field+"Identities", callExpr(ast.NewIdent("make"), &ast.MapType{Key: ast.NewIdent(owner.association.KeyType), Value: owner.record.value.pointerExpr()})))
		}
		identityKey, err := e.sharedIdentityKey(owner, role)
		if err != nil {
			return nil, err
		}
		original := &ast.TypeAssertExpr{X: selectExpr(selectExpr(selectExpr(ast.NewIdent("entry"), "Frame"), "State"), "Original"), Type: &ast.StarExpr{X: ast.NewIdent(role.association.StateType)}}
		loop := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("key"), ast.NewIdent("valid"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(ast.NewIdent(role.association.CurrentKeyFunction), selectExpr(ast.NewIdent("entry"), "Payload"), original)}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}}, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: ast.NewIdent("valid")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("reconciled mutation identity is missing"))}}}}
		changed := &ast.BinaryExpr{X: &ast.BinaryExpr{X: selectExpr(ast.NewIdent("entry"), "Action"), Op: token.NEQ, Y: selectExpr(ast.NewIdent(e.l.handlerAlias), "WriteInsert")}, Op: token.LAND, Y: &ast.BinaryExpr{X: ast.NewIdent("key"), Op: token.NEQ, Y: selectExpr(ast.NewIdent("entry"), "IdentityKey")}}
		loop = append(loop, &ast.IfStmt{Cond: changed, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("parent linking cannot change an existing identity"))}}})
		if e.entities.identity != nil {
			for _, part := range role.record.plan.IdentityKeys() {
				supplied := selectExpr(selectExpr(selectExpr(ast.NewIdent("entry"), "Frame"), "identityKnown"), part.Field)
				differs := &ast.BinaryExpr{X: selectExpr(ast.NewIdent("key"), part.Field), Op: token.NEQ, Y: selectExpr(selectExpr(ast.NewIdent("entry"), "IdentityKey"), part.Field)}
				loop = append(loop, &ast.IfStmt{Cond: &ast.BinaryExpr{X: supplied, Op: token.LAND, Y: differs}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("parent linking cannot change a frozen resolved identity part"))}}})
			}
		}
		index := &ast.IndexExpr{X: ast.NewIdent(owner.field + "Identities"), Index: identityKey}
		loop = append(loop, &ast.IfStmt{Init: defineStmt("prior", index), Cond: &ast.BinaryExpr{X: ast.NewIdent("prior"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("reconciled mutation identities are duplicated"))}}}, assignStmt(index, selectExpr(ast.NewIdent("entry"), "Payload")))
		body = append(body, e.decisionLoop(role, loop))
	}
	for _, role := range e.roles {
		publish, err := e.publish(role)
		if err != nil {
			return nil, err
		}
		if len(publish) > 0 {
			body = append(body, e.decisionLoop(role, publish))
		}
	}
	return e.phase("Reconcile", 3, body), nil
}

func (e *actionEmitter) decisionLoop(role actionRole, body []ast.Stmt) ast.Stmt {
	return &ast.RangeStmt{Key: ast.NewIdent("_"), Value: ast.NewIdent("entry"), Tok: token.DEFINE, X: selectExpr(ast.NewIdent("actions"), role.field), Body: &ast.BlockStmt{List: body}}
}

func (e *actionEmitter) restoreIdentity(role actionRole) ([]ast.Stmt, error) {
	update := &ast.BinaryExpr{X: selectExpr(ast.NewIdent("entry"), "Action"), Op: token.NEQ, Y: selectExpr(ast.NewIdent(e.l.handlerAlias), "WriteInsert")}
	statements := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: update, Op: token.LAND, Y: &ast.UnaryExpr{Op: token.NOT, X: selectExpr(ast.NewIdent("entry"), "IdentityAssigned")}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("update requires a resolved identity"))}}}}
	if e.entities.identity != nil {
		return statements, nil
	}
	for index, key := range role.record.plan.IdentityKeys() {
		path, err := e.fieldPath(role, key.Field)
		if err != nil {
			return nil, err
		}
		name := "identity" + strconv.Itoa(index)
		part := e.accessor(role.record, path, name)
		var value ast.Expr = selectExpr(selectExpr(ast.NewIdent("entry"), "IdentityKey"), key.Field)
		if key.Type.Pointer || strings.HasPrefix(key.Type.Name, "*") {
			part = append(part, defineStmt(name+"Value", value))
			value = &ast.UnaryExpr{Op: token.AND, X: ast.NewIdent(name + "Value")}
		}
		part = append(part, errorGuard(e.runtimeCall("AssignFields", selectExpr(ast.NewIdent("entry"), "Payload"), e.assignment(ast.NewIdent(name), value))))
		var restore ast.Expr = update
		if e.entities.identity != nil {
			original := selectExpr(selectExpr(selectExpr(ast.NewIdent("entry"), "Frame"), "State"), "Original")
			insert := &ast.BinaryExpr{X: selectExpr(ast.NewIdent("entry"), "Action"), Op: token.EQL, Y: selectExpr(ast.NewIdent(e.l.handlerAlias), "WriteInsert")}
			restore = &ast.BinaryExpr{X: update, Op: token.LOR, Y: &ast.BinaryExpr{X: insert, Op: token.LAND, Y: callExpr(selectExpr(original, "Has"), stringExpr(key.Field))}}
		}
		statements = append(statements, &ast.IfStmt{Cond: restore, Body: &ast.BlockStmt{List: part}})
	}
	return statements, nil
}
