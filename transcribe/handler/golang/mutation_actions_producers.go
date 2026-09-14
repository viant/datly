package golang

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// actionRelation describes target syntax for one existing canonical edge.
type actionRelation struct {
	parent    actionRole
	reference ast.Expr
	condition ast.Expr
	links     []plan.KeyLink
}

func (e *actionEmitter) relations(role actionRole, frame ast.Expr) []actionRelation {
	var result []actionRelation
	state := selectExpr(frame, "State")
	if parent, relation := e.parent(role); parent != nil {
		result = append(result, actionRelation{parent: *parent, reference: selectExpr(state, "Parent"), condition: ast.NewIdent("true"), links: relation.Links})
	}
	for _, relation := range role.record.plan.SelfRelations {
		ref := selectExpr(state, "SelfParent")
		condition := &ast.BinaryExpr{X: &ast.BinaryExpr{X: ref, Op: token.NEQ, Y: ast.NewIdent("nil")}, Op: token.LAND, Y: &ast.BinaryExpr{X: selectExpr(frame, "SelfHolder"), Op: token.EQL, Y: stringExpr(strings.Join(relation.FieldPath, "."))}}
		result = append(result, actionRelation{parent: role, reference: ref, condition: condition, links: relation.Links})
	}
	return result
}

func (e *actionEmitter) verifyRelations() (ast.Decl, error) {
	id := ast.NewIdent
	body := (actionValidationEmitter{action: e}).guard(4)
	body = append(body, e.verifyOrder()...)
	for _, role := range e.roles {
		policy := []ast.Stmt{defineStmt("frame", selectExpr(id("entry"), "Frame"))}
		policy = append(policy, e.original(role)...)
		policy = append(policy, e.originalKey(role, "_", "supplied")...)
		policy = append(policy, assignStmt(id("_"), id("supplied")))
		selection, action, err := e.actionSelection(role)
		if err != nil {
			return nil, err
		}
		policy = append(policy, selection...)
		policy = append(policy, &ast.IfStmt{Cond: &ast.BinaryExpr{X: selectExpr(id("entry"), "Action"), Op: token.NEQ, Y: action}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation decision differs from captured action policy"))}}})
		body = append(body, e.decisionLoop(role, policy))
	}
	cases := []ast.Stmt{}
	for _, role := range e.roles {
		positions := selectExpr(id("actions"), role.field+"Positions")
		body = append(body, assignStmt(positions, callExpr(id("make"), &ast.MapType{Key: role.record.value.pointerExpr(), Value: id("int")})))
		entry := &ast.IndexExpr{X: selectExpr(id("actions"), role.field), Index: selectExpr(id("visit"), "Index")}
		cases = append(cases, &ast.CaseClause{List: []ast.Expr{&ast.BasicLit{Kind: token.INT, Value: strconv.Itoa(role.record.order)}}, Body: []ast.Stmt{assignStmt(&ast.IndexExpr{X: positions, Index: selectExpr(selectExpr(entry, "Frame"), "Entity")}, id("position"))}})
	}
	body = append(body, &ast.RangeStmt{Key: id("position"), Value: id("visit"), Tok: token.DEFINE, X: selectExpr(id("frames"), e.frames.OrderField), Body: &ast.BlockStmt{List: []ast.Stmt{&ast.SwitchStmt{Tag: selectExpr(id("visit"), "Role"), Body: &ast.BlockStmt{List: cases}}}}})
	for _, role := range e.roles {
		frame := selectExpr(id("entry"), "Frame")
		var loop []ast.Stmt
		for _, relation := range e.relations(role, frame) {
			lookup := &ast.IndexExpr{X: selectExpr(id("actions"), relation.parent.field+"ByEntity"), Index: relation.reference}
			parentPosition := &ast.IndexExpr{X: selectExpr(id("actions"), relation.parent.field+"Positions"), Index: relation.reference}
			childPosition := &ast.IndexExpr{X: selectExpr(id("actions"), role.field+"Positions"), Index: selectExpr(frame, "Entity")}
			proof := []ast.Stmt{
				defineStmt("parent", lookup),
				&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("parent"), Op: token.EQL, Y: id("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("active relation has no typed parent decision"))}}},
				&ast.IfStmt{Cond: &ast.BinaryExpr{X: parentPosition, Op: token.GEQ, Y: childPosition}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("active parent must precede its child in mutation execution order"))}}},
			}
			loop = append(loop, &ast.IfStmt{Cond: relation.condition, Body: &ast.BlockStmt{List: proof}})
		}
		if len(loop) > 0 {
			body = append(body, e.decisionLoop(role, loop))
		}
	}
	return (actionValidationEmitter{action: e}).method("VerifyRelations", body), nil
}
