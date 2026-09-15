package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

func (e *actionEmitter) reconcileLinks(role actionRole) ([]ast.Stmt, error) {
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: selectExpr(ast.NewIdent("entry"), "Action"), Op: token.EQL, Y: selectExpr(ast.NewIdent(e.l.handlerAlias), "WriteDelete")}, Body: &ast.BlockStmt{List: []ast.Stmt{&ast.BranchStmt{Tok: token.CONTINUE}}}}}
	if parent, relation := e.parent(role); parent != nil {
		links, err := e.linkAssignments(role, *parent, relation.Links)
		if err != nil {
			return nil, err
		}
		reference := selectExpr(selectExpr(selectExpr(ast.NewIdent("entry"), "Frame"), "State"), "Parent")
		block := e.parentPayload(*parent, reference)
		block = append(block, links...)
		body = append(body, &ast.BlockStmt{List: block})
	}
	if len(role.record.plan.SelfRelations) > 0 {
		cases := []ast.Stmt{}
		for _, relation := range role.record.plan.SelfRelations {
			links, err := e.linkAssignments(role, role, relation.Links)
			if err != nil {
				return nil, err
			}
			cases = append(cases, &ast.CaseClause{List: []ast.Expr{stringExpr(strings.Join(relation.FieldPath, "."))}, Body: links})
		}
		cases = append(cases, &ast.CaseClause{Body: []ast.Stmt{returnStmt(e.errorExpr("self link holder has no canonical mutation policy"))}})
		reference := selectExpr(selectExpr(selectExpr(ast.NewIdent("entry"), "Frame"), "State"), "SelfParent")
		block := e.parentPayload(role, reference)
		block = append(block, &ast.SwitchStmt{Tag: selectExpr(selectExpr(ast.NewIdent("entry"), "Frame"), "SelfHolder"), Body: &ast.BlockStmt{List: cases}})
		body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: reference, Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: block}})
	}
	for _, key := range role.record.plan.IdentityKeys() {
		marks, err := e.markPayload(role, key.Field)
		if err != nil {
			return nil, err
		}
		body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: selectExpr(ast.NewIdent("entry"), "Action"), Op: token.EQL, Y: selectExpr(ast.NewIdent(e.l.handlerAlias), "WriteInsert")}, Body: &ast.BlockStmt{List: marks}})
	}
	if sequence := role.record.plan.Sequence; sequence != nil {
		identity := false
		for _, key := range role.record.plan.IdentityKeys() {
			identity = identity || key.Field == sequence.Field.Field
		}
		if !identity {
			marks, err := e.markPayload(role, sequence.Field.Field)
			if err != nil {
				return nil, err
			}
			body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: selectExpr(ast.NewIdent("entry"), "Action"), Op: token.EQL, Y: selectExpr(ast.NewIdent(e.l.handlerAlias), "WriteInsert")}, Body: &ast.BlockStmt{List: marks}})
		}
	}
	return body, nil
}

func (e *actionEmitter) parentPayload(role actionRole, reference ast.Expr) []ast.Stmt {
	return []ast.Stmt{defineStmt("parent", &ast.IndexExpr{X: ast.NewIdent(role.field + "Payloads"), Index: reference}), &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("parent"), Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation parent payload is missing"))}}}}
}

func (e *actionEmitter) linkAssignments(role, parent actionRole, links []plan.KeyLink) ([]ast.Stmt, error) {
	body := []ast.Stmt{}
	for index, link := range links {
		from, err := e.fieldPath(parent, link.Parent.Field)
		if err != nil {
			return nil, err
		}
		to, err := e.fieldPath(role, link.Child.Field)
		if err != nil {
			return nil, err
		}
		block := e.accessor(parent.record, from, "source")
		block = append(block, e.accessor(role.record, to, "target")...)
		block = append(block, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("value"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(ast.NewIdent("source"), "Get"), ast.NewIdent("parent"))}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}})
		typ, err := e.l.typeReference(link.Parent.Type)
		if err != nil {
			return nil, err
		}
		block = append(block, defineStmt("typed", &ast.TypeAssertExpr{X: callExpr(selectExpr(ast.NewIdent("value"), "Interface")), Type: typ}))
		var value ast.Expr = ast.NewIdent("typed")
		switch link.Conversion {
		case plan.LinkDirect:
		case plan.LinkAddress:
			value = &ast.UnaryExpr{Op: token.AND, X: value}
		case plan.LinkDereference:
			block = append(block, &ast.IfStmt{Cond: &ast.BinaryExpr{X: value, Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("parent link value is missing"))}}})
			value = &ast.StarExpr{X: value}
		default:
			return nil, fmt.Errorf("mutation link %d for %s has no checked conversion", index, role.record.plan.Identity)
		}
		if e.entities.identity != nil && !e.isIdentityField(role, link.Child.Field) {
			original := selectExpr(selectExpr(selectExpr(ast.NewIdent("entry"), "Frame"), "State"), "Original")
			captured := selectExpr(&ast.TypeAssertExpr{X: original, Type: &ast.StarExpr{X: ast.NewIdent(role.association.StateType)}}, "original")
			insert := &ast.BinaryExpr{X: selectExpr(ast.NewIdent("entry"), "Action"), Op: token.EQL, Y: selectExpr(ast.NewIdent(e.l.handlerAlias), "WriteInsert")}
			check := []ast.Stmt{
				&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("supplied"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(ast.NewIdent("target"), "Get"), captured)}}, errorGuard(ast.NewIdent("err")),
				(actionValidationEmitter{action: e}).equal(callExpr(selectExpr(ast.NewIdent("supplied"), "Interface")), value, "supplied relation field conflicts with the captured parent"),
			}
			block = append(block, &ast.IfStmt{Cond: &ast.BinaryExpr{X: insert, Op: token.LAND, Y: callExpr(selectExpr(original, "Has"), stringExpr(link.Child.Field))}, Body: &ast.BlockStmt{List: check}})
		}
		block = append(block, errorGuard(e.runtimeCall("AssignFields", selectExpr(ast.NewIdent("entry"), "Payload"), e.assignment(ast.NewIdent("target"), value))))
		marks, err := e.markPayload(role, link.Child.Field)
		if err != nil {
			return nil, err
		}
		block = append(block, marks...)
		body = append(body, &ast.BlockStmt{List: block})
	}
	return body, nil
}

func (e *actionEmitter) markPayload(role actionRole, field string) ([]ast.Stmt, error) {
	if _, err := e.fieldPath(role, field); err != nil {
		return nil, err
	}
	marker := role.record.plan.Entity.MarkerField
	if marker == "" {
		return nil, fmt.Errorf("mutation role %s requires a marker for identity/link field %s", role.record.plan.Identity, field)
	}
	body := e.accessor(role.record, marker+"."+field, "marker")
	reflectAlias := e.l.importsByPath["reflect"]
	body = append(body, defineStmt("flag", callExpr(selectExpr(callExpr(selectExpr(ast.NewIdent(reflectAlias), "New"), callExpr(selectExpr(ast.NewIdent("marker"), "Type"))), "Elem"))), &ast.IfStmt{Cond: &ast.BinaryExpr{X: callExpr(selectExpr(ast.NewIdent("flag"), "Kind")), Op: token.NEQ, Y: selectExpr(ast.NewIdent(reflectAlias), "Bool")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("identity/link marker must be boolean"))}}}, &ast.ExprStmt{X: callExpr(selectExpr(ast.NewIdent("flag"), "SetBool"), ast.NewIdent("true"))}, errorGuard(e.runtimeCall("AssignFields", selectExpr(ast.NewIdent("entry"), "Payload"), e.assignment(ast.NewIdent("marker"), callExpr(selectExpr(ast.NewIdent("flag"), "Interface"))))))
	return []ast.Stmt{&ast.BlockStmt{List: body}}, nil
}

func (e *actionEmitter) publish(role actionRole) ([]ast.Stmt, error) {
	names := []string{}
	seen := map[string]bool{}
	add := func(name string) {
		if !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	for _, key := range role.record.plan.IdentityKeys() {
		add(key.Field)
	}
	if sequence := role.record.plan.Sequence; sequence != nil {
		add(sequence.Field.Field)
	}
	if _, relation := e.parent(role); relation != nil {
		for _, link := range relation.Links {
			add(link.Child.Field)
		}
	}
	for _, relation := range role.record.plan.SelfRelations {
		for _, link := range relation.Links {
			add(link.Child.Field)
		}
	}
	if len(names) == 0 {
		return nil, nil
	}
	body := []ast.Stmt{defineStmt("fields", &ast.CompositeLit{Type: &ast.ArrayType{Elt: selectExpr(ast.NewIdent(e.shape), "FieldValue")}})}
	for index, name := range names {
		path, err := e.fieldPath(role, name)
		if err != nil {
			return nil, err
		}
		for suffix, path := range []string{path, role.record.plan.Entity.MarkerField + "." + name} {
			accessor := "publish" + strconv.Itoa(index) + "Field" + strconv.Itoa(suffix)
			body = append(body, e.accessor(role.record, path, accessor)...)
			value := accessor + "Value"
			body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent(value), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(ast.NewIdent(accessor), "Get"), selectExpr(ast.NewIdent("entry"), "Payload"))}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}}, assignStmt(ast.NewIdent("fields"), callExpr(ast.NewIdent("append"), ast.NewIdent("fields"), e.assignment(ast.NewIdent(accessor), callExpr(selectExpr(ast.NewIdent(value), "Interface"))))))
		}
	}
	call := e.runtimeCall("AssignFields", selectExpr(selectExpr(ast.NewIdent("entry"), "Frame"), "Entity"), ast.NewIdent("fields"))
	call.Ellipsis = 1
	body = append(body, errorGuard(call))
	return body, nil
}
