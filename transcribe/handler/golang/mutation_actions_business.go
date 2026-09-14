package golang

import (
	"go/ast"
	"go/token"
	"strconv"
)

func (e *actionEmitter) verifyBusiness() ([]ast.Stmt, error) {
	body := []ast.Stmt{}
	for _, role := range e.roles {
		excluded := map[string]bool{}
		for _, key := range role.record.plan.IdentityKeys() {
			excluded[key.Field] = true
		}
		if _, relation := e.parent(role); relation != nil {
			for _, link := range relation.Links {
				excluded[link.Child.Field] = true
			}
		}
		for _, relation := range role.record.plan.SelfRelations {
			for _, link := range relation.Links {
				excluded[link.Child.Field] = true
			}
		}
		loop := []ast.Stmt{defineStmt("baseline", &ast.IndexExpr{X: selectExpr(ast.NewIdent("actions"), role.field+"Validated"), Index: selectExpr(ast.NewIdent("frame"), "Entity")}), &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("baseline"), Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("validated business baseline is missing"))}}}}
		var sequenceChange ast.Expr
		if sequence := role.record.plan.Sequence; sequence != nil && !excluded[sequence.Field.Field] {
			loop = append(loop, e.original(role)...)
			loop = append(loop, e.originalKey(role, "_", "supplied")...)
			loop = append(loop, assignStmt(ast.NewIdent("_"), ast.NewIdent("supplied")))
			selection, action, err := e.actionSelection(role)
			if err != nil {
				return nil, err
			}
			loop = append(loop, selection...)
			sequenceChange = &ast.BinaryExpr{X: &ast.BinaryExpr{X: action, Op: token.EQL, Y: selectExpr(ast.NewIdent(e.l.handlerAlias), "WriteInsert")}, Op: token.LAND, Y: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(ast.NewIdent("original"), "Has"), stringExpr(sequence.Field.Field))}}
		}
		for index, field := range role.record.plan.Entity.Fields {
			if !field.Writable || field.Relation || excluded[field.Name] {
				continue
			}
			start := len(loop)
			path, err := e.fieldPath(role, field.Name)
			if err != nil {
				return nil, err
			}
			paths := []string{path}
			if marker := role.record.plan.Entity.MarkerField; marker != "" {
				paths = append(paths, marker+"."+field.Name)
			}
			for suffix, path := range paths {
				name := "business" + strconv.Itoa(index) + "Field" + strconv.Itoa(suffix)
				loop = append(loop, e.accessor(role.record, path, name)...)
				for _, pair := range []struct {
					name  string
					value ast.Expr
				}{{name + "Before", ast.NewIdent("baseline")}, {name + "After", selectExpr(ast.NewIdent("frame"), "Entity")}} {
					loop = append(loop, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent(pair.name), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(ast.NewIdent(name), "Get"), pair.value)}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}})
				}
				equal := callExpr(selectExpr(ast.NewIdent(e.l.importsByPath["reflect"]), "DeepEqual"), callExpr(selectExpr(ast.NewIdent(name+"Before"), "Interface")), callExpr(selectExpr(ast.NewIdent(name+"After"), "Interface")))
				loop = append(loop, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: equal}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("business field " + field.Name + " changed after validation"))}}})
			}
			if sequenceChange != nil && role.record.plan.Sequence.Field.Field == field.Name {
				checked := append([]ast.Stmt(nil), loop[start:]...)
				loop = append(loop[:start], &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: &ast.ParenExpr{X: sequenceChange}}, Body: &ast.BlockStmt{List: checked}})
			}
		}
		body = append(body, e.frameLoop(role, loop))
	}
	return body, nil
}
