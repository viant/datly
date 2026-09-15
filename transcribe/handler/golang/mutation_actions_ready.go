package golang

import (
	"go/ast"
	"go/token"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// insertReadiness checks producer completion, not original suppliedness. An
// originally absent key cannot silently rely on native INSERT backfill into a
// detached DTO. Explicit original zero keys never enter this check.
func (e *actionEmitter) insertReadiness(role actionRole) ([]ast.Stmt, error) {
	linked := map[string]bool{}
	selfLinked := map[string][]string{}
	if _, relation := e.parent(role); relation != nil {
		for _, link := range relation.Links {
			linked[link.Child.Field] = true
		}
	}
	for _, relation := range role.record.plan.SelfRelations {
		for _, link := range relation.Links {
			selfLinked[link.Child.Field] = append(selfLinked[link.Child.Field], strings.Join(relation.FieldPath, "."))
		}
	}
	body := []ast.Stmt{}
	keys := append([]plan.KeyPart(nil), role.record.plan.IdentityKeys()...)
	identityCount := len(keys)
	if sequence := role.record.plan.Sequence; sequence != nil {
		found := false
		for _, key := range keys {
			found = found || key.Field == sequence.Field.Field
		}
		if !found {
			keys = append(keys, plan.KeyPart{Field: sequence.Field.Field, Source: sequence.Field.Source, Type: sequence.Field.Type})
		}
	}
	for index, key := range keys {
		label := "new identity field " + key.Field
		if index >= identityCount {
			label = "sequence field " + key.Field
		}
		if linked[key.Field] {
			continue
		}
		path, err := e.fieldPath(role, key.Field)
		if err != nil {
			return nil, err
		}
		block := e.accessor(role.record, path, "keyAccessor")
		block = append(block, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("keyValue"), ast.NewIdent("present"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(ast.NewIdent("keyAccessor"), "GetOptional"), selectExpr(ast.NewIdent("frame"), "Entity"))}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}}, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: ast.NewIdent("present")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr(label + " is unresolved"))}}})
		if key.Type.Pointer || strings.HasPrefix(key.Type.Name, "*") {
			block = append(block, &ast.IfStmt{Cond: callExpr(selectExpr(ast.NewIdent("keyValue"), "IsNil")), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr(label + " is unresolved"))}}}, assignStmt(ast.NewIdent("keyValue"), callExpr(selectExpr(ast.NewIdent("keyValue"), "Elem"))))
		}
		marker := role.record.plan.Entity.MarkerField
		zero := []ast.Stmt{}
		if marker == "" {
			zero = append(zero, returnStmt(e.errorExpr(label+" has no completed producer")))
		} else {
			zero = append(zero, e.accessor(role.record, marker+"."+key.Field, "marker")...)
			zero = append(zero, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("flag"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(ast.NewIdent("marker"), "Get"), selectExpr(ast.NewIdent("frame"), "Entity"))}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}}, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(ast.NewIdent("flag"), "Bool"))}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr(label + " has no completed producer; configure sequencing or assign it with a marker-aware setter"))}}})
		}
		block = append(block, &ast.IfStmt{Cond: callExpr(selectExpr(ast.NewIdent("keyValue"), "IsZero")), Body: &ast.BlockStmt{List: zero}})
		pending := ast.Expr(&ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(ast.NewIdent("original"), "Has"), stringExpr(key.Field))})
		if e.entities.identity != nil && e.isIdentityField(role, key.Field) {
			pending = &ast.UnaryExpr{Op: token.NOT, X: selectExpr(selectExpr(ast.NewIdent("frame"), "identityKnown"), key.Field)}
		}
		block = []ast.Stmt{&ast.IfStmt{Cond: pending, Body: &ast.BlockStmt{List: block}}}
		if holders := selfLinked[key.Field]; len(holders) > 0 {
			var match ast.Expr
			for _, holder := range holders {
				equal := &ast.BinaryExpr{X: selectExpr(ast.NewIdent("frame"), "SelfHolder"), Op: token.EQL, Y: stringExpr(holder)}
				if match == nil {
					match = equal
				} else {
					match = &ast.BinaryExpr{X: match, Op: token.LOR, Y: equal}
				}
			}
			producer := &ast.BinaryExpr{X: &ast.BinaryExpr{X: selectExpr(selectExpr(ast.NewIdent("frame"), "State"), "SelfParent"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Op: token.LAND, Y: &ast.ParenExpr{X: match}}
			body = append(body, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: &ast.ParenExpr{X: producer}}, Body: &ast.BlockStmt{List: block}})
		} else {
			body = append(body, &ast.BlockStmt{List: block})
		}
	}
	return body, nil
}
