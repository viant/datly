package golang

import (
	"go/ast"
	"go/token"
	"strings"
)

// frozenIdentity verifies only the parts established before entity Init. Pending
// allocator/relation parts remain under their existing producer/receipt owners.
func (e *frameEmitter) frozenIdentity(record *recordLowering) ([]ast.Stmt, error) {
	id := ast.NewIdent
	var body []ast.Stmt
	entity := &entityEmitter{l: e.l}
	for _, part := range record.plan.IdentityKeys() {
		path := strings.Join(entity.entityFieldPath(record, part.Field), ".")
		block := []ast.Stmt{e.accessorAssignment(parseExpr(record.value.base), path, "identityAccess"), e.visitError(), &ast.AssignStmt{Lhs: []ast.Expr{id("identityValue"), id("present"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(id("identityAccess"), "GetOptional"), id("current"))}}, e.visitError()}
		invalid := ast.Expr(&ast.UnaryExpr{Op: token.NOT, X: id("present")})
		value := ast.Expr(id("identityValue"))
		if part.Type.Pointer || strings.HasPrefix(part.Type.Name, "*") {
			invalid = &ast.BinaryExpr{X: invalid, Op: token.LOR, Y: callExpr(selectExpr(value, "IsNil"))}
			value = callExpr(selectExpr(value, "Elem"))
		}
		block = append(block, &ast.IfStmt{Cond: invalid, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("frozen resolved identity field " + part.Field + " became absent"))}}})
		equal := callExpr(selectExpr(id(e.reflect), "DeepEqual"), callExpr(selectExpr(value, "Interface")), selectExpr(selectExpr(id("expected"), "identityKey"), part.Field))
		block = append(block, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: equal}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("frozen resolved identity field " + part.Field + " changed after resolution"))}}})
		body = append(body, &ast.IfStmt{Cond: selectExpr(selectExpr(id("expected"), "identityKnown"), part.Field), Body: &ast.BlockStmt{List: block}})
	}
	return body, nil
}
