package golang

import (
	"go/ast"
	"go/token"
	"strconv"
)

// freezePendingIdentity seals values supplied by entity initialization before
// any allocator runs. It never changes Previous or the selected operation.
func (e *frameEmitter) freezePendingIdentity() (ast.Decl, error) {
	id := ast.NewIdent
	var body []ast.Stmt
	for _, record := range e.l.records {
		if record.plan.Auxiliary {
			continue
		}
		role, err := e.layout.role(record.plan)
		if err != nil {
			return nil, err
		}
		association, err := e.association(record)
		if err != nil {
			return nil, err
		}
		original := &ast.TypeAssertExpr{X: selectExpr(selectExpr(id("frame"), "State"), "Original"), Type: &ast.StarExpr{X: id(association.StateType)}}
		loop := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{id("key"), id("known"), id("_"), id("_"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(id("_"+lowerInitial(e.l.factory)+"ResolvedIdentity"+strconv.Itoa(record.order)), selectExpr(id("frame"), "Entity"), original)}}, e.visitError(), assignStmt(id("_"), id("key")), assignStmt(id("_"), id("known"))}
		for _, part := range record.plan.IdentityKeys() {
			fixed := selectExpr(selectExpr(id("frame"), "identityKnown"), part.Field)
			loop = append(loop, &ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.UnaryExpr{Op: token.NOT, X: fixed}, Op: token.LAND, Y: selectExpr(id("known"), part.Field)}, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(selectExpr(selectExpr(id("frame"), "identityKey"), part.Field), selectExpr(id("key"), part.Field)), assignStmt(fixed, id("true"))}}})
		}
		body = append(body, &ast.RangeStmt{Key: id("_"), Value: id("frame"), Tok: token.DEFINE, X: selectExpr(id("frames"), role.Field), Body: &ast.BlockStmt{List: loop}})
	}
	body = append(body, returnStmt(id("nil")))
	fn := e.function("freezePendingIdentity", nil, []*ast.Field{{Type: id("error")}}, body)
	fn.Recv = &ast.FieldList{List: []*ast.Field{namedField("frames", &ast.StarExpr{X: id(e.layout.TypeName)})}}
	return fn, nil
}
