package golang

import (
	"go/ast"
	"go/token"
	"strconv"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// Producer eligibility uses the same original tuple and captured database index
// as frame preparation. No working values, markers or pending payloads classify
// a parent. The captured acyclic edges let Identity resolve produced key parts
// recursively, without a second graph or row index.
func (p *mutationIdentityPolicy) previousName(record *recordLowering) string {
	return "producerPrevious" + strconv.Itoa(record.order)
}

func (p *mutationIdentityPolicy) previousType(e *entityEmitter, record *recordLowering) *ast.FuncType {
	return &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("key", ast.NewIdent(e.matchKeyName(record)))}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("bool")}}}}
}

func (p *mutationIdentityPolicy) insert(e *entityEmitter, record *recordLowering) ast.Decl {
	id := ast.NewIdent
	adapter := &ast.ParenExpr{X: &ast.CompositeLit{Type: id(e.matchAdapterName(record))}}
	body := []ast.Stmt{
		&ast.AssignStmt{Lhs: []ast.Expr{id("key"), id("complete"), id("_"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(adapter, "Identity"), id("state"))}},
		&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("err"), Op: token.NEQ, Y: id("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("false"), id("err"))}}},
		assignStmt(id("_"), id("key")), assignStmt(id("_"), id("complete")),
	}
	action := record.plan.Write.Missing
	if e.l.plan.Operation == plan.OperationPut {
		action = record.plan.Write.Existing
	}
	if e.l.plan.Operation == plan.OperationPatch && record.plan.Current != nil {
		lookup := selectExpr(selectExpr(id("state"), "owner"), p.previousName(record))
		body = append(body,
			&ast.IfStmt{Cond: &ast.BinaryExpr{X: lookup, Op: token.EQL, Y: id("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("false"), e.invariantError("captured producer database policy is unavailable"))}}},
			&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("complete"), Op: token.LAND, Y: callExpr(lookup, id("key"))}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id(strconv.FormatBool(record.plan.Write.Existing == plan.ActionInsert)), id("nil"))}}},
		)
	}
	allowed := false
	for _, candidate := range record.plan.Write.Allowed {
		allowed = allowed || candidate == plan.ActionInsert
	}
	body = append(body, returnStmt(id(strconv.FormatBool(allowed && action == plan.ActionInsert)), id("nil")))
	return &ast.FuncDecl{Name: id("ProducerInsert"), Recv: &ast.FieldList{List: []*ast.Field{namedField("state", e.statePointer(record))}}, Type: &ast.FuncType{Params: &ast.FieldList{}, Results: &ast.FieldList{List: []*ast.Field{{Type: id("bool")}, {Type: id("error")}}}}, Body: &ast.BlockStmt{List: body}}
}

func (e *frameEmitter) bindProducers() (ast.Decl, error) {
	id := ast.NewIdent
	p := e.entities.identity
	entity := &entityEmitter{l: e.l, prefix: "_" + lowerInitial(e.l.factory)}
	body := []ast.Stmt{}
	for _, record := range e.l.records {
		if record.plan.Auxiliary || record.plan.Current == nil {
			continue
		}
		role, err := e.previousRole(record)
		if err != nil {
			return nil, err
		}
		database := selectExpr(id("database"), role.TypeName)
		body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: database, Op: token.EQL, Y: id("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("producer database role was not captured"))}}})
		found := &ast.BinaryExpr{X: &ast.IndexExpr{X: selectExpr(database, "byKey"), Index: id("key")}, Op: token.NEQ, Y: id("nil")}
		body = append(body, assignStmt(selectExpr(id("snapshot"), p.previousName(record)), &ast.FuncLit{Type: p.previousType(entity, record), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(found)}}}))
	}
	body = append(body, returnStmt(id("nil")))
	fn := e.function("bindProducers", []*ast.Field{namedField("snapshot", &ast.StarExpr{X: id(e.entities.SnapshotType)})}, []*ast.Field{{Type: id("error")}}, body)
	fn.Recv = &ast.FieldList{List: []*ast.Field{namedField("database", &ast.StarExpr{X: id(e.name)})}}
	return fn, nil
}
