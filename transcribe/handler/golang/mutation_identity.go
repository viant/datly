package golang

import (
	"go/ast"
	"go/token"
	"strconv"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// mutationIdentityPolicy owns captured-producer incomplete INSERT identity.
// It is installed only by MutationProgram, never by public EntitySupport.
type mutationIdentityPolicy struct{}

func (p *mutationIdentityPolicy) entities(value *plan.Plan, config Config) (*EntityAsset, error) {
	l := &lowerer{plan: value, config: config}
	if err := l.prepare(); err != nil {
		return nil, err
	}
	if err := p.prepare(l); err != nil {
		return nil, err
	}
	return (&entityEmitter{l: l, identity: p}).emit()
}

func (p *mutationIdentityPolicy) produces(record *recordLowering, field string) bool {
	write, sequence := record.plan.Write, record.plan.Sequence
	if write.Missing != plan.ActionInsert || sequence == nil || sequence.Field.Field != field {
		return false
	}
	for _, allowed := range write.Allowed {
		if allowed == plan.ActionInsert {
			return true
		}
	}
	return false
}

func (p *mutationIdentityPolicy) declarations(e *entityEmitter, record *recordLowering) []ast.Decl {
	id := ast.NewIdent
	keyType := id(e.matchKeyName(record))
	zero := &ast.CompositeLit{Type: keyType}
	results := []*ast.Field{{Type: keyType}, {Type: id("bool")}, {Type: id("bool")}, {Type: id("error")}}
	fail := func(message string) ast.Stmt {
		return returnStmt(zero, id("false"), id("false"), e.invariantError(message))
	}
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("state"), Op: token.EQL, Y: id("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(zero, id("false"), id("false"), id("nil"))}}}}
	keys := e.keys(record)
	if len(keys) == 0 {
		body = append(body, returnStmt(zero, id("false"), id("false"), id("nil")))
	} else {
		body = append(body, defineStmt("key", zero), defineStmt("supplied", &ast.BasicLit{Kind: token.INT, Value: "0"}), defineStmt("insertOnly", id("true")))
		for _, key := range keys {
			supplied := []ast.Stmt{
				&ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: selectExpr(id("state"), "key"+key.Field+"Valid")}, Body: &ast.BlockStmt{List: []ast.Stmt{fail("original identity field " + key.Field + " is supplied without a value")}}},
				assignStmt(selectExpr(id("key"), key.Field), selectExpr(id("state"), "key"+key.Field)),
				&ast.IncDecStmt{X: id("supplied"), Tok: token.INC},
			}
			branch := &ast.IfStmt{Cond: callExpr(selectExpr(id("state"), "Has"), stringExpr(key.Field)), Body: &ast.BlockStmt{List: supplied}}
			allowed := false
			for _, action := range record.plan.Write.Allowed {
				allowed = allowed || action == plan.ActionInsert
			}
			if !allowed || record.plan.Write.Missing != plan.ActionInsert || e.l.plan.Operation == plan.OperationPut {
				branch.Else = &ast.BlockStmt{List: []ast.Stmt{assignStmt(id("insertOnly"), id("false"))}}
			} else {
				branch.Else = &ast.BlockStmt{List: []ast.Stmt{
					&ast.AssignStmt{Lhs: []ast.Expr{id("produced"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(id("state"), "Produced"), stringExpr(key.Field))}},
					&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("err"), Op: token.NEQ, Y: id("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(zero, id("false"), id("false"), id("err"))}}},
					assignStmt(id("insertOnly"), &ast.BinaryExpr{X: id("insertOnly"), Op: token.LAND, Y: id("produced")}),
				}}
			}
			body = append(body, branch)
		}
		body = append(body,
			&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("supplied"), Op: token.EQL, Y: &ast.BasicLit{Kind: token.INT, Value: strconv.Itoa(len(keys))}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("key"), id("true"), id("false"), id("nil"))}}},
			&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("supplied"), Op: token.EQL, Y: &ast.BasicLit{Kind: token.INT, Value: "0"}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(zero, id("false"), id("false"), id("nil"))}}},
			&ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: id("insertOnly")}, Body: &ast.BlockStmt{List: []ast.Stmt{fail("partial original identity requires INSERT policy and an active captured producer for every missing key part")}}},
			returnStmt(id("key"), id("false"), id("true"), id("nil")),
		)
	}
	identity := &ast.FuncDecl{Name: id("Identity"), Recv: e.adapterReceiver(record), Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("state", e.statePointer(record))}}, Results: &ast.FieldList{List: results}}, Body: &ast.BlockStmt{List: body}}
	// CollectionMatcher and Previous lookup still see only complete keys.
	adapter := &ast.ParenExpr{X: &ast.CompositeLit{Type: id(e.matchAdapterName(record))}}
	key := &ast.FuncDecl{Name: id("Key"), Recv: e.adapterReceiver(record), Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("state", e.statePointer(record))}}, Results: &ast.FieldList{List: []*ast.Field{{Type: keyType}, {Type: id("bool")}, {Type: id("error")}}}}, Body: &ast.BlockStmt{List: []ast.Stmt{
		&ast.AssignStmt{Lhs: []ast.Expr{id("key"), id("complete"), id("_"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(adapter, "Identity"), id("state"))}},
		returnStmt(id("key"), id("complete"), id("err")),
	}}}
	return []ast.Decl{p.produced(e, record), p.insert(e, record), identity, key}
}

func (p *mutationIdentityPolicy) verifyAction(e *actionEmitter, action ast.Expr) []ast.Stmt {
	if p == nil {
		return nil
	}
	id := ast.NewIdent
	invalid := &ast.BinaryExpr{X: &ast.BinaryExpr{X: action, Op: token.NEQ, Y: selectExpr(id(e.l.handlerAlias), "WriteInsert")}, Op: token.LOR, Y: &ast.BinaryExpr{X: selectExpr(selectExpr(id("frame"), "State"), "Previous"), Op: token.NEQ, Y: id("nil")}}
	return []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("insertOnly"), Op: token.LAND, Y: &ast.ParenExpr{X: invalid}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("incomplete original identity requires INSERT without Previous"))}}}}
}
