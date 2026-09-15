package golang

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// resolvedIdentity projects initialized values, independently of original client
// facts. A candidate enables scoped lookup; it is never operation authority.
func (e *entityEmitter) resolvedIdentity(record *recordLowering) (ast.Decl, error) {
	id := ast.NewIdent
	keyType, knownType := id(e.matchKeyName(record)), id(e.matchKeyName(record)+"Known")
	fail := func(message string) ast.Stmt {
		return returnStmt(&ast.CompositeLit{Type: keyType}, &ast.CompositeLit{Type: knownType}, id("false"), id("false"), e.invariantError(message))
	}
	onError := func() ast.Stmt {
		return &ast.IfStmt{Cond: &ast.BinaryExpr{X: id("err"), Op: token.NEQ, Y: id("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("key"), id("known"), id("false"), id("false"), id("err"))}}}
	}
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("original"), Op: token.EQL, Y: id("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{fail("resolved identity requires original capture")}}}, defineStmt("key", &ast.CompositeLit{Type: keyType}), defineStmt("known", &ast.CompositeLit{Type: knownType}), defineStmt("count", &ast.BasicLit{Kind: token.INT, Value: "0"})}
	keys := e.keys(record)
	for _, part := range keys {
		original := callExpr(selectExpr(id("original"), "Has"), stringExpr(part.Field))
		block := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: original, Op: token.LAND, Y: &ast.UnaryExpr{Op: token.NOT, X: selectExpr(id("original"), "key"+part.Field+"Valid")}}, Body: &ast.BlockStmt{List: []ast.Stmt{fail("original identity field " + part.Field + " is supplied without a value")}}}}
		owner := selectExpr(id("original"), "owner")
		block = append(block, &ast.AssignStmt{Lhs: []ast.Expr{id("flag"), id("flagPresent"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(selectExpr(owner, e.accessName(record, "Mark", part.Field)), "GetOptional"), id("current"))}}, onError(), &ast.AssignStmt{Lhs: []ast.Expr{id("value"), id("present"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(selectExpr(owner, e.accessName(record, "Key", part.Field)), "GetOptional"), id("current"))}}, onError())
		marked := &ast.BinaryExpr{X: original, Op: token.LOR, Y: &ast.ParenExpr{X: &ast.BinaryExpr{X: id("flagPresent"), Op: token.LAND, Y: callExpr(selectExpr(id("flag"), "Bool"))}}}
		valid := ast.Expr(id("present"))
		value := ast.Expr(id("value"))
		pointer := part.Type.Pointer || strings.HasPrefix(part.Type.Name, "*")
		if pointer {
			valid = &ast.BinaryExpr{X: valid, Op: token.LAND, Y: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(value, "IsNil"))}}
			value = callExpr(selectExpr(value, "Elem"))
		}
		block = append(block, &ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.ParenExpr{X: marked}, Op: token.LAND, Y: &ast.UnaryExpr{Op: token.NOT, X: &ast.ParenExpr{X: valid}}}, Body: &ast.BlockStmt{List: []ast.Stmt{fail("resolved identity field " + part.Field + " is supplied without a value")}}})
		candidate := valid
		if !pointer {
			candidate = &ast.BinaryExpr{X: valid, Op: token.LAND, Y: &ast.ParenExpr{X: &ast.BinaryExpr{X: marked, Op: token.LOR, Y: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(value, "IsZero"))}}}}
		}
		typ, err := e.l.keyType(part.Type)
		if err != nil {
			return nil, err
		}
		block = append(block, &ast.IfStmt{Cond: candidate, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(selectExpr(id("key"), part.Field), &ast.TypeAssertExpr{X: callExpr(selectExpr(value, "Interface")), Type: parseExpr(typ)}), assignStmt(selectExpr(id("known"), part.Field), id("true")), &ast.IncDecStmt{X: id("count"), Tok: token.INC}}}})
		body = append(body, &ast.BlockStmt{List: block})
	}
	if len(keys) > 0 {
		body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: id("count"), Op: token.EQL, Y: &ast.BasicLit{Kind: token.INT, Value: strconv.Itoa(len(keys))}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("key"), id("known"), id("true"), id("false"), id("nil"))}}})
	}
	allowed := false
	for _, action := range record.plan.Write.Allowed {
		allowed = allowed || action == plan.ActionInsert
	}
	var pending []ast.Stmt
	if !allowed || record.plan.Write.Missing != plan.ActionInsert || e.l.plan.Operation == plan.OperationPut {
		pending = []ast.Stmt{fail("partial resolved identity requires INSERT policy")}
	} else {
		for _, part := range keys {
			pending = append(pending, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: selectExpr(id("known"), part.Field)}, Body: &ast.BlockStmt{List: []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{id("produced"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(id("original"), "Produced"), stringExpr(part.Field))}}, onError(), &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: id("produced")}, Body: &ast.BlockStmt{List: []ast.Stmt{fail("partial resolved identity requires an active captured producer for missing field " + part.Field)}}}}}})
		}
		pending = append(pending, returnStmt(id("key"), id("known"), id("false"), id("true"), id("nil")))
	}
	body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: id("count"), Op: token.GTR, Y: &ast.BasicLit{Kind: token.INT, Value: "0"}}, Body: &ast.BlockStmt{List: pending}}, returnStmt(id("key"), id("known"), id("false"), id("false"), id("nil")))
	return &ast.FuncDecl{Name: id(e.prefix + "ResolvedIdentity" + strconv.Itoa(record.order)), Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("current", record.value.pointerExpr()), namedField("original", e.statePointer(record))}}, Results: &ast.FieldList{List: []*ast.Field{{Type: keyType}, {Type: knownType}, {Type: id("bool")}, {Type: id("bool")}, {Type: id("error")}}}}, Body: &ast.BlockStmt{List: body}}, nil
}
