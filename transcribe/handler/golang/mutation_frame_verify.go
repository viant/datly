package golang

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

// verify checks working graph addresses against prepared frames. It neither
// reads database keys nor reclassifies original presence. Established identity
// parts, membership, traversal order, and parent/holder context must agree.
func (e *frameEmitter) verify() (ast.Decl, error) {
	id := ast.NewIdent
	nilExpr := id("nil")
	guard := func(condition ast.Expr, message string) ast.Stmt {
		return &ast.IfStmt{Cond: condition, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr(message))}}}
	}
	body := []ast.Stmt{guard(&ast.BinaryExpr{X: &ast.BinaryExpr{X: id("frames"), Op: token.EQL, Y: nilExpr}, Op: token.LOR, Y: &ast.BinaryExpr{X: id("input"), Op: token.EQL, Y: nilExpr}}, "frame verification requires input and frames")}
	body = append(body, defineStmt("visitIndex", &ast.BasicLit{Kind: token.INT, Value: "0"}))
	for _, record := range e.l.records {
		if record.plan.Auxiliary {
			continue
		}
		role, err := e.layout.role(record.plan)
		if err != nil {
			return nil, err
		}
		index := strconv.Itoa(record.order)
		typ := e.verifyFunctionType(record, role)
		body = append(body, defineStmt("next"+index, &ast.BasicLit{Kind: token.INT, Value: "0"}), defineStmt("seen"+index, callExpr(id("make"), &ast.MapType{Key: record.value.pointerExpr(), Value: id("bool")})), &ast.DeclStmt{Decl: &ast.GenDecl{Tok: token.VAR, Specs: []ast.Spec{&ast.ValueSpec{Names: []*ast.Ident{id("check" + index)}, Type: typ}}}})
	}
	for _, record := range e.l.records {
		if record.plan.Auxiliary {
			continue
		}
		role, err := e.layout.role(record.plan)
		if err != nil {
			return nil, err
		}
		statements, err := e.verifyRecord(record, role)
		if err != nil {
			return nil, err
		}
		body = append(body, assignStmt(id("check"+strconv.Itoa(record.order)), &ast.FuncLit{Type: e.verifyFunctionType(record, role), Body: &ast.BlockStmt{List: statements}}))
	}
	root := e.l.recordByPlan[e.l.plan.Root]
	path, err := selectPathExpr(id("input"), e.l.plan.Root.InputPath[1:])
	if err != nil {
		return nil, err
	}
	body = append(body, (&entityEmitter{l: e.l}).syncPointerValues(path, root, "roots")...)
	body = append(body, &ast.RangeStmt{Key: id("_"), Value: id("root"), Tok: token.DEFINE, X: id("roots"), Body: &ast.BlockStmt{List: []ast.Stmt{errorGuard(callExpr(id("check"+strconv.Itoa(root.order)), id("root"), nilExpr, nilExpr, stringExpr("")))}}})
	for _, record := range e.l.records {
		if record.plan.Auxiliary {
			continue
		}
		role, _ := e.layout.role(record.plan)
		body = append(body, guard(&ast.BinaryExpr{X: id("next" + strconv.Itoa(record.order)), Op: token.NEQ, Y: callExpr(id("len"), selectExpr(id("frames"), role.Field))}, "mutation graph removed prepared entities"))
	}
	body = append(body, guard(&ast.BinaryExpr{X: id("visitIndex"), Op: token.NEQ, Y: callExpr(id("len"), selectExpr(id("frames"), e.layout.OrderField))}, "mutation frame execution order changed"), returnStmt(nilExpr))
	fn := e.function("Verify", []*ast.Field{namedField("input", &ast.StarExpr{X: parseExpr(e.l.config.InputType)})}, []*ast.Field{{Type: id("error")}}, body)
	fn.Recv = &ast.FieldList{List: []*ast.Field{namedField("frames", &ast.StarExpr{X: id(e.layout.TypeName)})}}
	return fn, nil
}

func (e *frameEmitter) verifyFunctionType(record *recordLowering, role MutationFrameRole) *ast.FuncType {
	return &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("current", record.value.pointerExpr()), namedField("parent", &ast.StarExpr{X: parseExpr(role.ParentType)}), namedField("selfParent", record.value.pointerExpr()), namedField("selfHolder", ast.NewIdent("string"))}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}}}
}

func (e *frameEmitter) verifyRecord(record *recordLowering, role MutationFrameRole) ([]ast.Stmt, error) {
	id := ast.NewIdent
	nilExpr := id("nil")
	index := strconv.Itoa(record.order)
	next := id("next" + index)
	list := selectExpr(id("frames"), role.Field)
	seen := &ast.IndexExpr{X: id("seen" + index), Index: id("current")}
	guard := func(condition ast.Expr, message string) ast.Stmt {
		return &ast.IfStmt{Cond: condition, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr(message))}}}
	}
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("current"), Op: token.EQL, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr)}}}, guard(seen, "mutation graph has a duplicate entity reference"), assignStmt(seen, id("true")), guard(&ast.BinaryExpr{X: next, Op: token.GEQ, Y: callExpr(id("len"), list)}, "mutation graph added entities after frame preparation"), defineStmt("expected", &ast.IndexExpr{X: list, Index: next}), &ast.IncDecStmt{X: next, Tok: token.INC}}
	var changed ast.Expr = &ast.BinaryExpr{X: id("expected"), Op: token.EQL, Y: nilExpr}
	for _, pair := range [][2]ast.Expr{{selectExpr(id("expected"), "Entity"), id("current")}, {selectExpr(selectExpr(id("expected"), "State"), "Parent"), id("parent")}, {selectExpr(selectExpr(id("expected"), "State"), "SelfParent"), id("selfParent")}, {selectExpr(id("expected"), "SelfHolder"), id("selfHolder")}} {
		changed = &ast.BinaryExpr{X: changed, Op: token.LOR, Y: &ast.BinaryExpr{X: pair[0], Op: token.NEQ, Y: pair[1]}}
	}
	body = append(body, guard(changed, "mutation graph order or parent context changed after frame preparation"))
	if e.entities.identity != nil {
		identity, err := e.frozenIdentity(record)
		if err != nil {
			return nil, err
		}
		body = append(body, identity...)
	}
	order := selectExpr(id("frames"), e.layout.OrderField)
	body = append(body, guard(&ast.BinaryExpr{X: id("visitIndex"), Op: token.GEQ, Y: callExpr(id("len"), order)}, "mutation frame execution order is incomplete"), defineStmt("visit", &ast.IndexExpr{X: order, Index: id("visitIndex")}), &ast.IncDecStmt{X: id("visitIndex"), Tok: token.INC})
	orderMismatch := &ast.BinaryExpr{X: &ast.BinaryExpr{X: selectExpr(id("visit"), "Role"), Op: token.NEQ, Y: &ast.BasicLit{Kind: token.INT, Value: index}}, Op: token.LOR, Y: &ast.BinaryExpr{X: selectExpr(id("visit"), "Index"), Op: token.NEQ, Y: &ast.BinaryExpr{X: next, Op: token.SUB, Y: &ast.BasicLit{Kind: token.INT, Value: "1"}}}}
	body = append(body, guard(orderMismatch, "mutation frame execution order changed"))
	entity := &entityEmitter{l: e.l}
	relations, err := entity.entityRelations(record)
	if err != nil {
		return nil, err
	}
	for _, relation := range relations {
		block := []ast.Stmt{e.accessorAssignment(parseExpr(record.value.base), strings.Join(relation.holder, "."), "access"), e.visitError()}
		block = append(block, &ast.AssignStmt{Lhs: []ast.Expr{id("value"), id("present"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(id("access"), "GetOptional"), id("current"))}}, e.visitError())
		members := entity.syncPointerValues(entity.reflectedRelationValue(id("value"), relation.child), relation.child, "members")
		parent, selfParent, selfHolder := ast.Expr(id("current")), ast.Expr(nilExpr), ast.Expr(stringExpr(""))
		if relation.child.plan == record.plan {
			parent = id("parent")
			selfParent = id("current")
			selfHolder = stringExpr(strings.Join(relation.holder, "."))
		}
		members = append(members, &ast.RangeStmt{Key: id("_"), Value: id("child"), Tok: token.DEFINE, X: id("members"), Body: &ast.BlockStmt{List: []ast.Stmt{errorGuard(callExpr(id("check"+strconv.Itoa(relation.child.order)), id("child"), parent, selfParent, selfHolder))}}})
		block = append(block, &ast.IfStmt{Cond: id("present"), Body: &ast.BlockStmt{List: members}})
		body = append(body, &ast.BlockStmt{List: block})
	}
	body = append(body, returnStmt(nilExpr))
	return body, nil
}
