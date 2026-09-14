package golang

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

func (e *frameEmitter) frame(record *recordLowering, role MutationFrameRole) ([]ast.Stmt, error) {
	nilExpr, current := ast.NewIdent("nil"), ast.NewIdent("current")
	association, err := e.association(record)
	if err != nil {
		return nil, err
	}
	var producerCheck ast.Stmt
	if producer := e.entities.identity; producer != nil {
		var parentOriginal ast.Expr = ast.NewIdent("parent")
		if parent, _ := producer.parent(e.l, record); parent != nil {
			parentAssociation, err := e.association(parent)
			if err != nil {
				return nil, err
			}
			parentOriginal = &ast.IndexExpr{X: selectExpr(ast.NewIdent("sync"), parentAssociation.Field), Index: ast.NewIdent("parent")}
		}
		selfOriginal := &ast.IndexExpr{X: selectExpr(ast.NewIdent("sync"), association.Field), Index: ast.NewIdent("selfParent")}
		changed := &ast.BinaryExpr{X: &ast.BinaryExpr{X: selectExpr(ast.NewIdent("original"), "producerParent"), Op: token.NEQ, Y: parentOriginal}, Op: token.LOR, Y: &ast.BinaryExpr{X: selectExpr(ast.NewIdent("original"), "producerSelf"), Op: token.NEQ, Y: selfOriginal}}
		mismatch := &ast.BinaryExpr{X: changed, Op: token.LOR, Y: &ast.BinaryExpr{X: selectExpr(ast.NewIdent("original"), "producerHolder"), Op: token.NEQ, Y: ast.NewIdent("selfHolder")}}
		producerCheck = &ast.IfStmt{Cond: mismatch, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation graph changed the captured producer context"))}}}
	}
	seen := &ast.IndexExpr{X: ast.NewIdent("seen" + strconv.Itoa(record.order)), Index: current}
	conflict := &ast.BinaryExpr{X: &ast.BinaryExpr{X: selectExpr(selectExpr(ast.NewIdent("prior"), "State"), "Parent"), Op: token.NEQ, Y: ast.NewIdent("parent")}, Op: token.LOR, Y: &ast.BinaryExpr{X: selectExpr(selectExpr(ast.NewIdent("prior"), "State"), "SelfParent"), Op: token.NEQ, Y: ast.NewIdent("selfParent")}}
	conflict = &ast.BinaryExpr{X: conflict, Op: token.LOR, Y: &ast.BinaryExpr{X: selectExpr(ast.NewIdent("prior"), "SelfHolder"), Op: token.NEQ, Y: ast.NewIdent("selfHolder")}}
	body := []ast.Stmt{
		&ast.IfStmt{Cond: &ast.BinaryExpr{X: current, Op: token.EQL, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr)}}},
		&ast.IfStmt{Init: defineStmt("prior", seen), Cond: &ast.BinaryExpr{X: ast.NewIdent("prior"), Op: token.NEQ, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{
			&ast.IfStmt{Cond: conflict, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("entity role has ambiguous parent context"))}}}, returnStmt(nilExpr),
		}}},
		defineStmt("original", &ast.IndexExpr{X: selectExpr(ast.NewIdent("sync"), association.Field), Index: current}),
		&ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("original"), Op: token.EQL, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("entity was not associated by successful presence synchronization"))}}},
	}
	if producerCheck != nil {
		body = append(body, producerCheck)
	}
	stateType := &ast.IndexListExpr{X: selectExpr(ast.NewIdent(e.l.handlerAlias), "EntityState"), Indices: []ast.Expr{parseExpr(record.value.base), parseExpr(role.ParentType)}}
	state := &ast.CompositeLit{Type: stateType, Elts: []ast.Expr{&ast.KeyValueExpr{Key: ast.NewIdent("Parent"), Value: ast.NewIdent("parent")}, &ast.KeyValueExpr{Key: ast.NewIdent("SelfParent"), Value: ast.NewIdent("selfParent")}, &ast.KeyValueExpr{Key: ast.NewIdent("Original"), Value: ast.NewIdent("original")}}}
	body = append(body, defineStmt("frame", &ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: ast.NewIdent(role.FrameType), Elts: []ast.Expr{&ast.KeyValueExpr{Key: ast.NewIdent("Entity"), Value: current}, &ast.KeyValueExpr{Key: ast.NewIdent("State"), Value: state}, &ast.KeyValueExpr{Key: ast.NewIdent("SelfHolder"), Value: ast.NewIdent("selfHolder")}}}}))
	if record.plan.Current != nil {
		previous, err := e.previousRole(record)
		if err != nil {
			return nil, err
		}
		database := selectExpr(ast.NewIdent("database"), previous.TypeName)
		body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: database, Op: token.EQL, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("database role was not captured before input initialization"))}}}, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(ast.NewIdent("original"), "Available"))}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("original identity presence is unavailable"))}}})
		adapter := &ast.ParenExpr{X: &ast.CompositeLit{Type: ast.NewIdent(association.KeyAdapterType)}}
		body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("key"), ast.NewIdent("supplied"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(adapter, "Key"), ast.NewIdent("original"))}}, e.visitError())
		entry := &ast.IndexExpr{X: selectExpr(database, "byKey"), Index: ast.NewIdent("key")}
		block := []ast.Stmt{&ast.IfStmt{Init: defineStmt("previous", entry), Cond: &ast.BinaryExpr{X: ast.NewIdent("previous"), Op: token.NEQ, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(selectExpr(selectExpr(ast.NewIdent("frame"), "State"), "Previous"), selectExpr(ast.NewIdent("previous"), "value")), assignStmt(selectExpr(selectExpr(ast.NewIdent("frame"), "State"), "PreviousFields"), selectExpr(ast.NewIdent("previous"), "fields"))}}}}
		body = append(body, &ast.IfStmt{Cond: ast.NewIdent("supplied"), Body: &ast.BlockStmt{List: block}})
	}
	order := selectExpr(ast.NewIdent("frames"), e.layout.OrderField)
	visit := &ast.CompositeLit{Type: ast.NewIdent(e.layout.OrderEntryType), Elts: []ast.Expr{&ast.KeyValueExpr{Key: ast.NewIdent("Role"), Value: &ast.BasicLit{Kind: token.INT, Value: strconv.Itoa(record.order)}}, &ast.KeyValueExpr{Key: ast.NewIdent("Index"), Value: callExpr(ast.NewIdent("len"), selectExpr(ast.NewIdent("frames"), role.Field))}}}
	body = append(body, assignStmt(seen, ast.NewIdent("frame")), assignStmt(order, callExpr(ast.NewIdent("append"), order, visit)), assignStmt(selectExpr(ast.NewIdent("frames"), role.Field), callExpr(ast.NewIdent("append"), selectExpr(ast.NewIdent("frames"), role.Field), ast.NewIdent("frame"))))
	entity := &entityEmitter{l: e.l, prefix: "_" + lowerInitial(e.l.factory)}
	relations, err := entity.entityRelations(record)
	if err != nil {
		return nil, err
	}
	for index, relation := range relations {
		name := "members" + strconv.Itoa(index)
		accessor := selectExpr(selectExpr(ast.NewIdent("sync"), "owner"), entity.accessName(record, "Relation", relation.storage))
		block := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("value"), ast.NewIdent("present"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(accessor, "GetOptional"), current)}}, e.visitError()}
		members := entity.reflectedRelationValue(ast.NewIdent("value"), relation.child)
		present := entity.syncPointerValues(members, relation.child, name)
		parent, selfParent := ast.Expr(current), ast.Expr(nilExpr)
		selfHolder := ast.Expr(stringExpr(""))
		if relation.child.plan == record.plan {
			parent = ast.NewIdent("parent")
			selfParent = current
			selfHolder = stringExpr(strings.Join(relation.holder, "."))
		}
		present = append(present, &ast.RangeStmt{Key: ast.NewIdent("_"), Value: ast.NewIdent("child"), Tok: token.DEFINE, X: ast.NewIdent(name), Body: &ast.BlockStmt{List: []ast.Stmt{&ast.IfStmt{Init: defineStmt("err", callExpr(ast.NewIdent("add"+strconv.Itoa(relation.child.order)), ast.NewIdent("child"), parent, selfParent, selfHolder)), Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}}}}})
		block = append(block, &ast.IfStmt{Cond: ast.NewIdent("present"), Body: &ast.BlockStmt{List: present}})
		body = append(body, &ast.BlockStmt{List: block})
	}
	body = append(body, returnStmt(nilExpr))
	return body, nil
}
