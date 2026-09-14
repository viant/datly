package golang

import (
	"go/ast"
	"go/token"
	"strconv"
)

// Each node contributes marker paths to a shared, detached patch plan. The
// entire graph must validate and all marker copies must prepare before publish.
func (e *entityEmitter) syncNodeDeclaration(record *recordLowering) (ast.Decl, error) {
	current, original, context := ast.NewIdent("current"), ast.NewIdent("original"), ast.NewIdent("context")
	owner := selectExpr(context, "owner")
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: current, Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("nil"))}}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.BinaryExpr{X: original, Op: token.EQL, Y: ast.NewIdent("nil")}, Op: token.LOR, Y: &ast.BinaryExpr{X: selectExpr(original, "original"), Op: token.EQL, Y: ast.NewIdent("nil")}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.invariantError("original processing baseline is required"))}}}}
	if record.plan.Entity.MarkerField == "" {
		body = append(body, returnStmt(e.invariantError("entity "+record.value.base+" has no presence marker contract")))
		return e.syncNodeFunction(record, body), nil
	}
	seen := selectExpr(context, "seen"+strconv.Itoa(record.order))
	matched := selectExpr(context, "matched"+strconv.Itoa(record.order))
	body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: matched, Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(matched, callExpr(ast.NewIdent("make"), e.recordsType(record)))}}}, &ast.IfStmt{Init: defineStmt("prior", &ast.IndexExpr{X: matched, Index: current}), Cond: &ast.BinaryExpr{X: &ast.BinaryExpr{X: ast.NewIdent("prior"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Op: token.LAND, Y: &ast.BinaryExpr{X: ast.NewIdent("prior"), Op: token.NEQ, Y: original}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.invariantError("current entity has ambiguous original association within one view"))}}}, assignStmt(&ast.IndexExpr{X: matched, Index: current}, original))
	pairType := e.syncPair(record)
	body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: seen, Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(seen, callExpr(ast.NewIdent("make"), &ast.MapType{Key: pairType, Value: ast.NewIdent("bool")}))}}}, defineStmt("pair", &ast.CompositeLit{Type: pairType, Elts: []ast.Expr{&ast.KeyValueExpr{Key: ast.NewIdent("Current"), Value: current}, &ast.KeyValueExpr{Key: ast.NewIdent("Original"), Value: original}}}), &ast.IfStmt{Cond: &ast.IndexExpr{X: seen, Index: ast.NewIdent("pair")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("nil"))}}}, assignStmt(&ast.IndexExpr{X: seen, Index: ast.NewIdent("pair")}, ast.NewIdent("true")), defineStmt("marks", &ast.CompositeLit{Type: &ast.ArrayType{Elt: ast.NewIdent("string")}}))
	eligible := map[string]bool{}
	for index, field := range record.plan.Entity.Fields {
		if !field.Writable && !field.Identity {
			continue
		}
		eligible[field.Name] = true
		var wanted ast.Expr = callExpr(selectExpr(original, "Has"), stringExpr(field.Name))
		if !field.Identity && !field.Relation {
			name := "equal" + strconv.Itoa(index)
			body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent(name), ast.NewIdent(name + "Err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(ast.NewIdent(e.prefix+"EqualBusiness"), selectExpr(owner, e.accessName(record, "Value", field.Name)), current, selectExpr(original, "original"))}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent(name + "Err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent(name + "Err"))}}})
			wanted = &ast.BinaryExpr{X: wanted, Op: token.LOR, Y: &ast.UnaryExpr{Op: token.NOT, X: ast.NewIdent(name)}}
		}
		body = append(body, e.stageFlagStatements(record, field.Name, wanted, "flag"+strconv.Itoa(index))...)
	}
	relations, err := e.entityRelations(record)
	if err != nil {
		return nil, err
	}
	for index, relation := range relations {
		name := "relation" + strconv.Itoa(index)
		accessor := selectExpr(owner, e.accessName(record, "Relation", relation.storage))
		body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent(name), ast.NewIdent(name + "Present"), ast.NewIdent(name + "Err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(accessor, "GetOptional"), current)}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent(name + "Err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent(name + "Err"))}}})
		members := "members" + strconv.Itoa(index)
		sliceType := &ast.ArrayType{Elt: relation.child.value.pointerExpr()}
		if relation.child.value.many {
			body = append(body, &ast.DeclStmt{Decl: &ast.GenDecl{Tok: token.VAR, Specs: []ast.Spec{&ast.ValueSpec{Names: []*ast.Ident{ast.NewIdent(members)}, Type: sliceType}}}})
		} else {
			body = append(body, defineStmt(members, &ast.CompositeLit{Type: sliceType, Elts: []ast.Expr{ast.NewIdent("nil")}}))
		}
		value := e.reflectedRelationValue(ast.NewIdent(name), relation.child)
		normalized := e.syncPointerValues(value, relation.child, "presentMembers")
		normalized = append(normalized, assignStmt(ast.NewIdent(members), ast.NewIdent("presentMembers")))
		body = append(body, &ast.IfStmt{Cond: ast.NewIdent(name + "Present"), Body: &ast.BlockStmt{List: normalized}})
		changed := "changed" + strconv.Itoa(index)
		if !eligible[relation.marker] {
			changed = "_"
		}
		call := callExpr(selectExpr(context, e.syncCollectionName(relation.child)), ast.NewIdent(members), selectExpr(original, relation.storage), ast.NewIdent(strconv.FormatBool(relation.child.value.pointer)))
		body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent(changed), ast.NewIdent(name + "MatchErr")}, Tok: token.DEFINE, Rhs: []ast.Expr{call}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent(name + "MatchErr"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent(name + "MatchErr"))}}})
		if changed != "_" {
			body = append(body, e.stageFlagStatements(record, relation.marker, ast.NewIdent(changed), "relationFlag"+strconv.Itoa(index))...)
		}
	}
	body = append(body, returnStmt(callExpr(selectExpr(context, "stage"), current, selectExpr(owner, e.accessName(record, "Marker", "")), ast.NewIdent("marks"))))
	return e.syncNodeFunction(record, body), nil
}

func (e *entityEmitter) stageFlagStatements(record *recordLowering, name string, wanted ast.Expr, temporary string) []ast.Stmt {
	accessor := selectExpr(selectExpr(ast.NewIdent("context"), "owner"), e.accessName(record, "Mark", name))
	statements := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent(temporary), ast.NewIdent(temporary + "Err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(accessor, "Get"), ast.NewIdent("current"))}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent(temporary + "Err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent(temporary + "Err"))}}}, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(ast.NewIdent(temporary), "Bool"))}, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(ast.NewIdent("marks"), callExpr(ast.NewIdent("append"), ast.NewIdent("marks"), stringExpr(name)))}}}}
	return []ast.Stmt{&ast.IfStmt{Cond: wanted, Body: &ast.BlockStmt{List: statements}}}
}

func (e *entityEmitter) syncNodeFunction(record *recordLowering, body []ast.Stmt) ast.Decl {
	return &ast.FuncDecl{Name: ast.NewIdent(e.syncNodeName(record)), Recv: &ast.FieldList{List: []*ast.Field{namedField("context", e.syncType())}}, Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("current", record.value.pointerExpr()), namedField("original", e.statePointer(record))}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}
}
