package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
)

func (e *entityEmitter) syncName() string { return e.prefix + "PresenceSync" }
func (e *entityEmitter) syncNodeName(record *recordLowering) string {
	return "plan" + strconv.Itoa(record.order)
}
func (e *entityEmitter) syncCollectionName(record *recordLowering) string {
	return "collection" + strconv.Itoa(record.order)
}
func (e *entityEmitter) syncType() ast.Expr { return &ast.StarExpr{X: ast.NewIdent(e.syncName())} }
func (e *entityEmitter) genericType(name string, arguments ...ast.Expr) ast.Expr {
	return &ast.IndexListExpr{X: selectExpr(ast.NewIdent(e.shapeAlias), name), Indices: arguments}
}
func (e *entityEmitter) snapshotType(record *recordLowering) ast.Expr {
	return &ast.IndexExpr{X: selectExpr(ast.NewIdent(e.l.handlerAlias), "EntitySnapshot"), Index: parseExpr(record.value.base)}
}
func (e *entityEmitter) syncPair(record *recordLowering) ast.Expr {
	return &ast.StructType{Fields: &ast.FieldList{List: []*ast.Field{namedField("Current", record.value.pointerExpr()), namedField("Original", e.statePointer(record))}}}
}

func (e *entityEmitter) syncContextDeclaration() ast.Decl {
	fields := []*ast.Field{namedField("owner", &ast.StarExpr{X: ast.NewIdent(e.snapshot)}), namedField("patches", &ast.ArrayType{Elt: e.markerPatchPointer()}), namedField("markerLocations", &ast.MapType{Key: ast.NewIdent("any"), Value: e.markerPatchPointer()})}
	for _, record := range e.l.records {
		if e.enabled(record) {
			fields = append(fields, namedField("seen"+strconv.Itoa(record.order), &ast.MapType{Key: e.syncPair(record), Value: ast.NewIdent("bool")}))
			fields = append(fields, namedField("matched"+strconv.Itoa(record.order), e.recordsType(record)))
		}
	}
	return &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: ast.NewIdent(e.syncName()), Type: &ast.StructType{Fields: &ast.FieldList{List: fields}}}}}
}

func (e *entityEmitter) syncDeclarations(record *recordLowering) ([]ast.Decl, error) {
	result, err := e.matchDeclarations(record)
	if err != nil {
		return nil, err
	}
	node, err := e.syncNodeDeclaration(record)
	if err != nil {
		return nil, err
	}
	result = append(result, node, e.syncCollectionDeclaration(record), e.snapshotSyncDeclaration(record))
	if record.plan.Entity.Owned {
		key := record.value.base + ".SyncPresence"
		signature := record.plan.Entity.MarkerField + "|" + strconv.FormatBool(record.plan.Entity.MarkerPointer)
		for _, part := range e.keys(record) {
			signature += "|" + part.Field
		}
		if prior, found := e.setters[key]; found && prior != signature {
			return nil, fmt.Errorf("entity %s has incompatible identity/marker contracts across views", record.value.base)
		} else if !found {
			e.setters[key] = signature
			result = append(result, e.entitySyncDeclaration(record))
		}
	}
	return result, nil
}

func (e *entityEmitter) syncCollectionDeclaration(record *recordLowering) ast.Decl {
	current, original, context := ast.NewIdent("current"), ast.NewIdent("original"), ast.NewIdent("context")
	matcherType := e.genericType("CollectionMatcher", parseExpr(record.value.base), ast.NewIdent(e.matchKeyName(record)), e.statePointer(record))
	matcher := &ast.CompositeLit{Type: matcherType, Elts: []ast.Expr{&ast.KeyValueExpr{Key: ast.NewIdent("Adapter"), Value: &ast.CompositeLit{Type: ast.NewIdent(e.matchAdapterName(record)), Elts: []ast.Expr{&ast.KeyValueExpr{Key: ast.NewIdent("owner"), Value: selectExpr(context, "owner")}}}}}}
	request := &ast.CompositeLit{Type: e.genericType("CollectionMatchInput", parseExpr(record.value.base), e.statePointer(record)), Elts: []ast.Expr{&ast.KeyValueExpr{Key: ast.NewIdent("Current"), Value: current}, &ast.KeyValueExpr{Key: ast.NewIdent("Original"), Value: original}, &ast.KeyValueExpr{Key: ast.NewIdent("ByPointer"), Value: selectExpr(selectExpr(context, "owner"), e.recordsName(record))}, &ast.KeyValueExpr{Key: ast.NewIdent("PointerIdentity"), Value: ast.NewIdent("pointerIdentity")}}}
	body := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("matched"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(&ast.ParenExpr{X: matcher}, "Match"), request)}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("false"), ast.NewIdent("err"))}}}}
	call := callExpr(selectExpr(context, e.syncNodeName(record)), ast.NewIdent("entity"), &ast.IndexExpr{X: selectExpr(ast.NewIdent("matched"), "Original"), Index: ast.NewIdent("index")})
	body = append(body, &ast.RangeStmt{Key: ast.NewIdent("index"), Value: ast.NewIdent("entity"), Tok: token.DEFINE, X: current, Body: &ast.BlockStmt{List: []ast.Stmt{&ast.IfStmt{Init: defineStmt("err", call), Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("false"), ast.NewIdent("err"))}}}}}}, returnStmt(selectExpr(ast.NewIdent("matched"), "Changed"), ast.NewIdent("nil")))
	return &ast.FuncDecl{Name: ast.NewIdent(e.syncCollectionName(record)), Recv: &ast.FieldList{List: []*ast.Field{namedField("context", e.syncType())}}, Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("current", &ast.ArrayType{Elt: record.value.pointerExpr()}), namedField("original", e.stateSlice(record)), namedField("pointerIdentity", ast.NewIdent("bool"))}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("bool")}, {Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}
}

func (e *entityEmitter) syncApply(context ast.Expr) []ast.Stmt {
	return []ast.Stmt{returnStmt(callExpr(selectExpr(context, "finish")))}
}

func (e *entityEmitter) snapshotSyncDeclaration(record *recordLowering) ast.Decl {
	current, state := ast.NewIdent("current"), ast.NewIdent("state")
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: current, Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("nil"))}}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.BinaryExpr{X: state, Op: token.EQL, Y: ast.NewIdent("nil")}, Op: token.LOR, Y: &ast.BinaryExpr{X: selectExpr(state, "owner"), Op: token.EQL, Y: ast.NewIdent("nil")}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.invariantError("original processing snapshot is required"))}}}}
	body = append(body, defineStmt("context", &ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: ast.NewIdent(e.syncName()), Elts: []ast.Expr{&ast.KeyValueExpr{Key: ast.NewIdent("owner"), Value: selectExpr(state, "owner")}}}}))
	if !record.value.pointer {
		adapter := &ast.ParenExpr{X: &ast.CompositeLit{Type: ast.NewIdent(e.matchAdapterName(record)), Elts: []ast.Expr{&ast.KeyValueExpr{Key: ast.NewIdent("owner"), Value: selectExpr(state, "owner")}}}}
		checks := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("_"), ast.NewIdent("assigned"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(adapter, "Key"), state)}}}
		checks = append(checks, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}})
		equality := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("same"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(adapter, "Equal"), current, state)}}}
		equality = append(equality, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}})
		equality = append(equality, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: ast.NewIdent("same")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.invariantError("unassigned value from a multi-row capture has ambiguous original association"))}}})
		checks = append(checks, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: ast.NewIdent("assigned")}, Body: &ast.BlockStmt{List: equality}})
		body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: selectExpr(state, "valueScopeSize"), Op: token.GTR, Y: &ast.BasicLit{Kind: token.INT, Value: "1"}}, Body: &ast.BlockStmt{List: checks}})
	}
	values := &ast.CompositeLit{Type: &ast.ArrayType{Elt: record.value.pointerExpr()}, Elts: []ast.Expr{current}}
	originals := &ast.CompositeLit{Type: e.stateSlice(record), Elts: []ast.Expr{state}}
	call := callExpr(selectExpr(ast.NewIdent("context"), e.syncCollectionName(record)), values, originals, ast.NewIdent(strconv.FormatBool(record.value.pointer)))
	body = append(body, &ast.IfStmt{Init: &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("_"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{call}}, Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}})
	body = append(body, e.syncApply(ast.NewIdent("context"))...)
	return &ast.FuncDecl{Name: ast.NewIdent("SyncPresence"), Recv: &ast.FieldList{List: []*ast.Field{namedField("state", e.statePointer(record))}}, Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("current", record.value.pointerExpr())}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}
}

func (e *entityEmitter) entitySyncDeclaration(record *recordLowering) ast.Decl {
	entity, original := ast.NewIdent("entity"), ast.NewIdent("original")
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: entity, Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("nil"))}}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: original, Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.invariantError("original processing snapshot is required"))}}}, returnStmt(callExpr(selectExpr(original, "SyncPresence"), entity))}
	e.asset.Methods = append(e.asset.Methods, EntityMethod{Receiver: record.value.base, Name: "SyncPresence", Signature: fmt.Sprintf("func(%s.EntitySnapshot[%s]) error", e.l.handlerAlias, record.value.base)})
	return &ast.FuncDecl{Name: ast.NewIdent("SyncPresence"), Recv: &ast.FieldList{List: []*ast.Field{namedField("entity", record.value.pointerExpr())}}, Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("original", e.snapshotType(record))}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}
}

func (e *entityEmitter) syncInputDeclarations() ([]ast.Decl, error) {
	input, snapshot := ast.NewIdent("input"), ast.NewIdent("snapshot")
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: input, Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("nil"), ast.NewIdent("nil"))}}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: snapshot, Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("nil"), e.invariantError("original processing snapshot is required"))}}}, defineStmt("context", &ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: ast.NewIdent(e.syncName()), Elts: []ast.Expr{&ast.KeyValueExpr{Key: ast.NewIdent("owner"), Value: snapshot}}}})}
	values, err := pathExpr(e.l.plan.Root.InputPath)
	if err != nil {
		return nil, err
	}
	record := e.l.recordByPlan[e.l.plan.Root]
	body = append(body, e.syncPointerValues(values, record, "members")...)
	call := callExpr(selectExpr(ast.NewIdent("context"), e.syncCollectionName(record)), ast.NewIdent("members"), selectExpr(snapshot, "Roots"), ast.NewIdent(strconv.FormatBool(record.value.pointer)))
	body = append(body, &ast.IfStmt{Init: &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("_"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{call}}, Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("nil"), ast.NewIdent("err"))}}})
	body = append(body, &ast.IfStmt{Init: defineStmt("err", callExpr(selectExpr(ast.NewIdent("context"), "finish"))), Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("nil"), ast.NewIdent("err"))}}})
	body = append(body, returnStmt(ast.NewIdent("context"), ast.NewIdent("nil")))
	private := &ast.FuncDecl{Name: ast.NewIdent("synchronize"), Recv: &ast.FieldList{List: []*ast.Field{namedField("snapshot", &ast.StarExpr{X: ast.NewIdent(e.snapshot)})}}, Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("input", &ast.StarExpr{X: parseExpr(e.l.config.InputType)})}}, Results: &ast.FieldList{List: []*ast.Field{{Type: e.syncType()}, {Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}
	publicBody := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("_"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(snapshot, "synchronize"), input)}}, returnStmt(ast.NewIdent("err"))}
	public := &ast.FuncDecl{Name: ast.NewIdent("SyncPresence"), Recv: &ast.FieldList{List: []*ast.Field{namedField("snapshot", &ast.StarExpr{X: ast.NewIdent(e.snapshot)})}}, Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("input", &ast.StarExpr{X: parseExpr(e.l.config.InputType)})}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: publicBody}}
	return []ast.Decl{private, public}, nil
}

func (e *entityEmitter) syncPointerValues(values ast.Expr, record *recordLowering, name string) []ast.Stmt {
	typ := &ast.ArrayType{Elt: record.value.pointerExpr()}
	destination := ast.NewIdent(name)
	if !record.value.many {
		if !record.value.pointer {
			values = &ast.UnaryExpr{Op: token.AND, X: values}
		}
		return []ast.Stmt{defineStmt(name, &ast.CompositeLit{Type: typ, Elts: []ast.Expr{values}})}
	}
	if record.value.pointer {
		return []ast.Stmt{defineStmt(name, values)}
	}
	value := &ast.UnaryExpr{Op: token.AND, X: &ast.IndexExpr{X: values, Index: ast.NewIdent("index")}}
	return []ast.Stmt{&ast.DeclStmt{Decl: &ast.GenDecl{Tok: token.VAR, Specs: []ast.Spec{&ast.ValueSpec{Names: []*ast.Ident{destination}, Type: typ}}}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: values, Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(destination, callExpr(ast.NewIdent("make"), typ, callExpr(ast.NewIdent("len"), values)))}}}, &ast.RangeStmt{Key: ast.NewIdent("index"), Tok: token.DEFINE, X: values, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(&ast.IndexExpr{X: destination, Index: ast.NewIdent("index")}, value)}}}}
}
