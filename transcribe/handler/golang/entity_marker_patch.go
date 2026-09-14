package golang

import (
	"go/ast"
	"go/token"
)

func (e *entityEmitter) markerPatchName() string { return e.prefix + "MarkerPatch" }
func (e *entityEmitter) markerPatchPointer() ast.Expr {
	return &ast.StarExpr{X: ast.NewIdent(e.markerPatchName())}
}
func (e *entityEmitter) reflectCall(name string, args ...ast.Expr) ast.Expr {
	return callExpr(selectExpr(ast.NewIdent(e.reflectAlias), name), args...)
}

func (e *entityEmitter) markerDeclarations() []ast.Decl {
	valueType := selectExpr(ast.NewIdent(e.reflectAlias), "Value")
	declaration := &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: ast.NewIdent(e.markerPatchName()), Type: &ast.StructType{Fields: &ast.FieldList{List: []*ast.Field{namedField("destination", valueType), namedField("value", valueType), namedField("fields", &ast.ArrayType{Elt: ast.NewIdent("string")}), namedField("selected", &ast.MapType{Key: ast.NewIdent("string"), Value: ast.NewIdent("bool")})}}}}}}
	return []ast.Decl{declaration, e.stageMarkerDeclaration(), e.prepareMarkerDeclaration(), e.finishMarkersDeclaration(), e.equalBusinessDeclaration()}
}

func (e *entityEmitter) stageMarkerDeclaration() ast.Decl {
	context, names := ast.NewIdent("context"), ast.NewIdent("names")
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: callExpr(ast.NewIdent("len"), names), Op: token.EQL, Y: &ast.BasicLit{Kind: token.INT, Value: "0"}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("nil"))}}}, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("destination"), ast.NewIdent("present"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(ast.NewIdent("accessor"), "GetOptional"), ast.NewIdent("target"))}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}}, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: ast.NewIdent("present")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.invariantError("presence marker location is absent; synchronization cannot create a business embedding"))}}}}
	destination := ast.NewIdent("destination")
	valid := ast.Expr(callExpr(selectExpr(destination, "CanSet")))
	valid = &ast.BinaryExpr{X: valid, Op: token.LAND, Y: callExpr(selectExpr(destination, "CanAddr"))}
	valid = &ast.BinaryExpr{X: valid, Op: token.LAND, Y: callExpr(selectExpr(callExpr(selectExpr(destination, "Addr")), "CanInterface"))}
	body = append(body, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: &ast.ParenExpr{X: valid}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.invariantError("presence marker location is not writable"))}}})
	locations := selectExpr(context, "markerLocations")
	body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: locations, Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(locations, callExpr(ast.NewIdent("make"), &ast.MapType{Key: ast.NewIdent("any"), Value: e.markerPatchPointer()}))}}}, defineStmt("location", callExpr(selectExpr(callExpr(selectExpr(destination, "Addr")), "Interface"))), defineStmt("patch", &ast.IndexExpr{X: locations, Index: ast.NewIdent("location")}))
	patch := ast.NewIdent("patch")
	patches := selectExpr(context, "patches")
	body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: patch, Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(patch, &ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: ast.NewIdent(e.markerPatchName()), Elts: []ast.Expr{&ast.KeyValueExpr{Key: ast.NewIdent("destination"), Value: destination}, &ast.KeyValueExpr{Key: ast.NewIdent("selected"), Value: &ast.CompositeLit{Type: &ast.MapType{Key: ast.NewIdent("string"), Value: ast.NewIdent("bool")}}}}}}), assignStmt(&ast.IndexExpr{X: locations, Index: ast.NewIdent("location")}, patch), assignStmt(patches, callExpr(ast.NewIdent("append"), patches, patch))}}})
	selected := &ast.IndexExpr{X: selectExpr(patch, "selected"), Index: ast.NewIdent("name")}
	fields := selectExpr(patch, "fields")
	body = append(body, &ast.RangeStmt{Key: ast.NewIdent("_"), Value: ast.NewIdent("name"), Tok: token.DEFINE, X: names, Body: &ast.BlockStmt{List: []ast.Stmt{&ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: selected}, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(selected, ast.NewIdent("true")), assignStmt(fields, callExpr(ast.NewIdent("append"), fields, ast.NewIdent("name")))}}}}}}, returnStmt(ast.NewIdent("nil")))
	return &ast.FuncDecl{Name: ast.NewIdent("stage"), Recv: &ast.FieldList{List: []*ast.Field{namedField("context", e.syncType())}}, Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("target", ast.NewIdent("any")), namedField("accessor", &ast.StarExpr{X: selectExpr(ast.NewIdent(e.shapeAlias), "Accessor")}), namedField("names", &ast.ArrayType{Elt: ast.NewIdent("string")})}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}
}

func (e *entityEmitter) prepareMarkerDeclaration() ast.Decl {
	patch := ast.NewIdent("patch")
	destination := selectExpr(patch, "destination")
	pointer := &ast.BinaryExpr{X: callExpr(selectExpr(destination, "Kind")), Op: token.EQL, Y: selectExpr(ast.NewIdent(e.reflectAlias), "Pointer")}
	body := []ast.Stmt{defineStmt("source", callExpr(selectExpr(destination, "Interface")))}
	allocated := e.reflectCall("New", callExpr(selectExpr(callExpr(selectExpr(destination, "Type")), "Elem")))
	allocated = callExpr(selectExpr(allocated, "Convert"), callExpr(selectExpr(destination, "Type")))
	body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: pointer, Op: token.LAND, Y: callExpr(selectExpr(destination, "IsNil"))}, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(ast.NewIdent("source"), callExpr(selectExpr(allocated, "Interface")))}}})
	clone := callExpr(selectExpr(&ast.ParenExpr{X: &ast.CompositeLit{Type: selectExpr(ast.NewIdent(e.shapeAlias), "Runtime")}}, "WithZeroFields"), ast.NewIdent("source"), selectExpr(patch, "fields"))
	clone.Ellipsis = token.Pos(1)
	body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("copied"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{clone}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}}, defineStmt("working", e.reflectCall("ValueOf", ast.NewIdent("copied"))))
	body = append(body, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: &ast.ParenExpr{X: pointer}}, Body: &ast.BlockStmt{List: []ast.Stmt{defineStmt("address", e.reflectCall("New", callExpr(selectExpr(destination, "Type")))), &ast.ExprStmt{X: callExpr(selectExpr(callExpr(selectExpr(ast.NewIdent("address"), "Elem")), "Set"), ast.NewIdent("working"))}, assignStmt(ast.NewIdent("working"), ast.NewIdent("address"))}}})
	accessorCall := callExpr(selectExpr(callExpr(selectExpr(ast.NewIdent(e.shapeAlias), "Linked"), callExpr(selectExpr(ast.NewIdent("working"), "Type"))), "Accessor"), ast.NewIdent("name"))
	loop := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("accessor"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{accessorCall}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("err"))}}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: callExpr(selectExpr(callExpr(selectExpr(ast.NewIdent("accessor"), "Type")), "Kind")), Op: token.NEQ, Y: selectExpr(ast.NewIdent(e.reflectAlias), "Bool")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.invariantError("presence marker field is not bool"))}}}, defineStmt("flag", callExpr(selectExpr(e.reflectCall("New", callExpr(selectExpr(ast.NewIdent("accessor"), "Type"))), "Elem"))), &ast.ExprStmt{X: callExpr(selectExpr(ast.NewIdent("flag"), "SetBool"), ast.NewIdent("true"))}, errorGuard(callExpr(selectExpr(ast.NewIdent("accessor"), "Set"), callExpr(selectExpr(ast.NewIdent("working"), "Interface")), callExpr(selectExpr(ast.NewIdent("flag"), "Interface"))))}
	body = append(body, &ast.RangeStmt{Key: ast.NewIdent("_"), Value: ast.NewIdent("name"), Tok: token.DEFINE, X: selectExpr(patch, "fields"), Body: &ast.BlockStmt{List: loop}}, assignStmt(selectExpr(patch, "value"), ast.NewIdent("working")), &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: &ast.ParenExpr{X: pointer}}, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(selectExpr(patch, "value"), callExpr(selectExpr(ast.NewIdent("working"), "Elem")))}}}, returnStmt(ast.NewIdent("nil")))
	return &ast.FuncDecl{Name: ast.NewIdent("prepare"), Recv: &ast.FieldList{List: []*ast.Field{namedField("patch", e.markerPatchPointer())}}, Type: &ast.FuncType{Params: &ast.FieldList{}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}
}

func (e *entityEmitter) finishMarkersDeclaration() ast.Decl {
	patches := selectExpr(ast.NewIdent("context"), "patches")
	body := []ast.Stmt{&ast.RangeStmt{Key: ast.NewIdent("_"), Value: ast.NewIdent("patch"), Tok: token.DEFINE, X: patches, Body: &ast.BlockStmt{List: []ast.Stmt{errorGuard(callExpr(selectExpr(ast.NewIdent("patch"), "prepare")))}}}, &ast.RangeStmt{Key: ast.NewIdent("_"), Value: ast.NewIdent("patch"), Tok: token.DEFINE, X: patches, Body: &ast.BlockStmt{List: []ast.Stmt{&ast.ExprStmt{X: callExpr(selectExpr(selectExpr(ast.NewIdent("patch"), "destination"), "Set"), selectExpr(ast.NewIdent("patch"), "value"))}}}}, returnStmt(ast.NewIdent("nil"))}
	return &ast.FuncDecl{Name: ast.NewIdent("finish"), Recv: &ast.FieldList{List: []*ast.Field{namedField("context", e.syncType())}}, Type: &ast.FuncType{Params: &ast.FieldList{}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}
}

func (e *entityEmitter) equalBusinessDeclaration() ast.Decl {
	body := []ast.Stmt{}
	for _, name := range []string{"left", "right"} {
		body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent(name + "Value"), ast.NewIdent(name + "Err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(ast.NewIdent("accessor"), "Get"), ast.NewIdent(name))}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent(name + "Err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("false"), ast.NewIdent(name+"Err"))}}})
	}
	body = append(body, returnStmt(e.reflectCall("DeepEqual", callExpr(selectExpr(ast.NewIdent("leftValue"), "Interface")), callExpr(selectExpr(ast.NewIdent("rightValue"), "Interface"))), ast.NewIdent("nil")))
	return &ast.FuncDecl{Name: ast.NewIdent(e.prefix + "EqualBusiness"), Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("accessor", &ast.StarExpr{X: selectExpr(ast.NewIdent(e.shapeAlias), "Accessor")}), namedField("left", ast.NewIdent("any")), namedField("right", ast.NewIdent("any"))}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("bool")}, {Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}
}
