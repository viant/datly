package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// invariantBackfillHelper uses the same native paths for linked and owned
// entities. All missing values and writable target branches are staged before
// publishing, so clone/access errors cannot leave a partly hydrated group.
func (e *entityEmitter) invariantBackfillHelper(record *recordLowering, group plan.InvariantGroup, name string) (ast.Decl, error) {
	if record.plan.Entity.MarkerField == "" {
		return nil, fmt.Errorf("invariant %s requires a presence marker", group.Name)
	}
	id := ast.NewIdent
	nilExpr := id("nil")
	guard := func() ast.Stmt {
		return &ast.IfStmt{Cond: &ast.BinaryExpr{X: id("err"), Op: token.NEQ, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("err"))}}}
	}
	assignCall := func(name string, call ast.Expr) ast.Stmt {
		return &ast.AssignStmt{Lhs: []ast.Expr{id(name), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{call}}
	}
	runtimeCall := func(method string, args ...ast.Expr) *ast.CallExpr {
		return callExpr(selectExpr(id("runtime"), method), args...)
	}
	meta := &ast.StructType{Fields: &ast.FieldList{List: []*ast.Field{namedField("Name", id("string")), namedField("Path", id("string")), namedField("Mark", id("string"))}}}
	values := []ast.Expr{}
	for _, field := range group.Fields {
		values = append(values, &ast.CompositeLit{Type: meta, Elts: []ast.Expr{stringExpr(field), stringExpr(strings.Join(e.entityFieldPath(record, field), ".")), stringExpr(record.plan.Entity.MarkerField + "." + field)}})
	}
	field := func(name string) ast.Expr { return selectExpr(id("field"), name) }
	var affected ast.Expr = id("false")
	marked := []ast.Stmt{assignStmt(id("affected"), id("true"))}
	if record.plan.Entity.Owned {
		// Retained authored Has<Group>Changes methods keep their activation policy.
		affected = callExpr(selectExpr(id("entity"), "Has"+group.Name+"Changes"))
		marked = nil
	}
	body := []ast.Stmt{
		&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("entity"), Op: token.EQL, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr)}}},
		defineStmt("runtime", &ast.CompositeLit{Type: selectExpr(id(e.shapeAlias), "Runtime")}),
		defineStmt("typ", callExpr(&ast.IndexExpr{X: selectExpr(id(e.reflectAlias), "TypeFor"), Index: parseExpr(record.value.base)})),
		defineStmt("shape", callExpr(selectExpr(id(e.shapeAlias), "Linked"), id("typ"))),
		defineStmt("fields", &ast.CompositeLit{Type: &ast.ArrayType{Elt: meta}, Elts: values}),
		defineStmt("missing", &ast.CompositeLit{Type: &ast.ArrayType{Elt: id("int")}}),
		defineStmt("paths", &ast.CompositeLit{Type: &ast.ArrayType{Elt: id("string")}}),
		defineStmt("affected", affected),
	}
	loop := []ast.Stmt{
		assignCall("mark", callExpr(selectExpr(id("shape"), "Accessor"), field("Mark"))), guard(),
		assignCall("flag", callExpr(selectExpr(id("mark"), "Get"), id("entity"))), guard(),
		&ast.IfStmt{Cond: &ast.BinaryExpr{X: callExpr(selectExpr(id("flag"), "Kind")), Op: token.NEQ, Y: selectExpr(id(e.reflectAlias), "Bool")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.invariantError("invariant " + group.Name + ": marker must be boolean"))}}},
		&ast.IfStmt{Cond: callExpr(selectExpr(id("flag"), "Bool")), Body: &ast.BlockStmt{List: marked}, Else: &ast.BlockStmt{List: []ast.Stmt{assignStmt(id("missing"), callExpr(id("append"), id("missing"), id("index"))), assignStmt(id("paths"), callExpr(id("append"), id("paths"), field("Path")))}}},
	}
	body = append(body, &ast.RangeStmt{Key: id("index"), Value: id("field"), Tok: token.DEFINE, X: id("fields"), Body: &ast.BlockStmt{List: loop}})
	stop := &ast.BinaryExpr{X: &ast.BinaryExpr{X: &ast.UnaryExpr{Op: token.NOT, X: id("affected")}, Op: token.LOR, Y: &ast.BinaryExpr{X: id("previous"), Op: token.EQL, Y: nilExpr}}, Op: token.LOR, Y: &ast.BinaryExpr{X: callExpr(id("len"), id("missing")), Op: token.EQL, Y: &ast.BasicLit{Kind: token.INT, Value: "0"}}}
	body = append(body, &ast.IfStmt{Cond: stop, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr)}}})
	body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.BinaryExpr{X: id("previousFields"), Op: token.NEQ, Y: nilExpr}, Op: token.LAND, Y: runtimeCall("IsNil", id("previousFields"))}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.invariantError("invariant " + group.Name + ": previous field evidence is typed nil"))}}})
	missingField := defineStmt("field", &ast.IndexExpr{X: id("fields"), Index: id("index")})
	message := e.invariantError("invariant " + group.Name + ": previous field %s was not loaded").(*ast.CallExpr)
	message.Args = append(message.Args, field("Name"))
	body = append(body, &ast.RangeStmt{Key: id("_"), Value: id("index"), Tok: token.DEFINE, X: id("missing"), Body: &ast.BlockStmt{List: []ast.Stmt{missingField, &ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.BinaryExpr{X: id("previousFields"), Op: token.NEQ, Y: nilExpr}, Op: token.LAND, Y: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(id("previousFields"), "Has"), field("Name"))}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(message)}}}}}})
	body = append(body, defineStmt("options", &ast.CompositeLit{Type: selectExpr(id(e.shapeAlias), "CloneOptions")}))
	selection := callExpr(selectExpr(id("options"), "Select"), id("typ"), id("paths"))
	selection.Ellipsis = 1
	body = append(body, &ast.IfStmt{Init: defineStmt("err", selection), Cond: &ast.BinaryExpr{X: id("err"), Op: token.NEQ, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("err"))}}}, assignCall("cloned", runtimeCall("CloneValue", id("previous"), id("options"))), guard())
	assignmentType := selectExpr(id(e.shapeAlias), "FieldValue")
	body = append(body, defineStmt("assignments", &ast.CompositeLit{Type: &ast.ArrayType{Elt: assignmentType}}))
	assignment := &ast.CompositeLit{Type: assignmentType, Elts: []ast.Expr{&ast.KeyValueExpr{Key: id("Accessor"), Value: id("access")}, &ast.KeyValueExpr{Key: id("Value"), Value: callExpr(selectExpr(id("value"), "Interface"))}}}
	apply := []ast.Stmt{missingField, assignCall("access", callExpr(selectExpr(id("shape"), "Accessor"), field("Path"))), guard(), assignCall("value", callExpr(selectExpr(id("access"), "Get"), id("cloned"))), guard(), assignStmt(id("assignments"), callExpr(id("append"), id("assignments"), assignment))}
	commit := runtimeCall("AssignFields", id("entity"), id("assignments"))
	commit.Ellipsis = 1
	body = append(body, &ast.RangeStmt{Key: id("_"), Value: id("index"), Tok: token.DEFINE, X: id("missing"), Body: &ast.BlockStmt{List: apply}}, returnStmt(commit))
	return &ast.FuncDecl{Name: id(name), Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("entity", record.value.pointerExpr()), namedField("previous", record.value.pointerExpr()), namedField("previousFields", selectExpr(id(e.l.handlerAlias), "FieldSet"))}}, Results: &ast.FieldList{List: []*ast.Field{{Type: id("error")}}}}, Body: &ast.BlockStmt{List: body}}, nil
}
