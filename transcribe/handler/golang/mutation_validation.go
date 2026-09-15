package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// MutationValidationAsset validates every typed frame before custom Validate
// hooks. It shares action selection with diff and consumes native scoped checks.
type MutationValidationAsset struct {
	File     *ast.File
	TypeName string
}
type validationEmitter struct {
	action                     *actionEmitter
	name, fields, reflectAlias string
}

func MutationValidationSupport(value *plan.Plan, config Config, entities *EntityAsset, frames *MutationFrameAsset) (*MutationValidationAsset, error) {
	a, err := newMutationActionEmitter(value, config, entities, frames)
	if err != nil {
		return nil, err
	}
	for _, role := range a.roles {
		if value.Operation == plan.OperationPut && role.record.plan.Write.Existing == plan.ActionUpdate && role.record.plan.Current == nil {
			return nil, fmt.Errorf("framework validation for update role %s requires an authored Current prior read", role.record.plan.Identity)
		}
	}
	e := &validationEmitter{action: a, name: "_" + lowerInitial(a.l.factory) + "FrameworkValidation"}
	e.fields = e.name + "Fields"
	for _, name := range []string{e.name, e.fields} {
		if err := a.l.reserveDeclaration(name); err != nil {
			return nil, err
		}
	}
	e.reflectAlias = a.l.availableAlias("reflect")
	a.l.pathsByAlias[e.reflectAlias] = "reflect"
	a.l.importsByPath["reflect"] = e.reflectAlias
	file, err := e.file()
	if err != nil {
		return nil, err
	}
	return &MutationValidationAsset{File: file, TypeName: e.name}, nil
}

func (e *validationEmitter) member(name string) ast.Expr {
	return selectExpr(ast.NewIdent("checks"), name)
}
func (e *validationEmitter) accessName(role actionRole, field plan.EntityField) string {
	return "mark" + strconv.Itoa(role.record.order) + field.Name
}
func (e *validationEmitter) method(name string, params []*ast.Field, body []ast.Stmt) ast.Decl {
	return &ast.FuncDecl{Name: ast.NewIdent(name), Recv: &ast.FieldList{List: []*ast.Field{namedField("checks", &ast.StarExpr{X: ast.NewIdent(e.name)})}}, Type: &ast.FuncType{Params: &ast.FieldList{List: params}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: body}}
}
func (e *validationEmitter) guard(condition ast.Expr, message string) ast.Stmt {
	return &ast.IfStmt{Cond: condition, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.action.errorExpr(message))}}}
}

func (e *validationEmitter) file() (*ast.File, error) {
	id := ast.NewIdent
	a := e.action
	validator := namedField("Validator", selectExpr(id(a.l.handlerAlias), "Validator"))
	validator.Tag = &ast.BasicLit{Kind: token.STRING, Value: "`bind:\"kind=frameworkValidator\"`"}
	fields := []*ast.Field{validator, namedField("ready", id("bool")), namedField("failed", id("bool")), namedField("businessComplete", id("bool")), namedField("finalComplete", id("bool"))}
	for _, role := range a.roles {
		fields = append(fields, namedField(role.field+"Policies", &ast.MapType{Key: &ast.StarExpr{X: id(role.frame.FrameType)}, Value: selectExpr(id(a.l.handlerAlias), "ValidationOptions")}))
		for _, field := range role.record.plan.Entity.Fields {
			if field.Relation {
				continue
			}
			fields = append(fields, namedField(e.accessName(role, field), &ast.StarExpr{X: selectExpr(id(a.shape), "Accessor")}))
		}
	}
	file := &ast.File{Name: id(a.l.config.Package), Decls: []ast.Decl{
		&ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: id(e.name), Type: &ast.StructType{Fields: &ast.FieldList{List: fields}}}}},
		&ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: id(e.fields), Type: &ast.MapType{Key: id("string"), Value: id("bool")}}}},
		&ast.FuncDecl{Name: id("Has"), Recv: &ast.FieldList{List: []*ast.Field{namedField("fields", id(e.fields))}}, Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("name", id("string"))}}, Results: &ast.FieldList{List: []*ast.Field{{Type: id("bool")}}}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(&ast.IndexExpr{X: id("fields"), Index: id("name")})}}},
	}}
	prepare := []ast.Stmt{e.guard(&ast.BinaryExpr{X: id("checks"), Op: token.EQL, Y: id("nil")}, "framework validation state is required"), e.guard(&ast.BinaryExpr{X: e.member("ready"), Op: token.LOR, Y: e.member("failed")}, "framework validation preparation already attempted"), assignStmt(e.member("failed"), id("true")), e.guard(a.isNil(id("ctx")), "framework validation context is required"), errorGuard(callExpr(selectExpr(id("ctx"), "Err"))), e.guard(a.isNil(id("binder")), "framework validation binder is required"), errorGuard(callExpr(selectExpr(id("binder"), "Bind"), id("ctx"), id("checks"))), e.guard(a.isNil(e.member("Validator")), "framework validator is unavailable")}
	for _, role := range a.roles {
		for _, field := range role.record.plan.Entity.Fields {
			if field.Relation {
				continue
			}
			typ := callExpr(selectExpr(callExpr(selectExpr(id(e.reflectAlias), "TypeOf"), callExpr(role.record.value.pointerExpr(), id("nil"))), "Elem"))
			path := role.record.plan.Entity.MarkerField + "." + field.Name
			markerKind := callExpr(selectExpr(callExpr(selectExpr(id("access"), "Type")), "Kind"))
			prepare = append(prepare, &ast.IfStmt{Init: &ast.AssignStmt{Lhs: []ast.Expr{id("access"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(callExpr(selectExpr(id(a.shape), "Linked"), typ), "Accessor"), stringExpr(path))}}, Cond: &ast.BinaryExpr{X: id("err"), Op: token.NEQ, Y: id("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("err"))}}, Else: &ast.BlockStmt{List: []ast.Stmt{e.guard(&ast.BinaryExpr{X: markerKind, Op: token.NEQ, Y: selectExpr(id(e.reflectAlias), "Bool")}, "framework validation marker "+path+" must be boolean"), assignStmt(e.member(e.accessName(role, field)), id("access"))}}})
		}
	}
	prepare = append(prepare, assignStmt(e.member("ready"), id("true")), assignStmt(e.member("failed"), id("false")), returnStmt(id("nil")))
	file.Decls = append(file.Decls, e.method("Prepare", []*ast.Field{namedField("ctx", selectExpr(id(a.l.contextAlias), "Context")), namedField("binder", selectExpr(id(a.l.handlerAlias), "Binder"))}, prepare))
	validate, err := e.validate(false)
	if err != nil {
		return nil, err
	}
	file.Decls = append(file.Decls, validate)
	final, err := e.validate(true)
	if err != nil {
		return nil, err
	}
	file.Decls = append(file.Decls, final)
	file.Decls = append([]ast.Decl{(&entityEmitter{l: a.l}).importDeclaration(file)}, file.Decls...)
	return file, nil
}

func (e *validationEmitter) coverage(role actionRole) []ast.Stmt {
	id := ast.NewIdent
	body := []ast.Stmt{defineStmt("coverage", &ast.CompositeLit{Type: id(e.fields)})}
	for _, field := range role.record.plan.Entity.Fields {
		if field.Relation {
			continue
		}
		body = append(body, &ast.BlockStmt{List: []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{id("mark"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(e.member(e.accessName(role, field)), "Get"), selectExpr(id("frame"), "Entity"))}}, errorGuard(id("err")), assignStmt(&ast.IndexExpr{X: id("coverage"), Index: stringExpr(field.Name)}, &ast.BinaryExpr{X: callExpr(selectExpr(id("mark"), "Bool")), Op: token.LOR, Y: callExpr(selectExpr(id("original"), "Has"), stringExpr(field.Name))})}})
	}
	for _, group := range role.record.plan.Entity.Invariants {
		var active ast.Expr = id("false")
		var fill []ast.Stmt
		for _, name := range group.Fields {
			at := &ast.IndexExpr{X: id("coverage"), Index: stringExpr(name)}
			active = &ast.BinaryExpr{X: active, Op: token.LOR, Y: at}
			fill = append(fill, assignStmt(at, id("true")))
		}
		body = append(body, &ast.IfStmt{Cond: active, Body: &ast.BlockStmt{List: fill}})
	}
	return body
}

func (e *validationEmitter) validate(final bool) (ast.Decl, error) {
	id := ast.NewIdent
	a := e.action
	body := []ast.Stmt{e.guard(&ast.BinaryExpr{X: id("checks"), Op: token.EQL, Y: id("nil")}, "framework validation state is required"), e.guard(&ast.UnaryExpr{Op: token.NOT, X: e.member("ready")}, "framework validation is not prepared"), e.guard(&ast.BinaryExpr{X: id("frames"), Op: token.EQL, Y: id("nil")}, "framework validation frames are required"), e.guard(a.isNil(id("ctx")), "framework validation context is required"), errorGuard(callExpr(selectExpr(id("ctx"), "Err"))), defineStmt("result", &ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: selectExpr(id(a.l.handlerAlias), "Validation")}})}
	invalid := ast.Expr(e.member("businessComplete"))
	if final {
		invalid = &ast.BinaryExpr{X: &ast.UnaryExpr{Op: token.NOT, X: e.member("businessComplete")}, Op: token.LOR, Y: e.member("finalComplete")}
	}
	body = append(body, e.guard(&ast.BinaryExpr{X: e.member("failed"), Op: token.LOR, Y: invalid}, "framework validation phase is out of order or failed"), assignStmt(e.member("failed"), id("true")))
	if final {
		body = append(body, e.guard(&ast.BinaryExpr{X: id("actions"), Op: token.EQL, Y: id("nil")}, "final validation requires reconciled actions"), errorGuard(callExpr(selectExpr(id("actions"), "VerifyRelations"), id("ctx"), id("frames"))))
	}

	if !final {
		concurrency, err := e.concurrency()
		if err != nil {
			return nil, err
		}
		body = append(body, concurrency...)
		graph, err := e.deletionGraph()
		if err != nil {
			return nil, err
		}
		body = append(body, graph...)
	}
	batches := e.batches()
	for _, batch := range batches {
		body = append(body, e.declareBatch(batch)...)
	}
	for _, role := range a.roles {
		policies := e.member(role.field + "Policies")
		if !final {
			body = append(body, assignStmt(policies, callExpr(id("make"), &ast.MapType{Key: &ast.StarExpr{X: id(role.frame.FrameType)}, Value: selectExpr(id(a.l.handlerAlias), "ValidationOptions")})))
		}
		loop := a.original(role)
		loop = append(loop, a.decisionIdentity(role, "_", "supplied")...)
		loop = append(loop, assignStmt(id("_"), id("supplied")))
		selection, action, err := a.actionSelection(role)
		if err != nil {
			return nil, err
		}
		loop = append(loop, selection...)
		loop = append(loop, &ast.IfStmt{Cond: &ast.BinaryExpr{X: action, Op: token.EQL, Y: selectExpr(id(a.l.handlerAlias), "WriteDelete")}, Body: &ast.BlockStmt{List: []ast.Stmt{&ast.BranchStmt{Tok: token.CONTINUE}}}})
		missingPrevious := &ast.BinaryExpr{X: &ast.BinaryExpr{X: action, Op: token.EQL, Y: selectExpr(id(a.l.handlerAlias), "WriteUpdate")}, Op: token.LAND, Y: &ast.BinaryExpr{X: selectExpr(selectExpr(id("frame"), "State"), "Previous"), Op: token.EQL, Y: id("nil")}}
		loop = append(loop, e.guard(missingPrevious, "framework validation of update requires a matched Previous row"))
		loop = append(loop, e.coverage(role)...)
		options := &ast.CompositeLit{Type: selectExpr(id(a.l.handlerAlias), "ValidationOptions"), Elts: []ast.Expr{&ast.KeyValueExpr{Key: id("Action"), Value: action}, &ast.KeyValueExpr{Key: id("Shallow"), Value: id("true")}, &ast.KeyValueExpr{Key: id("Location"), Value: callExpr(selectExpr(id(a.l.fmtAlias), "Sprintf"), stringExpr(strings.Join(role.record.plan.InputPath, ".")+"[%d]"), id("ordinal"))}}}
		loop = append(loop, defineStmt("options", options), &ast.IfStmt{Cond: &ast.BinaryExpr{X: action, Op: token.EQL, Y: selectExpr(id(a.l.handlerAlias), "WriteUpdate")}, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(selectExpr(id("options"), "Previous"), selectExpr(selectExpr(id("frame"), "State"), "Previous")), assignStmt(selectExpr(id("options"), "PreviousFields"), selectExpr(selectExpr(id("frame"), "State"), "PreviousFields")), assignStmt(selectExpr(id("options"), "Fields"), id("coverage"))}}}, errorGuard(callExpr(selectExpr(id("ctx"), "Err"))))
		loop = append(loop, e.producerPolicy(role, final)...)
		if final {
			loop = append(loop, e.receipts(role)...)
		}
		batch := e.batchFor(role, batches)
		loop = append(loop, assignStmt(id(batch.candidates), callExpr(id("append"), id(batch.candidates), selectExpr(id("frame"), "Entity"))), assignStmt(id(batch.options), callExpr(id("append"), id(batch.options), id("options"))))
		body = append(body, &ast.RangeStmt{Key: id("ordinal"), Value: id("frame"), Tok: token.DEFINE, X: selectExpr(id("frames"), role.frame.Field), Body: &ast.BlockStmt{List: loop}})
	}
	for _, batch := range batches {
		body = append(body, e.validateBatch(batch)...)
	}
	name, complete := "Validate", "businessComplete"
	if final {
		name, complete = "ValidateFinal", "finalComplete"
	}
	body = append(body, errorGuard(callExpr(selectExpr(id("result"), "Err"))), assignStmt(e.member(complete), id("true")), assignStmt(e.member("failed"), id("false")), returnStmt(id("nil")))
	params := []*ast.Field{namedField("ctx", selectExpr(id(a.l.contextAlias), "Context")), namedField("frames", &ast.StarExpr{X: id(a.frames.TypeName)})}
	if final {
		params = append(params, namedField("actions", &ast.StarExpr{X: id(a.name)}))
	}
	return e.method(name, params, body), nil
}
