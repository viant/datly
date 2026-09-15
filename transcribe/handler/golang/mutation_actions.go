package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"

	plan "github.com/viant/datly/transcribe/handler/ast"
	xhandler "github.com/viant/xdatly/handler"
)

// MutationActionAsset is typed policy lowering, never a transaction owner.
type MutationActionAsset struct {
	File     *ast.File
	TypeName string
}

func MutationActionSupport(value *plan.Plan, config Config, entities *EntityAsset, frames *MutationFrameAsset) (*MutationActionAsset, error) {
	e, err := newMutationActionEmitter(value, config, entities, frames)
	if err != nil {
		return nil, err
	}
	file, err := e.file()
	if err != nil {
		return nil, err
	}
	return &MutationActionAsset{File: file, TypeName: e.name}, nil
}

func newMutationActionEmitter(value *plan.Plan, config Config, entities *EntityAsset, frames *MutationFrameAsset) (*actionEmitter, error) {
	l := &lowerer{plan: value, config: config}
	if err := l.prepare(); err != nil {
		return nil, err
	}
	if entities == nil || frames == nil || frames.Layout == nil || frames.VerifyMethod == "" || frames.Layout.OrderField == "" {
		return nil, fmt.Errorf("mutation actions require entity and frame products")
	}
	e := &actionEmitter{l: l, entities: entities, frames: frames.Layout, verify: frames.VerifyMethod, name: "_" + lowerInitial(l.factory) + "MutationActions"}
	e.shape = l.availableAlias("xshape")
	l.pathsByAlias[e.shape] = "github.com/viant/x/shape"
	l.pathsByAlias[l.fmtAlias] = "fmt"
	for _, record := range l.records {
		if record.plan.Auxiliary {
			continue
		}
		if record.plan.Entity == nil {
			return nil, fmt.Errorf("mutation role %s requires entity metadata", record.plan.Identity)
		}
		if record.plan.Entity.LateWrite.Unresolved != "" {
			return nil, fmt.Errorf("generic mutation native policy for %s is unresolved: %s", record.plan.Identity, record.plan.Entity.LateWrite.Unresolved)
		}
		for _, allowed := range record.plan.Write.Allowed {
			effects := record.plan.Entity.LateWrite
			if allowed == plan.ActionInsert {
				if effects.InsertHook {
					return nil, fmt.Errorf("generic mutation cannot execute native OnInsert for %s after validation; use pre-validation entity hooks or custom orchestration", record.plan.Identity)
				}
				if len(effects.DefaultFields) > 0 {
					return nil, fmt.Errorf("generic mutation cannot execute native default generators for %s fields %v after validation; initialize them before validation or use custom orchestration", record.plan.Identity, effects.DefaultFields)
				}
			}
			if allowed == plan.ActionUpdate && effects.UpdateHook {
				return nil, fmt.Errorf("generic mutation cannot execute native OnUpdate for %s after validation; use pre-validation entity hooks or custom orchestration", record.plan.Identity)
			}
		}
		frame, err := frames.Layout.role(record.plan)
		if err != nil {
			return nil, err
		}
		association, err := (&previousEmitter{l: l, entities: entities}).association(record)
		if err != nil {
			return nil, err
		}
		if record.plan.Sequence != nil && record.plan.Sequence.Field.Field == "" {
			return nil, fmt.Errorf("mutation sequence for %s requires canonical local field authority", record.plan.Identity)
		}
		for _, action := range record.plan.Write.Allowed {
			if action != plan.ActionInsert && action != plan.ActionUpdate {
				return nil, fmt.Errorf("mutation action %s is unsupported", action)
			}
		}
		e.roles = append(e.roles, actionRole{record: record, frame: frame, association: association, entry: e.name + "Row" + strconv.Itoa(record.order), field: "role" + strconv.Itoa(record.order)})
	}
	return e, nil
}

type actionRole struct {
	record       *recordLowering
	frame        MutationFrameRole
	association  EntityAssociation
	entry, field string
}
type actionEmitter struct {
	l                   *lowerer
	entities            *EntityAsset
	frames              *MutationFrameLayout
	roles               []actionRole
	name, shape, verify string
}

func (e *actionEmitter) errorExpr(message string) ast.Expr {
	return callExpr(selectExpr(ast.NewIdent(e.l.fmtAlias), "Errorf"), stringExpr(message))
}
func (e *actionEmitter) isNil(value ast.Expr) ast.Expr {
	return callExpr(selectExpr(&ast.ParenExpr{X: &ast.CompositeLit{Type: selectExpr(ast.NewIdent(e.shape), "Runtime")}}, "IsNil"), value)
}
func (e *actionEmitter) method(name string, params, results []*ast.Field, body []ast.Stmt) *ast.FuncDecl {
	return &ast.FuncDecl{Name: ast.NewIdent(name), Recv: &ast.FieldList{List: []*ast.Field{namedField("actions", &ast.StarExpr{X: ast.NewIdent(e.name)})}}, Type: &ast.FuncType{Params: &ast.FieldList{List: params}, Results: &ast.FieldList{List: results}}, Body: &ast.BlockStmt{List: body}}
}
func (e *actionEmitter) file() (*ast.File, error) {
	file := &ast.File{Name: ast.NewIdent(e.l.config.Package)}
	fields := []*ast.Field{namedField("stage", ast.NewIdent("int")), namedField("failed", ast.NewIdent("bool")), namedField("frames", &ast.StarExpr{X: ast.NewIdent(e.frames.TypeName)}), namedField("previousCaptured", ast.NewIdent("bool"))}
	input := namedField("Input", &ast.StarExpr{X: parseExpr(e.l.config.InputType)})
	input.Tag = &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(capabilityTag(xhandler.InputKey))}
	fields = append(fields, input)
	fields = append(fields, namedField("validationState", &ast.StarExpr{X: parseExpr(e.l.config.InputType)}))
	dml := namedField("DML", selectExpr(ast.NewIdent(e.l.handlerAlias), "DML"))
	dml.Tag = &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(capabilityTag(xhandler.DMLKey))}
	fields = append(fields, dml)
	sequence := false
	for _, role := range e.roles {
		if role.record.plan.Sequence != nil {
			sequence = true
		}
	}
	if sequence {
		field := namedField("Sequencer", selectExpr(ast.NewIdent(e.l.handlerAlias), "Sequencer"))
		field.Tag = &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(capabilityTag(xhandler.SequencerKey))}
		fields = append(fields, field)
	}
	for _, role := range e.roles {
		fields = append(fields, namedField(role.field, &ast.ArrayType{Elt: &ast.StarExpr{X: ast.NewIdent(role.entry)}}))
		fields = append(fields, namedField(role.field+"ByEntity", &ast.MapType{Key: role.record.value.pointerExpr(), Value: &ast.StarExpr{X: ast.NewIdent(role.entry)}}), namedField(role.field+"Positions", &ast.MapType{Key: role.record.value.pointerExpr(), Value: ast.NewIdent("int")}))
		fields = append(fields, namedField(role.field+"Validated", &ast.MapType{Key: role.record.value.pointerExpr(), Value: role.record.value.pointerExpr()}))
		framePointer := &ast.StarExpr{X: ast.NewIdent(role.frame.FrameType)}
		fields = append(fields, namedField(role.field+"PreviousState", &ast.MapType{Key: framePointer, Value: role.record.value.pointerExpr()}))
		fields = append(fields, namedField(role.field+"PreviousFields", &ast.MapType{Key: framePointer, Value: &ast.MapType{Key: ast.NewIdent("string"), Value: ast.NewIdent("bool")}}))
		fields = append(fields, namedField(role.field+"QueuedState", &ast.MapType{Key: role.record.value.pointerExpr(), Value: role.record.value.pointerExpr()}))
		entryFields := []*ast.Field{namedField("Frame", &ast.StarExpr{X: ast.NewIdent(role.frame.FrameType)}), namedField("Action", selectExpr(ast.NewIdent(e.l.handlerAlias), "WriteAction")), namedField("IdentityKey", ast.NewIdent(role.association.KeyType)), namedField("IdentityAssigned", ast.NewIdent("bool")), namedField("Payload", role.record.value.pointerExpr())}
		file.Decls = append(file.Decls, &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: ast.NewIdent(role.entry), Type: &ast.StructType{Fields: &ast.FieldList{List: entryFields}}}}})
		file.Decls = append(file.Decls, e.payloadOptions(role))
	}
	file.Decls = append(file.Decls, &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: ast.NewIdent(e.name), Type: &ast.StructType{Fields: &ast.FieldList{List: fields}}}}}, e.prepare(sequence), e.requiresTransaction())
	diff, err := e.diff()
	if err != nil {
		return nil, err
	}
	reconcile, err := e.reconcile()
	if err != nil {
		return nil, err
	}
	queue, err := e.queue()
	if err != nil {
		return nil, err
	}
	sequenceMethod, err := e.sequence()
	if err != nil {
		return nil, err
	}
	file.Decls = append(file.Decls, sequenceMethod, diff, reconcile, queue, e.verifyQueued())
	file.Decls = append(file.Decls, (actionValidationEmitter{action: e}).declarations()...)
	proof, err := e.verifyRelations()
	if err != nil {
		return nil, err
	}
	file.Decls = append(file.Decls, proof)
	file.Decls = append([]ast.Decl{(&entityEmitter{l: e.l}).importDeclaration(file)}, file.Decls...)
	return file, nil
}

func (e *actionEmitter) prepare(sequence bool) ast.Decl {
	nilExpr := ast.NewIdent("nil")
	actions := ast.NewIdent("actions")
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: actions, Op: token.EQL, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation action state is required"))}}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: selectExpr(actions, "failed"), Op: token.LOR, Y: &ast.BinaryExpr{X: selectExpr(actions, "stage"), Op: token.NEQ, Y: &ast.BasicLit{Kind: token.INT, Value: "0"}}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation dependencies may only be prepared once"))}}}, assignStmt(selectExpr(actions, "failed"), ast.NewIdent("true")), &ast.IfStmt{Cond: e.isNil(ast.NewIdent("binder")), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("mutation dependency binder is required"))}}}, errorGuard(callExpr(selectExpr(ast.NewIdent("binder"), "Bind"), ast.NewIdent("ctx"), actions)), &ast.IfStmt{Cond: e.isNil(selectExpr(actions, "DML")), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("scoped DML capability is required"))}}}}
	if sequence {
		body = append(body, &ast.IfStmt{Cond: e.isNil(selectExpr(actions, "Sequencer")), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("scoped sequencer capability is required"))}}})
	}
	body = append(body, &ast.IfStmt{Cond: e.isNil(selectExpr(actions, "Input")), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("canonical invocation input is required"))}}})
	body = append(body, assignStmt(selectExpr(actions, "stage"), &ast.BasicLit{Kind: token.INT, Value: "1"}), assignStmt(selectExpr(actions, "failed"), ast.NewIdent("false")), returnStmt(nilExpr))
	return e.method("Prepare", []*ast.Field{namedField("ctx", selectExpr(ast.NewIdent(e.l.contextAlias), "Context")), namedField("binder", selectExpr(ast.NewIdent(e.l.handlerAlias), "Binder"))}, []*ast.Field{{Type: ast.NewIdent("error")}}, body)
}

func (e *actionEmitter) requiresTransaction() ast.Decl {
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("frames"), Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("false"))}}}}
	for _, role := range e.roles {
		if len(role.record.plan.Write.Allowed) == 0 {
			continue
		}
		body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: callExpr(ast.NewIdent("len"), selectExpr(ast.NewIdent("frames"), role.frame.Field)), Op: token.GTR, Y: &ast.BasicLit{Kind: token.INT, Value: "0"}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(ast.NewIdent("true"))}}})
	}
	body = append(body, returnStmt(ast.NewIdent("false")))
	return e.method("RequiresTransaction", []*ast.Field{namedField("frames", &ast.StarExpr{X: ast.NewIdent(e.frames.TypeName)})}, []*ast.Field{{Type: ast.NewIdent("bool")}}, body)
}
