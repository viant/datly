package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// MutationProgramAsset assembles real policy phases. Support products remain
// separate source files in the same package; transaction completion stays in
// the canonical runtime adapter/engine.
type MutationProgramAsset struct {
	File                         *ast.File
	Factory, Definition, Program string
	Indexes                      *ReadIndexAsset
	Entities                     *EntityAsset
	Frames                       *MutationFrameAsset
	Hooks                        *MutationHookAsset
	Invariants                   *MutationInvariantAsset
	Actions                      *MutationActionAsset
	Validation                   *MutationValidationAsset
	Output                       *MutationOutputAsset
}

func MutationProgram(value *plan.Plan, config Config) (*MutationProgramAsset, error) {
	if config.PackagePath == "" {
		return nil, fmt.Errorf("mutation program requires canonical package identity")
	}
	l := &lowerer{plan: value, config: config}
	if err := l.prepare(); err != nil {
		return nil, err
	}
	asset := &MutationProgramAsset{Factory: l.factory, Definition: strings.TrimPrefix(l.factory, "New") + "Definition", Program: "_" + lowerInitial(l.factory) + "Program"}
	var err error
	if asset.Entities, err = (&mutationIdentityPolicy{}).entities(value, config); err != nil {
		return nil, err
	}
	if asset.Frames, err = MutationFrameSupport(value, config, asset.Entities); err != nil {
		return nil, err
	}
	if asset.Hooks, err = MutationHookSupport(value, config, asset.Frames.Layout); err != nil {
		return nil, err
	}
	if asset.Invariants, err = MutationInvariantSupport(value, config, asset.Entities, asset.Frames.Layout); err != nil {
		return nil, err
	}
	if asset.Actions, err = MutationActionSupport(value, config, asset.Entities, asset.Frames); err != nil {
		return nil, err
	}
	if asset.Validation, err = MutationValidationSupport(value, config, asset.Entities, asset.Frames); err != nil {
		return nil, err
	}
	if asset.Output, err = MutationOutputSupport(value, config); err != nil {
		return nil, err
	}
	for _, name := range []string{asset.Definition, asset.Program} {
		if err := l.reserveDeclaration(name); err != nil {
			return nil, err
		}
	}
	e := &programEmitter{l: l, asset: asset, policy: l.availableAlias("mutationPolicy")}
	l.pathsByAlias[e.policy] = "github.com/viant/xdatly/handler/mutation"
	e.shape = l.availableAlias("xshape")
	l.pathsByAlias[e.shape] = "github.com/viant/x/shape"
	l.pathsByAlias[l.fmtAlias] = "fmt"
	if asset.Indexes, err = l.readIndexes(); err != nil {
		return nil, err
	}
	asset.File = e.file()
	return asset, nil
}

// Files returns the complete generated package source products in stable order.
func (a *MutationProgramAsset) Files() ([]*ast.File, error) {
	if a == nil || a.Entities == nil || a.Frames == nil || a.Frames.Previous == nil || a.Frames.Layout == nil || a.Actions == nil || a.Validation == nil || a.Output == nil {
		return nil, fmt.Errorf("complete mutation program products are required")
	}
	files := []*ast.File{a.File, a.Entities.File, a.Frames.File, a.Frames.Previous.File, a.Frames.Layout.File, a.Actions.File, a.Output.File}
	files = append(files, a.Validation.File)
	if a.Indexes != nil {
		files = append(files, a.Indexes.File)
	}
	if a.Hooks != nil {
		files = append(files, a.Hooks.File)
	}
	if a.Invariants != nil {
		files = append(files, a.Invariants.File)
	}
	for _, file := range files {
		if file == nil {
			return nil, fmt.Errorf("mutation program source file is missing")
		}
	}
	return files, nil
}

type programEmitter struct {
	l             *lowerer
	asset         *MutationProgramAsset
	policy, shape string
}

func (e *programEmitter) member(name string) ast.Expr { return selectExpr(ast.NewIdent("p"), name) }
func (e *programEmitter) policyType(name string, types ...ast.Expr) ast.Expr {
	return &ast.IndexListExpr{X: selectExpr(ast.NewIdent(e.policy), name), Indices: types}
}
func (e *programEmitter) ioTypes() []ast.Expr {
	return []ast.Expr{parseExpr(e.l.config.InputType), parseExpr(e.l.config.OutputType)}
}
func (e *programEmitter) failure(message string) ast.Expr {
	return callExpr(selectExpr(ast.NewIdent(e.l.fmtAlias), "Errorf"), stringExpr(message))
}
func (e *programEmitter) isNil(value ast.Expr) ast.Expr {
	return callExpr(selectExpr(&ast.ParenExpr{X: &ast.CompositeLit{Type: selectExpr(ast.NewIdent(e.shape), "Runtime")}}, "IsNil"), value)
}
func (e *programEmitter) method(receiverType, name string, params, results []*ast.Field, body []ast.Stmt) *ast.FuncDecl {
	return &ast.FuncDecl{Name: ast.NewIdent(name), Recv: &ast.FieldList{List: []*ast.Field{namedField("p", &ast.StarExpr{X: ast.NewIdent(receiverType)})}}, Type: &ast.FuncType{Params: &ast.FieldList{List: params}, Results: &ast.FieldList{List: results}}, Body: &ast.BlockStmt{List: body}}
}
func (e *programEmitter) contextParam() *ast.Field {
	return namedField("ctx", selectExpr(ast.NewIdent(e.l.contextAlias), "Context"))
}
func (e *programEmitter) phase(name string, stage int, body []ast.Stmt, extra ...*ast.Field) ast.Decl {
	id := ast.NewIdent
	nilExpr := id("nil")
	guard := func(condition ast.Expr, message string) ast.Stmt {
		return &ast.IfStmt{Cond: condition, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.failure(message))}}}
	}
	checks := []ast.Stmt{guard(&ast.BinaryExpr{X: id("p"), Op: token.EQL, Y: nilExpr}, "mutation program is required"), guard(&ast.BinaryExpr{X: &ast.BinaryExpr{X: e.member("failed"), Op: token.LOR, Y: e.member("finalized")}, Op: token.LOR, Y: &ast.BinaryExpr{X: e.member("stage"), Op: token.NEQ, Y: &ast.BasicLit{Kind: token.INT, Value: strconv.Itoa(stage)}}}, "mutation program phase "+name+" is out of order or failed"), assignStmt(e.member("failed"), id("true")), guard(e.isNil(id("ctx")), "mutation context is required"), errorGuard(callExpr(selectExpr(id("ctx"), "Err")))}
	checks = append(checks, body...)
	checks = append(checks, assignStmt(e.member("stage"), &ast.BasicLit{Kind: token.INT, Value: strconv.Itoa(stage + 1)}), assignStmt(e.member("failed"), id("false")), returnStmt(nilExpr))
	return e.method(e.asset.Program, name, append([]*ast.Field{e.contextParam()}, extra...), []*ast.Field{{Type: id("error")}}, checks)
}
func (e *programEmitter) delegate(owner, name string, args ...ast.Expr) ast.Stmt {
	return errorGuard(callExpr(selectExpr(e.member(owner), name), append([]ast.Expr{ast.NewIdent("ctx")}, args...)...))
}
func (e *programEmitter) project() ast.Stmt {
	return errorGuard(callExpr(ast.NewIdent(e.asset.Output.Function), e.member("input"), e.member("output")))
}

func (e *programEmitter) file() *ast.File {
	id := ast.NewIdent
	finalizer := e.policyType("Finalizer", e.ioTypes()...)
	structure := func(name string, fields []*ast.Field) ast.Decl {
		return &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: id(name), Type: &ast.StructType{Fields: &ast.FieldList{List: fields}}}}}
	}
	fields := []*ast.Field{namedField("input", &ast.StarExpr{X: parseExpr(e.l.config.InputType)}), namedField("output", &ast.StarExpr{X: parseExpr(e.l.config.OutputType)}), namedField("finalizer", finalizer), namedField("original", &ast.StarExpr{X: id(e.asset.Entities.SnapshotType)}), namedField("database", &ast.StarExpr{X: id(e.asset.Frames.TypeName)}), namedField("frames", &ast.StarExpr{X: id(e.asset.Frames.Layout.TypeName)}), namedField("actions", &ast.StarExpr{X: id(e.asset.Actions.TypeName)}), namedField("stage", id("int")), namedField("failed", id("bool")), namedField("finalized", id("bool"))}
	fields = append(fields, namedField("validation", &ast.StarExpr{X: id(e.asset.Validation.TypeName)}))
	if e.asset.Hooks != nil {
		fields = append(fields, namedField("hooks", &ast.StarExpr{X: id(e.asset.Hooks.TypeName)}))
	}
	file := &ast.File{Name: id(e.l.config.Package), Decls: []ast.Decl{structure(e.asset.Definition, e.definitionFields(finalizer)), structure(e.asset.Program, fields)}}
	file.Decls = append(file.Decls, &ast.FuncDecl{Name: id(e.asset.Factory), Type: &ast.FuncType{Params: &ast.FieldList{}, Results: &ast.FieldList{List: []*ast.Field{{Type: e.policyType("Definition", e.ioTypes()...)}}}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(&ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: id(e.asset.Definition)}})}}}, e.capture(), e.finalize(false), e.finalize(true))
	prepare := []ast.Stmt{}
	if e.asset.Hooks != nil {
		prepare = append(prepare, e.delegate("hooks", "Prepare", id("binder"), e.member("output")))
	}
	prepare = append(prepare, e.delegate("actions", "Prepare", id("binder")))
	prepare = append(prepare, e.delegate("validation", "Prepare", id("binder")))
	file.Decls = append(file.Decls, e.phase("Prepare", 1, prepare, namedField("binder", selectExpr(id(e.l.handlerAlias), "Binder"))))
	sync := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{id("synced"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(e.member("original"), e.asset.Entities.SyncMethod), e.member("input"))}}, errorGuard(id("err")), &ast.AssignStmt{Lhs: []ast.Expr{e.member("frames"), id("err")}, Tok: token.ASSIGN, Rhs: []ast.Expr{callExpr(selectExpr(e.member("database"), e.asset.Frames.BuildMethod), e.member("input"), id("synced"))}}, errorGuard(id("err")), e.project()}
	sync = append(sync, e.delegate("actions", "CapturePrevious", e.member("frames")))
	file.Decls = append(file.Decls, e.phase("SyncPresence", 2, sync))
	for index, name := range []string{"Invariants", "Init", "Validate", "Sequence", "AfterSequence", "Diff", "Reconcile", "Queue", "AfterQueue"} {
		body := []ast.Stmt{e.delegate("actions", "VerifyPrevious", e.member("frames"))}
		switch name {
		case "Invariants":
			if e.asset.Invariants != nil {
				body = append(body, errorGuard(callExpr(id(e.asset.Invariants.Function), id("ctx"), e.member("frames"))))
			}
		case "Init", "Validate", "AfterSequence", "AfterQueue":
			if name == "Validate" {
				body = append(body, e.delegate("actions", "CaptureValidation", e.member("frames")), e.delegate("validation", "Validate", e.member("frames")), e.delegate("actions", "VerifyValidation", e.member("frames")))
			}
			if e.asset.Hooks != nil {
				body = append(body, e.delegate("hooks", name, e.member("frames")))
			}
			if name == "Validate" {
				body = append(body, e.delegate("actions", "VerifyValidation", e.member("frames")))
			}
			body = append(body, errorGuard(callExpr(selectExpr(e.member("frames"), e.asset.Frames.VerifyMethod), e.member("input"))))
			if name == "AfterQueue" {
				body = append(body, e.delegate("actions", "VerifyQueued", e.member("frames")), e.project())
			}
		default:
			if name == "Queue" {
				body = append(body, e.delegate("actions", "VerifyRelations", e.member("frames")), e.delegate("actions", "VerifyValidation", e.member("frames")), e.delegate("actions", "VerifyPayload", e.member("frames")), e.project())
			}
			body = append(body, e.delegate("actions", name, e.member("frames")))
			if name == "Reconcile" {
				body = append(body, e.delegate("actions", "VerifyPayload", e.member("frames")), e.delegate("actions", "CaptureValidation", e.member("frames")), e.delegate("validation", "ValidateFinal", e.member("frames"), e.member("actions")), e.delegate("actions", "VerifyValidation", e.member("frames")), e.delegate("actions", "VerifyPayload", e.member("frames")))
			}
		}
		body = append(body, e.delegate("actions", "VerifyPrevious", e.member("frames")))
		file.Decls = append(file.Decls, e.phase(name, index+3, body))
	}
	required := &ast.BinaryExpr{X: &ast.BinaryExpr{X: &ast.BinaryExpr{X: id("p"), Op: token.NEQ, Y: id("nil")}, Op: token.LAND, Y: &ast.UnaryExpr{Op: token.NOT, X: e.member("failed")}}, Op: token.LAND, Y: &ast.BinaryExpr{X: e.member("stage"), Op: token.EQL, Y: &ast.BasicLit{Kind: token.INT, Value: "6"}}}
	required = &ast.BinaryExpr{X: required, Op: token.LAND, Y: callExpr(selectExpr(e.member("actions"), "RequiresTransaction"), e.member("frames"))}
	file.Decls = append(file.Decls, e.method(e.asset.Program, "RequiresTransaction", nil, []*ast.Field{{Type: id("bool")}}, []ast.Stmt{returnStmt(required)}), e.method(e.asset.Program, "Output", nil, []*ast.Field{{Type: &ast.StarExpr{X: parseExpr(e.l.config.OutputType)}}}, []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("p"), Op: token.EQL, Y: id("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("nil"))}}}, returnStmt(e.member("output"))}))
	file.Decls = append([]ast.Decl{(&entityEmitter{l: e.l}).importDeclaration(file)}, file.Decls...)
	return file
}
