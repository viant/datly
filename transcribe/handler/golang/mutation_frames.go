package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// MutationFrameAsset composes capture and frame preparation from the same
// immutable record plan. The caller captures Current before input initialization.
type MutationFrameAsset struct {
	File                                   *ast.File
	Layout                                 *MutationFrameLayout
	Previous                               *MutationPreviousAsset
	TypeName, CaptureFunction, BuildMethod string
	VerifyMethod                           string
}

func MutationFrameSupport(value *plan.Plan, config Config, entities *EntityAsset) (*MutationFrameAsset, error) {
	if entities == nil || entities.CaptureFunction == "" || entities.SyncContextType == "" {
		return nil, fmt.Errorf("mutation frame support requires a complete original entity snapshot product")
	}
	l := &lowerer{plan: value, config: config}
	if err := l.prepare(); err != nil {
		return nil, err
	}
	layout, err := l.mutationFrames()
	if err != nil {
		return nil, err
	}
	if entities.identity != nil {
		for _, association := range entities.Associations {
			for _, role := range layout.Roles {
				if role.Identity != association.Identity || strings.Join(role.Path, ".") != strings.Join(association.Path, ".") {
					continue
				}
				for _, declaration := range layout.File.Decls {
					decl, ok := declaration.(*ast.GenDecl)
					if !ok || decl.Tok != token.TYPE {
						continue
					}
					for _, item := range decl.Specs {
						typ := item.(*ast.TypeSpec)
						if typ.Name.Name != role.FrameType {
							continue
						}
						fields := typ.Type.(*ast.StructType).Fields
						fields.List = append(fields.List, namedField("identityKey", ast.NewIdent(association.KeyType)), namedField("identityKnown", ast.NewIdent(association.KeyType+"Known")), namedField("identityAssigned", ast.NewIdent("bool")), namedField("identityInsertOnly", ast.NewIdent("bool")))
					}
				}
			}
		}
	}
	previous, err := MutationPreviousSupport(value, config, entities)
	if err != nil {
		return nil, err
	}
	prefix := "_" + lowerInitial(l.factory)
	e := &frameEmitter{previousEmitter: previousEmitter{l: l, entities: entities}, layout: layout, previous: previous, name: prefix + "DatabaseSnapshot"}
	e.shape = l.availableAlias("xshape")
	l.pathsByAlias[e.shape] = "github.com/viant/x/shape"
	e.reflect = l.availableAlias("reflect")
	l.pathsByAlias[e.reflect] = "reflect"
	l.pathsByAlias[l.fmtAlias] = "fmt"
	file := &ast.File{Name: ast.NewIdent(config.Package)}
	fields := []*ast.Field{}
	for _, role := range previous.Roles {
		fields = append(fields, namedField(role.TypeName, &ast.StarExpr{X: ast.NewIdent(role.TypeName)}))
	}
	file.Decls = append(file.Decls, e.structure(e.name, fields...))
	capture, err := e.captureInput()
	if err != nil {
		return nil, err
	}
	file.Decls = append(file.Decls, capture)
	if entities.identity != nil {
		bind, err := e.bindProducers()
		if err != nil {
			return nil, err
		}
		file.Decls = append(file.Decls, bind)
	}
	build, err := e.build()
	if err != nil {
		return nil, err
	}
	file.Decls = append(file.Decls, build)
	verify, err := e.verify()
	if err != nil {
		return nil, err
	}
	file.Decls = append(file.Decls, verify)
	if entities.identity != nil {
		freeze, err := e.freezePendingIdentity()
		if err != nil {
			return nil, err
		}
		file.Decls = append(file.Decls, freeze)
	}
	file.Decls = append([]ast.Decl{(&entityEmitter{l: l}).importDeclaration(file)}, file.Decls...)
	return &MutationFrameAsset{File: file, Layout: layout, Previous: previous, TypeName: e.name, CaptureFunction: e.name + "Capture", BuildMethod: "Build", VerifyMethod: "Verify"}, nil
}

type frameEmitter struct {
	previousEmitter
	layout   *MutationFrameLayout
	previous *MutationPreviousAsset
	name     string
}

func (e *frameEmitter) previousRole(record *recordLowering) (MutationPreviousRole, error) {
	for _, role := range e.previous.Roles {
		if role.Identity == record.plan.Identity && strings.Join(role.Path, ".") == strings.Join(record.plan.InputPath, ".") {
			return role, nil
		}
	}
	return MutationPreviousRole{}, fmt.Errorf("database snapshot role %s is missing", record.plan.Identity)
}

func (e *frameEmitter) captureInput() (ast.Decl, error) {
	nilExpr := ast.NewIdent("nil")
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("input"), Op: token.EQL, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, e.errorExpr("input is required for database snapshot"))}}}, defineStmt("result", &ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: ast.NewIdent(e.name)}})}
	if len(e.previous.Roles) > 0 {
		body = append(body, &ast.IfStmt{Cond: e.isNil(ast.NewIdent("metadata")), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, e.errorExpr("read metadata is required before input initialization"))}}})
	}
	for _, record := range e.l.records {
		if record.plan.Auxiliary || record.plan.Current == nil {
			continue
		}
		role, err := e.previousRole(record)
		if err != nil {
			return nil, err
		}
		path := record.plan.Current.InputPath
		if len(path) == 0 || path[0] != "Input" {
			return nil, fmt.Errorf("current role %s has no canonical input path", record.plan.Identity)
		}
		block := []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("projection"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(ast.NewIdent("metadata"), "Projection"), stringExpr(strings.Join(path[1:], ".")))}}, e.guardError(nilExpr)}
		block = append(block, &ast.IfStmt{Cond: e.isNil(ast.NewIdent("projection")), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, e.errorExpr("actual read projection is required"))}}})
		block = append(block, e.accessor(parseExpr(e.l.config.InputType), strings.Join(path[1:], "."), "inputAccess")...)
		block = append(block, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("carrier"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(ast.NewIdent("inputAccess"), "Get"), ast.NewIdent("input"))}}, e.guardError(nilExpr), defineStmt("value", callExpr(selectExpr(ast.NewIdent("carrier"), "Interface"))))
		block = append(block, &ast.IfStmt{Cond: &ast.BinaryExpr{X: callExpr(selectExpr(ast.NewIdent("projection"), "DirectOutput")), Op: token.LAND, Y: &ast.BinaryExpr{X: callExpr(selectExpr(ast.NewIdent("projection"), "RootHolder")), Op: token.NEQ, Y: stringExpr("")}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, e.errorExpr("direct read projection cannot declare a root holder"))}}})
		unwrap := e.unwrapCarrier(record)
		block = append(block, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(ast.NewIdent("projection"), "DirectOutput"))}, Body: &ast.BlockStmt{List: unwrap}})
		collection := &ast.ParenExpr{X: &ast.CompositeLit{Type: &ast.IndexExpr{X: selectExpr(ast.NewIdent(e.shape), "Collection"), Index: parseExpr(record.current.base)}}}
		block = append(block, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("rows"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(collection, "Pointers"), ast.NewIdent("value"))}}, e.guardError(nilExpr), &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("snapshot"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(ast.NewIdent(role.CaptureFunction), ast.NewIdent("rows"), ast.NewIdent("projection"))}}, e.guardError(nilExpr), assignStmt(selectExpr(ast.NewIdent("result"), role.TypeName), ast.NewIdent("snapshot")))
		body = append(body, &ast.BlockStmt{List: block})
	}
	body = append(body, returnStmt(ast.NewIdent("result"), nilExpr))
	return e.function(e.name+"Capture", []*ast.Field{namedField("input", &ast.StarExpr{X: parseExpr(e.l.config.InputType)}), namedField("metadata", selectExpr(ast.NewIdent(e.l.handlerAlias), "ReadMetadata"))}, []*ast.Field{{Type: &ast.StarExpr{X: ast.NewIdent(e.name)}}, {Type: ast.NewIdent("error")}}, body), nil
}

func (e *frameEmitter) unwrapCarrier(record *recordLowering) []ast.Stmt {
	nilExpr := ast.NewIdent("nil")
	accessor := callExpr(selectExpr(callExpr(selectExpr(ast.NewIdent(e.shape), "Linked"), callExpr(selectExpr(ast.NewIdent("carrier"), "Type"))), "Accessor"), ast.NewIdent("holder"))
	emptyRows := callExpr(&ast.ArrayType{Elt: record.current.pointerExpr()}, nilExpr)
	collection := &ast.ParenExpr{X: &ast.CompositeLit{Type: &ast.IndexExpr{X: selectExpr(ast.NewIdent(e.shape), "Collection"), Index: parseExpr(record.current.base)}}}
	zeroHolder := callExpr(selectExpr(callExpr(selectExpr(ast.NewIdent(e.reflect), "Zero"), callExpr(selectExpr(ast.NewIdent("accessor"), "Type"))), "Interface"))
	return []ast.Stmt{
		defineStmt("holder", callExpr(selectExpr(ast.NewIdent("projection"), "RootHolder"))),
		&ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("holder"), Op: token.EQL, Y: stringExpr("")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, e.errorExpr("wrapped read projection requires a canonical root holder"))}}},
		&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("accessor"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{accessor}}, e.guardError(nilExpr),
		// Validate exact holder row authority even when the wrapper is absent.
		&ast.IfStmt{Init: &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("_"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(collection, "Pointers"), zeroHolder)}}, Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, ast.NewIdent("err"))}}},
		&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("root"), ast.NewIdent("present"), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(ast.NewIdent("accessor"), "GetOptional"), ast.NewIdent("value"))}}, e.guardError(nilExpr),
		&ast.IfStmt{Cond: ast.NewIdent("present"), Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(ast.NewIdent("value"), callExpr(selectExpr(ast.NewIdent("root"), "Interface")))}}, Else: &ast.BlockStmt{List: []ast.Stmt{assignStmt(ast.NewIdent("value"), emptyRows)}}},
	}
}

func (e *frameEmitter) build() (ast.Decl, error) {
	nilExpr := ast.NewIdent("nil")
	guard := &ast.BinaryExpr{X: &ast.BinaryExpr{X: ast.NewIdent("database"), Op: token.EQL, Y: nilExpr}, Op: token.LOR, Y: &ast.BinaryExpr{X: ast.NewIdent("sync"), Op: token.EQL, Y: nilExpr}}
	guard = &ast.BinaryExpr{X: guard, Op: token.LOR, Y: &ast.BinaryExpr{X: ast.NewIdent("input"), Op: token.EQL, Y: nilExpr}}
	body := []ast.Stmt{&ast.IfStmt{Cond: guard, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, e.errorExpr("captured database and successful presence synchronization are required"))}}}, defineStmt("frames", &ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: ast.NewIdent(e.layout.TypeName)}})}
	for _, record := range e.l.records {
		if record.plan.Auxiliary {
			continue
		}
		role, err := e.layout.role(record.plan)
		if err != nil {
			return nil, err
		}
		name := "add" + strconv.Itoa(record.order)
		typ := e.frameVisitorType(record, role)
		body = append(body, defineStmt("seen"+strconv.Itoa(record.order), callExpr(ast.NewIdent("make"), &ast.MapType{Key: record.value.pointerExpr(), Value: &ast.StarExpr{X: ast.NewIdent(role.FrameType)}})), &ast.DeclStmt{Decl: &ast.GenDecl{Tok: token.VAR, Specs: []ast.Spec{&ast.ValueSpec{Names: []*ast.Ident{ast.NewIdent(name)}, Type: typ}}}})
	}
	for _, record := range e.l.records {
		if record.plan.Auxiliary {
			continue
		}
		role, _ := e.layout.role(record.plan)
		statements, err := e.frame(record, role)
		if err != nil {
			return nil, err
		}
		typ := e.frameVisitorType(record, role)
		body = append(body, assignStmt(ast.NewIdent("add"+strconv.Itoa(record.order)), &ast.FuncLit{Type: typ, Body: &ast.BlockStmt{List: statements}}))
	}
	root := e.l.recordByPlan[e.l.plan.Root]
	path, err := selectPathExpr(ast.NewIdent("input"), e.l.plan.Root.InputPath[1:])
	if err != nil {
		return nil, err
	}
	entity := &entityEmitter{l: e.l}
	body = append(body, entity.syncPointerValues(path, root, "roots")...)
	body = append(body, &ast.RangeStmt{Key: ast.NewIdent("_"), Value: ast.NewIdent("root"), Tok: token.DEFINE, X: ast.NewIdent("roots"), Body: &ast.BlockStmt{List: []ast.Stmt{&ast.IfStmt{Init: defineStmt("err", callExpr(ast.NewIdent("add"+strconv.Itoa(root.order)), ast.NewIdent("root"), nilExpr, nilExpr, stringExpr(""))), Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, ast.NewIdent("err"))}}}}}}, returnStmt(ast.NewIdent("frames"), nilExpr))
	function := e.function("Build", []*ast.Field{namedField("input", &ast.StarExpr{X: parseExpr(e.l.config.InputType)}), namedField("sync", &ast.StarExpr{X: ast.NewIdent(e.entities.SyncContextType)})}, []*ast.Field{{Type: &ast.StarExpr{X: ast.NewIdent(e.layout.TypeName)}}, {Type: ast.NewIdent("error")}}, body)
	function.Recv = &ast.FieldList{List: []*ast.Field{namedField("database", &ast.StarExpr{X: ast.NewIdent(e.name)})}}
	return function, nil
}

func (e *frameEmitter) frameVisitorType(record *recordLowering, role MutationFrameRole) *ast.FuncType {
	return &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("current", record.value.pointerExpr()), namedField("parent", &ast.StarExpr{X: parseExpr(role.ParentType)}), namedField("selfParent", record.value.pointerExpr()), namedField("selfHolder", ast.NewIdent("string"))}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}}}
}
