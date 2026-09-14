package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// MutationPreviousRole describes a generated typed database snapshot product.
// Its identity index is deliberately separate from processing associations.
type MutationPreviousRole struct {
	Identity                             string
	Path                                 plan.FieldPath
	TypeName, CaptureFunction, EntryType string
}

// MutationPreviousAsset freezes read rows and their actual projection evidence.
// It is a phase product, not an implementation of the generic Program contract.
type MutationPreviousAsset struct {
	File  *ast.File
	Roles []MutationPreviousRole
}

func MutationPreviousSupport(value *plan.Plan, config Config, entities *EntityAsset) (*MutationPreviousAsset, error) {
	l := &lowerer{plan: value, config: config}
	if err := l.prepare(); err != nil {
		return nil, err
	}
	if entities == nil {
		return nil, fmt.Errorf("database previous support requires entity snapshot metadata")
	}
	e := &previousEmitter{l: l, entities: entities, asset: &MutationPreviousAsset{}}
	l.pathsByAlias[l.fmtAlias] = "fmt"
	e.shape = l.availableAlias("xshape")
	l.pathsByAlias[e.shape] = "github.com/viant/x/shape"
	e.reflect = l.availableAlias("reflect")
	l.pathsByAlias[e.reflect] = "reflect"
	file := &ast.File{Name: ast.NewIdent(config.Package)}
	for _, record := range l.records {
		if record.plan.Auxiliary || record.plan.Current == nil {
			continue
		}
		association, err := e.association(record)
		if err != nil {
			return nil, err
		}
		role := MutationPreviousRole{Identity: record.plan.Identity, Path: append(plan.FieldPath(nil), record.plan.InputPath...), TypeName: "_" + lowerInitial(l.factory) + "Previous" + strconv.Itoa(record.order)}
		role.CaptureFunction = role.TypeName + "Capture"
		role.EntryType = role.TypeName + "Row"
		for _, name := range []string{role.TypeName, role.CaptureFunction, role.EntryType, role.TypeName + "Fields"} {
			if err = l.reserveDeclaration(name); err != nil {
				return nil, err
			}
		}
		declarations, err := e.role(record, association, role)
		if err != nil {
			return nil, err
		}
		file.Decls = append(file.Decls, declarations...)
		e.asset.Roles = append(e.asset.Roles, role)
	}
	file.Decls = append([]ast.Decl{(&entityEmitter{l: l}).importDeclaration(file)}, file.Decls...)
	e.asset.File = file
	return e.asset, nil
}

type previousEmitter struct {
	l              *lowerer
	entities       *EntityAsset
	asset          *MutationPreviousAsset
	shape, reflect string
}

func (e *previousEmitter) association(record *recordLowering) (EntityAssociation, error) {
	for _, item := range e.entities.Associations {
		if item.Identity == record.plan.Identity && strings.Join(item.Path, ".") == strings.Join(record.plan.InputPath, ".") {
			if item.KeyType == "" || item.KeyAdapterType == "" || item.StateType == "" {
				return EntityAssociation{}, fmt.Errorf("entity role %s lacks original identity metadata", record.plan.Identity)
			}
			return item, nil
		}
	}
	return EntityAssociation{}, fmt.Errorf("entity role %s has no snapshot association", record.plan.Identity)
}

func (e *previousEmitter) errorExpr(message string) ast.Expr {
	return callExpr(selectExpr(ast.NewIdent(e.l.fmtAlias), "Errorf"), stringExpr(message))
}
func (e *previousEmitter) isNil(value ast.Expr) ast.Expr {
	return callExpr(selectExpr(&ast.ParenExpr{X: &ast.CompositeLit{Type: selectExpr(ast.NewIdent(e.shape), "Runtime")}}, "IsNil"), value)
}
func (e *previousEmitter) guardError(result ast.Expr) ast.Stmt {
	return &ast.IfStmt{Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(result, ast.NewIdent("err"))}}}
}
func (e *previousEmitter) fieldSet() ast.Expr {
	return selectExpr(ast.NewIdent(e.l.handlerAlias), "FieldSet")
}
func (e *previousEmitter) structure(name string, fields ...*ast.Field) ast.Decl {
	return &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: ast.NewIdent(name), Type: &ast.StructType{Fields: &ast.FieldList{List: fields}}}}}
}
func (e *previousEmitter) function(name string, params, results []*ast.Field, body []ast.Stmt) *ast.FuncDecl {
	return &ast.FuncDecl{Name: ast.NewIdent(name), Type: &ast.FuncType{Params: &ast.FieldList{List: params}, Results: &ast.FieldList{List: results}}, Body: &ast.BlockStmt{List: body}}
}

func (e *previousEmitter) role(record *recordLowering, association EntityAssociation, role MutationPreviousRole) ([]ast.Decl, error) {
	if record.current.base == "" {
		return nil, fmt.Errorf("database previous role %s has no row type", record.plan.Identity)
	}
	if len(record.plan.Current.Keys) == 0 {
		return nil, fmt.Errorf("database previous role %s has no identity", record.plan.Identity)
	}
	keys := record.plan.IdentityKeys()
	if len(keys) != len(record.plan.Current.Keys) {
		return nil, fmt.Errorf("database previous role %s has inconsistent key arity", record.plan.Identity)
	}
	projected := map[string]plan.CurrentField{}
	for _, field := range record.plan.Current.Fields {
		if _, exists := projected[field.Entity.Field]; exists {
			return nil, fmt.Errorf("database previous role %s projects entity field %s more than once", record.plan.Identity, field.Entity.Field)
		}
		projected[field.Entity.Field] = field
	}
	for index, key := range keys {
		field, ok := projected[key.Field]
		if !ok || field.Current.Field != record.plan.Current.Keys[index].Field {
			return nil, fmt.Errorf("database previous role %s requires the declared projection of identity field %s", record.plan.Identity, key.Field)
		}
	}
	rowPtr := &ast.StarExpr{X: ast.NewIdent(role.EntryType)}
	indexType := &ast.MapType{Key: ast.NewIdent(association.KeyType), Value: rowPtr}
	declarations := []ast.Decl{e.structure(role.EntryType, namedField("value", record.value.pointerExpr()), namedField("fields", e.fieldSet())), e.structure(role.TypeName, namedField("byKey", indexType)), e.structure(role.TypeName+"Fields", namedField("source", e.fieldSet()))}
	fieldsReceiver := &ast.FieldList{List: []*ast.Field{namedField("fields", ast.NewIdent(role.TypeName+"Fields"))}}
	clauses := []ast.Stmt{}
	for _, field := range record.plan.Current.Fields {
		clauses = append(clauses, &ast.CaseClause{List: []ast.Expr{stringExpr(field.Entity.Field)}, Body: []ast.Stmt{returnStmt(callExpr(selectExpr(selectExpr(ast.NewIdent("fields"), "source"), "Has"), stringExpr(field.Current.Field)))}})
	}
	clauses = append(clauses, &ast.CaseClause{Body: []ast.Stmt{returnStmt(ast.NewIdent("false"))}})
	has := e.function("Has", []*ast.Field{namedField("name", ast.NewIdent("string"))}, []*ast.Field{{Type: ast.NewIdent("bool")}}, []ast.Stmt{&ast.SwitchStmt{Tag: ast.NewIdent("name"), Body: &ast.BlockStmt{List: clauses}}})
	has.Recv = fieldsReceiver
	declarations = append(declarations, has)
	capture, err := e.capture(record, association, role, keys, indexType)
	if err != nil {
		return nil, err
	}
	declarations = append(declarations, capture)
	return declarations, nil
}

func (e *previousEmitter) accessor(typ ast.Expr, path, name string) []ast.Stmt {
	return []ast.Stmt{e.accessorAssignment(typ, path, name), e.guardError(ast.NewIdent("nil"))}
}

func (e *previousEmitter) accessorAssignment(typ ast.Expr, path, name string) ast.Stmt {
	reflectType := callExpr(selectExpr(callExpr(selectExpr(ast.NewIdent(e.reflect), "TypeOf"), callExpr(&ast.StarExpr{X: typ}, ast.NewIdent("nil"))), "Elem"))
	return &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent(name), ast.NewIdent("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(callExpr(selectExpr(ast.NewIdent(e.shape), "Linked"), reflectType), "Accessor"), stringExpr(path))}}
}

func (e *previousEmitter) capture(record *recordLowering, association EntityAssociation, role MutationPreviousRole, keys []plan.KeyPart, indexType ast.Expr) (ast.Decl, error) {
	nilExpr := ast.NewIdent("nil")
	resultType := &ast.StarExpr{X: ast.NewIdent(role.TypeName)}
	body := []ast.Stmt{&ast.IfStmt{Cond: e.isNil(ast.NewIdent("projection")), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, e.errorExpr("actual read projection is required for "+record.plan.Current.ViewIdentity))}}}, defineStmt("result", &ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: ast.NewIdent(role.TypeName), Elts: []ast.Expr{&ast.KeyValueExpr{Key: ast.NewIdent("byKey"), Value: callExpr(ast.NewIdent("make"), indexType)}}}})}
	for i, key := range record.plan.Current.Keys {
		body = append(body, e.accessor(parseExpr(record.current.base), key.Field, "keyAccess"+strconv.Itoa(i))...)
	}
	for i, field := range record.plan.Current.Fields {
		body = append(body, e.accessor(parseExpr(record.current.base), field.Current.Field, "sourceAccess"+strconv.Itoa(i))...)
		body = append(body, e.accessor(parseExpr(record.value.base), field.Entity.Field, "targetAccess"+strconv.Itoa(i))...)
	}
	for i, holder := range record.plan.Current.Self {
		body = append(body, e.accessor(parseExpr(record.current.base), holder.Field, "selfAccess"+strconv.Itoa(i))...)
	}
	body = append(body, defineStmt("options", &ast.CompositeLit{Type: selectExpr(ast.NewIdent(e.shape), "CloneOptions")}))
	selectArgs := []ast.Expr{callExpr(selectExpr(callExpr(selectExpr(ast.NewIdent(e.reflect), "TypeOf"), callExpr(record.value.pointerExpr(), nilExpr)), "Elem"))}
	for _, field := range record.plan.Current.Fields {
		selectArgs = append(selectArgs, stringExpr(field.Entity.Field))
	}
	body = append(body, &ast.IfStmt{Init: defineStmt("err", callExpr(selectExpr(ast.NewIdent("options"), "Select"), selectArgs...)), Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, ast.NewIdent("err"))}}})
	rowType := record.current.pointerExpr()
	visitedType := &ast.MapType{Key: rowType, Value: e.fieldSet()}
	body = append(body, defineStmt("visited", callExpr(ast.NewIdent("make"), visitedType)))
	body = append(body, defineStmt("active", callExpr(ast.NewIdent("make"), &ast.MapType{Key: rowType, Value: ast.NewIdent("bool")})))
	stepType := selectExpr(ast.NewIdent(e.l.handlerAlias), "ReadStep")
	visitType := &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("row", rowType), namedField("ordinal", ast.NewIdent("int")), namedField("steps", &ast.ArrayType{Elt: stepType})}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}}}
	body = append(body, &ast.DeclStmt{Decl: &ast.GenDecl{Tok: token.VAR, Specs: []ast.Spec{&ast.ValueSpec{Names: []*ast.Ident{ast.NewIdent("visit")}, Type: visitType}}}})
	visit, err := e.visit(record, association, role, keys)
	if err != nil {
		return nil, err
	}
	body = append(body, assignStmt(ast.NewIdent("visit"), &ast.FuncLit{Type: visitType, Body: &ast.BlockStmt{List: visit}}))
	body = append(body, &ast.RangeStmt{Key: ast.NewIdent("ordinal"), Value: ast.NewIdent("row"), Tok: token.DEFINE, X: ast.NewIdent("rows"), Body: &ast.BlockStmt{List: []ast.Stmt{&ast.IfStmt{Init: defineStmt("err", callExpr(ast.NewIdent("visit"), ast.NewIdent("row"), ast.NewIdent("ordinal"), nilExpr)), Cond: &ast.BinaryExpr{X: ast.NewIdent("err"), Op: token.NEQ, Y: nilExpr}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(nilExpr, ast.NewIdent("err"))}}}}}}, returnStmt(ast.NewIdent("result"), nilExpr))
	return e.function(role.CaptureFunction, []*ast.Field{namedField("rows", &ast.ArrayType{Elt: rowType}), namedField("projection", selectExpr(ast.NewIdent(e.l.handlerAlias), "ReadProjection"))}, []*ast.Field{{Type: resultType}, {Type: ast.NewIdent("error")}}, body), nil
}
