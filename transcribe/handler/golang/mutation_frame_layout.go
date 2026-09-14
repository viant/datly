package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// MutationFrameRole references one existing semantic role using target names.
// It does not define another relation or mutation policy tree.
type MutationFrameRole struct {
	Identity                                 string
	Path                                     plan.FieldPath
	FrameType, Field, EntityType, ParentType string
}

// MutationFrameLayout is the sole declaration owner for typed entity frames,
// including roles without hooks that still need invariants or database state.
type MutationFrameLayout struct {
	File                       *ast.File
	TypeName                   string
	Roles                      []MutationFrameRole
	OrderField, OrderEntryType string
}

func MutationFrames(value *plan.Plan, config Config) (*MutationFrameLayout, error) {
	l := &lowerer{plan: value, config: config}
	if err := l.prepare(); err != nil {
		return nil, err
	}
	return l.mutationFrames()
}

func (l *lowerer) mutationFrames() (*MutationFrameLayout, error) {
	prefix := "_" + lowerInitial(l.factory)
	result := &MutationFrameLayout{TypeName: prefix + "MutationFrames"}
	result.OrderField, result.OrderEntryType = "Order", prefix+"MutationVisit"
	if err := l.reserveDeclaration(result.OrderEntryType); err != nil {
		return nil, err
	}
	if err := l.reserveDeclaration(result.TypeName); err != nil {
		return nil, err
	}
	file := &ast.File{Name: ast.NewIdent(l.config.Package)}
	fields := []*ast.Field{}
	file.Decls = append(file.Decls, &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: ast.NewIdent(result.OrderEntryType), Type: &ast.StructType{Fields: &ast.FieldList{List: []*ast.Field{namedField("Role", ast.NewIdent("int")), namedField("Index", ast.NewIdent("int"))}}}}}})
	var collect func(*plan.RecordPlan, *recordLowering) error
	collect = func(record *plan.RecordPlan, parent *recordLowering) error {
		if record == nil {
			return fmt.Errorf("mutation frame role is required")
		}
		if record.Auxiliary {
			return nil
		}
		actual := l.recordByPlan[record]
		if actual == nil {
			return fmt.Errorf("mutation frame role %s was not lowered", record.Identity)
		}
		parentType := selectExpr(ast.NewIdent(l.handlerAlias), "NoParent")
		var parentExpression ast.Expr = parentType
		parentName := l.handlerAlias + ".NoParent"
		if parent != nil {
			parentExpression = parseExpr(parent.value.base)
			parentName = parent.value.base
		}
		frame := MutationFrameRole{Identity: record.Identity, Path: append(plan.FieldPath(nil), record.InputPath...), FrameType: prefix + "MutationHooksFrame" + strconv.Itoa(actual.order), Field: "Role" + strconv.Itoa(actual.order), EntityType: actual.value.base, ParentType: parentName}
		if err := l.reserveDeclaration(frame.FrameType); err != nil {
			return err
		}
		result.Roles = append(result.Roles, frame)
		state := &ast.IndexListExpr{X: selectExpr(ast.NewIdent(l.handlerAlias), "EntityState"), Indices: []ast.Expr{parseExpr(actual.value.base), parentExpression}}
		file.Decls = append(file.Decls, &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: ast.NewIdent(frame.FrameType), Type: &ast.StructType{Fields: &ast.FieldList{List: []*ast.Field{namedField("Entity", actual.value.pointerExpr()), namedField("State", state), namedField("SelfHolder", ast.NewIdent("string"))}}}}}})
		fields = append(fields, namedField(frame.Field, &ast.ArrayType{Elt: &ast.StarExpr{X: ast.NewIdent(frame.FrameType)}}))
		for _, relation := range record.Relations {
			if relation != nil {
				if err := collect(relation.Child, actual); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := collect(l.plan.Root, nil); err != nil {
		return nil, err
	}
	fields = append(fields, namedField(result.OrderField, &ast.ArrayType{Elt: ast.NewIdent(result.OrderEntryType)}))
	file.Decls = append(file.Decls, &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: ast.NewIdent(result.TypeName), Type: &ast.StructType{Fields: &ast.FieldList{List: fields}}}}})
	file.Decls = append([]ast.Decl{(&entityEmitter{l: l}).importDeclaration(file)}, file.Decls...)
	result.File = file
	return result, nil
}

func (l *MutationFrameLayout) role(record *plan.RecordPlan) (MutationFrameRole, error) {
	if l == nil {
		return MutationFrameRole{}, fmt.Errorf("mutation frame layout is required")
	}
	for _, role := range l.Roles {
		if role.Identity == record.Identity && strings.Join(role.Path, ".") == strings.Join(record.InputPath, ".") {
			return role, nil
		}
	}
	return MutationFrameRole{}, fmt.Errorf("mutation frame layout has no role %s at %v", record.Identity, record.InputPath)
}
