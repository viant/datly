package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

func (e *entityEmitter) invariantDeclarations(record *recordLowering) ([]ast.Decl, error) {
	var result []ast.Decl
	for _, group := range record.plan.Entity.Invariants {
		if len(group.Fields) == 0 {
			return nil, fmt.Errorf("invariant %s has no fields", group.Name)
		}
		for _, name := range group.Fields {
			found := false
			for _, field := range record.plan.Entity.Fields {
				if field.Name == name {
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("invariant %s field %s is absent from entity", group.Name, name)
			}
		}
		helper := e.prefix + "Backfill" + strconv.Itoa(record.order) + group.Name
		declaration, err := e.invariantBackfillHelper(record, group, helper)
		if err != nil {
			return nil, err
		}
		result = append(result, declaration)
		e.asset.Invariants = append(e.asset.Invariants, EntityInvariant{Identity: record.plan.Identity, Path: append(plan.FieldPath(nil), record.plan.InputPath...), Group: group.Name, BackfillFunction: helper})
		if !record.plan.Entity.Owned {
			continue
		}
		signature := record.value.base + ".Backfill" + group.Name + "IfNeeded"
		definition := strings.Join(group.Fields, ",")
		if prior, ok := e.setters[signature]; ok {
			if prior != definition {
				return nil, fmt.Errorf("entity invariant %s has conflicting fields", signature)
			}
			continue
		}
		e.setters[signature] = definition
		result = append(result, e.hasInvariantDeclaration(record, group), e.backfillInvariantDeclaration(record, group, helper))
	}
	return result, nil
}

func (e *entityEmitter) hasInvariantDeclaration(record *recordLowering, group plan.InvariantGroup) ast.Decl {
	entity := ast.NewIdent("entity")
	marker := selectExpr(entity, record.plan.Entity.MarkerField)
	var present ast.Expr = ast.NewIdent("false")
	for _, field := range group.Fields {
		present = &ast.BinaryExpr{X: present, Op: token.LOR, Y: callExpr(ast.NewIdent("bool"), selectExpr(marker, field))}
	}
	guard := &ast.BinaryExpr{X: entity, Op: token.NEQ, Y: ast.NewIdent("nil")}
	var condition ast.Expr = guard
	if record.plan.Entity.MarkerPointer {
		condition = &ast.BinaryExpr{X: condition, Op: token.LAND, Y: &ast.BinaryExpr{X: marker, Op: token.NEQ, Y: ast.NewIdent("nil")}}
	}
	condition = &ast.BinaryExpr{X: condition, Op: token.LAND, Y: &ast.ParenExpr{X: present}}
	name := "Has" + group.Name + "Changes"
	e.asset.Methods = append(e.asset.Methods, EntityMethod{Receiver: record.value.base, Name: name, Getter: true, ValueType: "bool"})
	return &ast.FuncDecl{Name: ast.NewIdent(name), Recv: &ast.FieldList{List: []*ast.Field{namedField("entity", record.value.pointerExpr())}}, Type: &ast.FuncType{Params: &ast.FieldList{}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("bool")}}}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(condition)}}}
}

func (e *entityEmitter) backfillInvariantDeclaration(record *recordLowering, group plan.InvariantGroup, helper string) ast.Decl {
	name := "Backfill" + group.Name + "IfNeeded"
	e.asset.Methods = append(e.asset.Methods, EntityMethod{Receiver: record.value.base, Name: name, Signature: fmt.Sprintf("func(*%s, %s.FieldSet) error", record.value.base, e.l.handlerAlias)})
	return &ast.FuncDecl{
		Doc:  &ast.CommentGroup{List: []*ast.Comment{{Text: "// " + name + " hydrates omitted group values without marking presence."}, {Text: "// A nil previousFields denotes a full typed contract; generated programs pass actual field evidence."}}},
		Name: ast.NewIdent(name), Recv: &ast.FieldList{List: []*ast.Field{namedField("entity", record.value.pointerExpr())}},
		Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("previous", record.value.pointerExpr()), namedField("previousFields", selectExpr(ast.NewIdent(e.l.handlerAlias), "FieldSet"))}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("error")}}}},
		Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(callExpr(ast.NewIdent(helper), ast.NewIdent("entity"), ast.NewIdent("previous"), ast.NewIdent("previousFields")))}},
	}
}

func (e *entityEmitter) invariantError(message string) ast.Expr {
	if e.fmtAlias == "" {
		e.fmtAlias = e.l.importsByPath["fmt"]
		if e.fmtAlias == "" {
			e.fmtAlias = e.l.availableAlias("fmt")
			e.l.pathsByAlias[e.fmtAlias] = "fmt"
		}
	}
	return callExpr(selectExpr(ast.NewIdent(e.fmtAlias), "Errorf"), stringExpr(message))
}
