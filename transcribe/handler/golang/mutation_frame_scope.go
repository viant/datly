package golang

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// matchedScope verifies the canonical loaded parent equality before an update
// reaches business hooks. A union of several authorized parents' child reads
// does not authorize moving a child from one parent to another.
func (e *frameEmitter) matchedScope(record *recordLowering) ([]ast.Stmt, error) {
	id := ast.NewIdent
	var body []ast.Stmt
	add := func(parent *recordLowering, links []plan.KeyLink, reference ast.Expr, condition ast.Expr) error {
		parentFrame := &ast.IndexExpr{X: id("seen" + strconv.Itoa(parent.order)), Index: reference}
		previous := selectExpr(selectExpr(id("frame"), "State"), "Previous")
		parentPrevious := selectExpr(selectExpr(id("scopeParent"), "State"), "Previous")
		block := []ast.Stmt{defineStmt("scopeParent", parentFrame), &ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.BinaryExpr{X: id("scopeParent"), Op: token.EQL, Y: id("nil")}, Op: token.LOR, Y: &ast.BinaryExpr{X: parentPrevious, Op: token.EQL, Y: id("nil")}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("matched child requires an authorized Previous parent"))}}}}
		entity := &entityEmitter{l: e.l}
		for index, link := range links {
			pair := []struct {
				record        *recordLowering
				field         plan.KeyPart
				value, fields ast.Expr
				name          string
			}{
				{parent, link.Parent, parentPrevious, selectExpr(selectExpr(id("scopeParent"), "State"), "PreviousFields"), "scopeParentValue"},
				{record, link.Child, previous, selectExpr(selectExpr(id("frame"), "State"), "PreviousFields"), "scopeChildValue"},
			}
			check := []ast.Stmt{}
			for _, item := range pair {
				check = append(check, &ast.IfStmt{Cond: &ast.BinaryExpr{X: e.isNil(item.fields), Op: token.LOR, Y: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(item.fields, "Has"), stringExpr(item.field.Field))}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("matched parent equality field was not loaded: " + item.field.Field))}}})
				accessor := item.name + "Access"
				path := strings.Join(entity.entityFieldPath(item.record, item.field.Field), ".")
				check = append(check, e.accessorAssignment(parseExpr(item.record.value.base), path, accessor), e.visitError(), &ast.AssignStmt{Lhs: []ast.Expr{id(item.name), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(id(accessor), "Get"), item.value)}}, e.visitError())
				if item.field.Type.Pointer || strings.HasPrefix(item.field.Type.Name, "*") {
					check = append(check, &ast.IfStmt{Cond: callExpr(selectExpr(id(item.name), "IsNil")), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("matched parent equality is null"))}}}, assignStmt(id(item.name), callExpr(selectExpr(id(item.name), "Elem"))))
				}
			}
			equal := callExpr(selectExpr(id(e.reflect), "DeepEqual"), callExpr(selectExpr(id("scopeParentValue"), "Interface")), callExpr(selectExpr(id("scopeChildValue"), "Interface")))
			check = append(check, &ast.IfStmt{Cond: &ast.UnaryExpr{Op: token.NOT, X: equal}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("resolved child identity is outside its Previous parent scope (link " + strconv.Itoa(index) + ")"))}}})
			block = append(block, &ast.BlockStmt{List: check})
		}
		present := &ast.BinaryExpr{X: previous, Op: token.NEQ, Y: id("nil")}

		if condition != nil {
			present = &ast.BinaryExpr{X: present, Op: token.LAND, Y: condition}
		}
		body = append(body, &ast.IfStmt{Cond: present, Body: &ast.BlockStmt{List: block}})
		return nil
	}
	if parent, relation := e.entities.identity.parent(e.l, record); parent != nil && !relation.AllowReparent {
		if err := add(parent, relation.Links, id("parent"), nil); err != nil {
			return nil, err
		}
	}
	for _, relation := range record.plan.SelfRelations {
		if relation.AllowReparent {
			continue
		}
		condition := &ast.BinaryExpr{X: &ast.BinaryExpr{X: id("selfParent"), Op: token.NEQ, Y: id("nil")}, Op: token.LAND, Y: &ast.BinaryExpr{X: id("selfHolder"), Op: token.EQL, Y: stringExpr(strings.Join(relation.FieldPath, "."))}}
		if err := add(record, relation.Links, id("selfParent"), condition); err != nil {
			return nil, err
		}
	}
	return body, nil
}
