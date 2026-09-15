package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

func (p *mutationIdentityPolicy) parent(l *lowerer, record *recordLowering) (*recordLowering, *plan.RelationPlan) {
	for _, parent := range l.records {
		for _, relation := range parent.plan.Relations {
			if relation.Child == record.plan {
				return parent, relation
			}
		}
	}
	return nil, nil
}

func (p *mutationIdentityPolicy) prepare(l *lowerer) error {
	parents := map[*plan.RecordPlan]bool{}
	for _, record := range l.records {
		holders := map[string]bool{}
		sets := [][]plan.KeyLink{}
		for _, relation := range record.plan.Relations {
			if parents[relation.Child] {
				return fmt.Errorf("mutation role %s has ambiguous canonical parent ownership", relation.Child.Identity)
			}
			parents[relation.Child] = true
			sets = append(sets, relation.Links)
		}
		for _, relation := range record.plan.SelfRelations {
			holder := strings.Join(relation.FieldPath, ".")
			if holders[holder] {
				return fmt.Errorf("mutation role %s repeats self holder %s", record.plan.Identity, holder)
			}
			holders[holder] = true
			sets = append(sets, relation.Links)
		}
		for _, links := range sets {
			fields := map[string]bool{}
			for _, link := range links {
				if fields[link.Child.Field] {
					return fmt.Errorf("mutation edge repeats destination field %s", link.Child.Field)
				}
				fields[link.Child.Field] = true
			}
		}
	}
	return nil
}

func (p *mutationIdentityPolicy) parentType(e *entityEmitter, record *recordLowering) ast.Expr {
	if parent, _ := p.parent(e.l, record); parent != nil {
		return e.statePointer(parent)
	}
	return &ast.StarExpr{X: selectExpr(ast.NewIdent(e.l.handlerAlias), "NoParent")}
}

func (p *mutationIdentityPolicy) captureFields(e *entityEmitter, record *recordLowering) []*ast.Field {
	return []*ast.Field{
		namedField("producerSeen", ast.NewIdent("bool")),
		namedField("producerParent", p.parentType(e, record)),
		namedField("producerSelf", e.statePointer(record)),
		namedField("producerHolder", ast.NewIdent("string")),
	}
}

func (p *mutationIdentityPolicy) capture(e *entityEmitter) (ast.Decl, error) {
	id := ast.NewIdent
	params := func(record *recordLowering) *ast.FuncType {
		return &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("state", e.statePointer(record)), namedField("parent", p.parentType(e, record)), namedField("self", e.statePointer(record)), namedField("holder", id("string"))}}, Results: &ast.FieldList{List: []*ast.Field{{Type: id("error")}}}}
	}
	name := func(record *recordLowering) string { return "visit" + strconv.Itoa(record.order) }
	body := []ast.Stmt{}
	for _, record := range e.l.records {
		if e.enabled(record) {
			body = append(body, &ast.DeclStmt{Decl: &ast.GenDecl{Tok: token.VAR, Specs: []ast.Spec{&ast.ValueSpec{Names: []*ast.Ident{id(name(record))}, Type: params(record)}}}})
		}
	}
	for _, record := range e.l.records {
		if !e.enabled(record) {
			continue
		}
		visit := []ast.Stmt{
			&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("state"), Op: token.EQL, Y: id("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("nil"))}}},
			&ast.IfStmt{Cond: selectExpr(id("state"), "producerSeen"), Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.invariantError("duplicate or ambiguous captured mutation edge"))}}},
			assignStmt(selectExpr(id("state"), "producerSeen"), id("true")),
			assignStmt(selectExpr(id("state"), "producerParent"), id("parent")),
			assignStmt(selectExpr(id("state"), "producerSelf"), id("self")),
			assignStmt(selectExpr(id("state"), "producerHolder"), id("holder")),
		}
		relations, err := e.entityRelations(record)
		if err != nil {
			return nil, err
		}
		for _, relation := range relations {
			parent, self, holder := ast.Expr(id("state")), ast.Expr(id("nil")), ast.Expr(stringExpr(""))
			if relation.child.plan == record.plan {
				parent, self, holder = id("parent"), id("state"), stringExpr(strings.Join(relation.holder, "."))
			}
			visit = append(visit, &ast.RangeStmt{Key: id("_"), Value: id("child"), Tok: token.DEFINE, X: selectExpr(id("state"), relation.storage), Body: &ast.BlockStmt{List: []ast.Stmt{errorGuard(callExpr(id(name(relation.child)), id("child"), parent, self, holder))}}})
		}
		visit = append(visit, returnStmt(id("nil")))
		body = append(body, assignStmt(id(name(record)), &ast.FuncLit{Type: params(record), Body: &ast.BlockStmt{List: visit}}))
	}
	root := e.l.recordByPlan[e.l.plan.Root]
	body = append(body, &ast.RangeStmt{Key: id("_"), Value: id("root"), Tok: token.DEFINE, X: selectExpr(id("snapshot"), "Roots"), Body: &ast.BlockStmt{List: []ast.Stmt{errorGuard(callExpr(id(name(root)), id("root"), id("nil"), id("nil"), stringExpr("")))}}}, returnStmt(id("nil")))
	return &ast.FuncDecl{Name: id("captureProducers"), Recv: &ast.FieldList{List: []*ast.Field{namedField("snapshot", &ast.StarExpr{X: id(e.snapshot)})}}, Type: &ast.FuncType{Params: &ast.FieldList{}, Results: &ast.FieldList{List: []*ast.Field{{Type: id("error")}}}}, Body: &ast.BlockStmt{List: body}}, nil
}

func (p *mutationIdentityPolicy) produced(e *entityEmitter, record *recordLowering) ast.Decl {
	id := ast.NewIdent
	type source struct{ condition, parent ast.Expr }
	sources := map[string][]source{}
	names := []string{}
	add := func(field string, condition, parent ast.Expr) {
		if _, found := sources[field]; !found {
			names = append(names, field)
		}
		sources[field] = append(sources[field], source{condition: condition, parent: parent})
	}
	if sequence := record.plan.Sequence; sequence != nil && p.produces(record, sequence.Field.Field) {
		add(sequence.Field.Field, id("true"), nil)
	}
	if _, relation := p.parent(e.l, record); relation != nil {
		for _, link := range relation.Links {
			condition := &ast.BinaryExpr{X: selectExpr(id("state"), "producerParent"), Op: token.NEQ, Y: id("nil")}
			add(link.Child.Field, condition, selectExpr(id("state"), "producerParent"))
		}
	}
	for _, relation := range record.plan.SelfRelations {
		for _, link := range relation.Links {
			condition := &ast.BinaryExpr{X: &ast.BinaryExpr{X: selectExpr(id("state"), "producerSelf"), Op: token.NEQ, Y: id("nil")}, Op: token.LAND, Y: &ast.BinaryExpr{X: selectExpr(id("state"), "producerHolder"), Op: token.EQL, Y: stringExpr(strings.Join(relation.FieldPath, "."))}}
			add(link.Child.Field, condition, selectExpr(id("state"), "producerSelf"))
		}
	}
	body := []ast.Stmt{&ast.IfStmt{Cond: &ast.BinaryExpr{X: &ast.BinaryExpr{X: id("state"), Op: token.EQL, Y: id("nil")}, Op: token.LOR, Y: &ast.UnaryExpr{Op: token.NOT, X: selectExpr(id("state"), "producerSeen")}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("false"), e.invariantError("captured producer context is required"))}}}, defineStmt("count", &ast.BasicLit{Kind: token.INT, Value: "0"})}
	cases := []ast.Stmt{}
	for _, field := range names {
		var checks []ast.Stmt
		for _, source := range sources[field] {
			proof := []ast.Stmt{&ast.IncDecStmt{X: id("count"), Tok: token.INC}}
			if parent := source.parent; parent != nil {
				proof = []ast.Stmt{
					&ast.AssignStmt{Lhs: []ast.Expr{id("writable"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(parent, "ProducerWrite"))}},
					&ast.IfStmt{Cond: &ast.BinaryExpr{X: id("err"), Op: token.NEQ, Y: id("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("false"), id("err"))}}},
					&ast.IfStmt{Cond: id("writable"), Body: &ast.BlockStmt{List: proof}},
				}
			}
			checks = append(checks, &ast.IfStmt{Cond: source.condition, Body: &ast.BlockStmt{List: proof}})
		}
		cases = append(cases, &ast.CaseClause{List: []ast.Expr{stringExpr(field)}, Body: checks})
	}
	body = append(body, &ast.SwitchStmt{Tag: id("field"), Body: &ast.BlockStmt{List: cases}}, &ast.IfStmt{Cond: &ast.BinaryExpr{X: id("count"), Op: token.GTR, Y: &ast.BasicLit{Kind: token.INT, Value: "1"}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(id("false"), e.invariantError("mutation field has competing captured producers"))}}}, returnStmt(&ast.BinaryExpr{X: id("count"), Op: token.EQL, Y: &ast.BasicLit{Kind: token.INT, Value: "1"}}, id("nil")))
	return &ast.FuncDecl{Name: id("Produced"), Recv: &ast.FieldList{List: []*ast.Field{namedField("state", e.statePointer(record))}}, Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("field", id("string"))}}, Results: &ast.FieldList{List: []*ast.Field{{Type: id("bool")}, {Type: id("error")}}}}, Body: &ast.BlockStmt{List: body}}
}
