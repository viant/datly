package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
	xshape "github.com/viant/x/shape"
)

// markerValue reads a canonical field through the shared shape accessor. Its
// result has the declared Go type; token equality never stringifies values.
func (e *actionEmitter) markerValue(role actionRole, field plan.EntityField, source ast.Expr, name string) ([]ast.Stmt, ast.Expr, error) {
	path, err := e.fieldPath(role, field.Name)
	if err != nil {
		return nil, nil, err
	}
	typ, err := e.l.typeReference(field.Type)
	if err != nil {
		return nil, nil, err
	}
	id := ast.NewIdent
	body := e.accessor(role.record, path, name+"Access")
	body = append(body, &ast.AssignStmt{Lhs: []ast.Expr{id(name + "Value"), id("err")}, Tok: token.DEFINE, Rhs: []ast.Expr{callExpr(selectExpr(id(name+"Access"), "Get"), source)}}, errorGuard(id("err")), defineStmt(name, &ast.TypeAssertExpr{X: callExpr(selectExpr(id(name+"Value"), "Interface")), Type: typ}))
	return body, id(name), nil
}

func (e *actionEmitter) deleteSelection(role actionRole, action ast.Expr) ([]ast.Stmt, error) {
	id := ast.NewIdent
	for _, field := range role.record.plan.Entity.Fields {
		if !field.DeleteMarker {
			continue
		}
		body, value, err := e.markerValue(role, field, selectExpr(id("original"), "original"), "deleteRequested")
		if err != nil {
			return nil, err
		}
		requested := ast.Expr(value)
		if field.Type.Pointer || strings.HasPrefix(field.Type.Name, "*") {
			requested = &ast.BinaryExpr{X: &ast.BinaryExpr{X: value, Op: token.NEQ, Y: id("nil")}, Op: token.LAND, Y: &ast.StarExpr{X: value}}
		}
		requested = &ast.BinaryExpr{X: callExpr(selectExpr(id("original"), "Has"), stringExpr(field.Name)), Op: token.LAND, Y: requested}
		invalid := ast.Expr(&ast.BinaryExpr{X: selectExpr(selectExpr(id("frame"), "State"), "Previous"), Op: token.EQL, Y: id("nil")})
		for _, key := range role.record.plan.IdentityKeys() {
			invalid = &ast.BinaryExpr{X: invalid, Op: token.LOR, Y: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(id("original"), "Has"), stringExpr(key.Field))}}
			if e.entities.identity != nil {
				changed := &ast.BinaryExpr{X: selectExpr(selectExpr(id("frame"), "identityKey"), key.Field), Op: token.NEQ, Y: selectExpr(id("original"), "key"+key.Field)}
				invalid = &ast.BinaryExpr{X: invalid, Op: token.LOR, Y: changed}
			}
		}
		body = append(body, &ast.IfStmt{Cond: requested, Body: &ast.BlockStmt{List: []ast.Stmt{&ast.IfStmt{Cond: invalid, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(e.errorExpr("delete_marker requires a complete supplied identity and an authorized matched Previous row"))}}}, assignStmt(action, selectExpr(id(e.l.handlerAlias), "WriteDelete"))}}})
		return body, nil
	}
	return nil, nil
}

func (e *validationEmitter) concurrency() ([]ast.Stmt, error) {
	a, id := e.action, ast.NewIdent
	var body []ast.Stmt
	for _, role := range a.roles {
		for _, field := range role.record.plan.Entity.Fields {
			if !field.ConcurrencyToken {
				continue
			}
			conflict := func(reason string) ast.Expr {
				return &ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: selectExpr(id(a.l.handlerAlias), "Conflict"), Elts: []ast.Expr{&ast.KeyValueExpr{Key: id("Entity"), Value: stringExpr(role.record.plan.Identity)}, &ast.KeyValueExpr{Key: id("Field"), Value: stringExpr(field.Name)}, &ast.KeyValueExpr{Key: id("Reason"), Value: stringExpr(reason)}}}}
			}
			fail := func(condition ast.Expr, reason string) ast.Stmt {
				return &ast.IfStmt{Cond: condition, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(conflict(reason))}}}
			}
			loop := a.original(role)
			loop = append(loop, a.decisionIdentity(role, "_", "supplied")...)
			loop = append(loop, assignStmt(id("_"), id("supplied")))
			selection, action, err := a.actionSelection(role)
			if err != nil {
				return nil, err
			}
			loop = append(loop, selection...)
			previous := selectExpr(selectExpr(id("frame"), "State"), "Previous")
			evidence := selectExpr(selectExpr(id("frame"), "State"), "PreviousFields")
			checks := []ast.Stmt{fail(&ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(id("original"), "Has"), stringExpr(field.Name))}, "expected token is missing"), fail(&ast.BinaryExpr{X: &ast.BinaryExpr{X: previous, Op: token.EQL, Y: id("nil")}, Op: token.LOR, Y: &ast.BinaryExpr{X: a.isNil(evidence), Op: token.LOR, Y: &ast.UnaryExpr{Op: token.NOT, X: callExpr(selectExpr(evidence, "Has"), stringExpr(field.Name))}}}, "Previous token was not loaded")}
			for _, item := range []struct {
				name   string
				source ast.Expr
			}{{"expectedToken", selectExpr(id("original"), "original")}, {"previousToken", previous}} {
				statements, _, err := a.markerValue(role, field, item.source, item.name)
				if err != nil {
					return nil, err
				}
				checks = append(checks, statements...)
			}
			var expected, prior ast.Expr = id("expectedToken"), id("previousToken")
			if field.Type.Pointer || strings.HasPrefix(field.Type.Name, "*") {
				checks = append(checks, fail(&ast.BinaryExpr{X: &ast.BinaryExpr{X: expected, Op: token.EQL, Y: id("nil")}, Op: token.LOR, Y: &ast.BinaryExpr{X: prior, Op: token.EQL, Y: id("nil")}}, "token is null"))
				expected = &ast.StarExpr{X: expected}
				prior = &ast.StarExpr{X: prior}
			}
			var equal ast.Expr = &ast.BinaryExpr{X: expected, Op: token.EQL, Y: prior}
			name, err := a.markerType(field)
			if err != nil {
				return nil, err
			}
			if name == "time.Time" {
				equal = callExpr(selectExpr(&ast.ParenExpr{X: expected}, "Equal"), prior)
			}
			checks = append(checks, fail(&ast.UnaryExpr{Op: token.NOT, X: &ast.ParenExpr{X: equal}}, "expected token differs from Previous"))
			loop = append(loop, &ast.IfStmt{Cond: &ast.BinaryExpr{X: action, Op: token.EQL, Y: selectExpr(id(a.l.handlerAlias), "WriteUpdate")}, Body: &ast.BlockStmt{List: checks}})
			body = append(body, a.frameLoop(role, loop))
		}
	}
	return body, nil
}

func (e *actionEmitter) validateMarkers(role *plan.RecordPlan) error {
	markers, tokens := 0, 0
	for _, field := range role.Entity.Fields {
		if !field.DeleteMarker && !field.ConcurrencyToken {
			continue
		}
		if field.Identity || field.Relation || field.DeleteMarker && field.Invariant != "" {
			return fmt.Errorf("mutation marker %s must be a non-identity scalar; delete markers cannot join invariants", field.Name)
		}
		if role.Current == nil || e.l.plan.Operation == plan.OperationPost {
			return fmt.Errorf("mutation marker %s requires a generated update policy with authorized Previous", field.Name)
		}
		name, err := e.markerType(field)
		if err != nil {
			return err
		}
		if field.DeleteMarker {
			markers++
			if role.Write.DeleteMarker.Field != field.Name || !containsAction(role.Write.Allowed, plan.ActionDelete) {
				return fmt.Errorf("delete_marker %s requires its canonical allowed delete policy", field.Name)
			}
			if name != "bool" || field.ConcurrencyToken || field.Writable {
				return fmt.Errorf("delete_marker %s must be a logical bool field", field.Name)
			}
		}
		if field.ConcurrencyToken {
			tokens++
			if role.Write.ConcurrencyToken.Field != field.Name {
				return fmt.Errorf("concurrency_token %s requires its canonical write policy", field.Name)
			}
			switch name {
			case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "time.Time":
			default:
				return fmt.Errorf("concurrency_token %s must have a canonical numeric or time.Time type", field.Name)
			}
		}
	}
	if markers > 1 || tokens > 1 {
		return fmt.Errorf("each mutation role permits one delete_marker and one concurrency_token")
	}
	return nil
}

// deletionGraph rejects writes below explicitly deleted parents before any
// framework/business validation or allocation. Missing children create no work.
func (e *validationEmitter) deletionGraph() ([]ast.Stmt, error) {
	a, id := e.action, ast.NewIdent
	enabled := false
	for _, role := range a.roles {
		for _, field := range role.record.plan.Entity.Fields {
			enabled = enabled || field.DeleteMarker
		}
	}
	if !enabled {
		return nil, nil
	}
	var body []ast.Stmt
	for _, role := range a.roles {
		deleted := id(role.field + "Deleted")
		body = append(body, defineStmt(deleted.Name, callExpr(id("make"), &ast.MapType{Key: role.record.value.pointerExpr(), Value: id("bool")})))
		loop := a.original(role)
		loop = append(loop, a.decisionIdentity(role, "_", "supplied")...)
		loop = append(loop, assignStmt(id("_"), id("supplied")))
		selection, action, err := a.actionSelection(role)
		if err != nil {
			return nil, err
		}
		loop = append(loop, selection...)
		loop = append(loop, assignStmt(&ast.IndexExpr{X: deleted, Index: selectExpr(id("frame"), "Entity")}, &ast.BinaryExpr{X: action, Op: token.EQL, Y: selectExpr(id(a.l.handlerAlias), "WriteDelete")}))
		body = append(body, a.frameLoop(role, loop))
	}
	for _, role := range a.roles {
		var loop []ast.Stmt
		for _, relation := range a.relations(role, id("frame")) {
			parent := &ast.IndexExpr{X: id(relation.parent.field + "Deleted"), Index: relation.reference}
			child := &ast.IndexExpr{X: id(role.field + "Deleted"), Index: selectExpr(id("frame"), "Entity")}
			invalid := &ast.BinaryExpr{X: relation.condition, Op: token.LAND, Y: &ast.BinaryExpr{X: parent, Op: token.LAND, Y: &ast.UnaryExpr{Op: token.NOT, X: child}}}
			loop = append(loop, e.guard(invalid, "supplied child of a deleted parent must be explicitly marked for deletion"))
		}
		if len(loop) > 0 {
			body = append(body, a.frameLoop(role, loop))
		}
	}
	return body, nil
}

// markerType consumes canonical type authority, including authored import aliases.
func (e *actionEmitter) markerType(field plan.EntityField) (string, error) {
	name, err := (xshape.Resolver{Package: field.Type.Package, Imports: e.l.pathsByAlias}).Canonical(field.Type.Name)
	return strings.TrimPrefix(name, "*"), err
}
