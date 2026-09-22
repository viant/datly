package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

type beforeRecord func(ast.Expr) ([]ast.Stmt, error)

func (l *lowerer) traversalStatements(record *recordLowering, before beforeRecord) ([]ast.Stmt, error) {
	values, err := pathExpr(record.plan.InputPath)
	if err != nil {
		return nil, err
	}
	return l.traverseValues(record, values, before)
}

func (l *lowerer) traverseValues(record *recordLowering, values ast.Expr, before beforeRecord) ([]ast.Stmt, error) {
	recordVariable := fmt.Sprintf("record%d", record.order)
	body, err := l.recordBody(record, ast.NewIdent(recordVariable), before)
	if err != nil {
		return nil, err
	}
	if record.value.many {
		rangeStatement := &ast.RangeStmt{Tok: token.DEFINE, X: values, Body: &ast.BlockStmt{List: body}}
		if record.value.pointer {
			rangeStatement.Key = ast.NewIdent("_")
			rangeStatement.Value = ast.NewIdent(recordVariable)
			rangeStatement.Body.List = append([]ast.Stmt{nilContinue(recordVariable)}, body...)
		} else {
			offset := fmt.Sprintf("recordIndex%d", record.order)
			rangeStatement.Key = ast.NewIdent(offset)
			rangeStatement.Body.List = append([]ast.Stmt{&ast.AssignStmt{
				Lhs: []ast.Expr{ast.NewIdent(recordVariable)}, Tok: token.DEFINE,
				Rhs: []ast.Expr{&ast.UnaryExpr{Op: token.AND, X: &ast.IndexExpr{X: values, Index: ast.NewIdent(offset)}}},
			}}, body...)
		}
		return []ast.Stmt{rangeStatement}, nil
	}
	assignment := &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent(recordVariable)}, Tok: token.DEFINE}
	if record.value.pointer {
		assignment.Rhs = []ast.Expr{values}
		return []ast.Stmt{assignment, &ast.IfStmt{
			Cond: &ast.BinaryExpr{X: ast.NewIdent(recordVariable), Op: token.NEQ, Y: ast.NewIdent("nil")},
			Body: &ast.BlockStmt{List: body},
		}}, nil
	}
	assignment.Rhs = []ast.Expr{&ast.UnaryExpr{Op: token.AND, X: values}}
	return append([]ast.Stmt{assignment}, body...), nil
}

func (l *lowerer) recordBody(record *recordLowering, value ast.Expr, before beforeRecord) ([]ast.Stmt, error) {
	var result []ast.Stmt
	if !record.plan.Auxiliary {
		if before != nil {
			statements, err := before(value)
			if err != nil {
				return nil, err
			}
			result = append(result, statements...)
		}
		result = append(result, l.entityWriteHookStatements(record, value)...)
		writes, err := l.recordWriteStatements(record, value)
		if err != nil {
			return nil, err
		}
		result = append(result, writes...)
	}
	for _, relation := range record.plan.Relations {
		if relation.Child.Auxiliary {
			continue
		}
		child := l.recordByPlan[relation.Child]
		if child == nil {
			return nil, fmt.Errorf("generated Go relation %q has no compiled child record", relation.Identity)
		}
		holder, pathErr := selectPathExpr(value, relation.FieldPath)
		if pathErr != nil {
			return nil, pathErr
		}
		hook := l.hookByRelation[relation]
		if hook == nil {
			return nil, fmt.Errorf("generated Go relation %q has no compiled hook", relation.Identity)
		}
		result = append(result, l.relationWriteHookStatements(hook, value, holder)...)
		parent := value
		children, childErr := l.traverseValues(child, holder, func(childValue ast.Expr) ([]ast.Stmt, error) {
			return l.relationLinkStatements(relation, parent, childValue)
		})
		if childErr != nil {
			return nil, childErr
		}
		result = append(result, children...)
	}
	return result, nil
}

func (l *lowerer) entityWriteHookStatements(record *recordLowering, value ast.Expr) []ast.Stmt {
	return []ast.Stmt{
		l.optionalWriteHook(
			fmt.Sprintf("writeInitializer%d", record.order), value,
			selectExpr(ast.NewIdent(l.handlerAlias), "WriteInitializer"), "InitWrite",
		),
		l.optionalWriteHook(
			fmt.Sprintf("writeValidator%d", record.order), value,
			selectExpr(ast.NewIdent(l.handlerAlias), "WriteValidator"), "ValidateWrite",
		),
	}
}

func (l *lowerer) relationWriteHookStatements(hook *relationHookLowering, parent, holder ast.Expr) []ast.Stmt {
	name := fmt.Sprintf("relationWriter%d", hook.order)
	return []ast.Stmt{&ast.IfStmt{
		Init: &ast.AssignStmt{
			Lhs: []ast.Expr{ast.NewIdent(name), ast.NewIdent("ok")}, Tok: token.DEFINE,
			Rhs: []ast.Expr{&ast.TypeAssertExpr{X: emptyInterfaceConversion(parent), Type: ast.NewIdent(hook.interfaceName)}},
		},
		Cond: ast.NewIdent("ok"),
		Body: &ast.BlockStmt{List: []ast.Stmt{errorGuard(callExpr(
			selectExpr(ast.NewIdent(name), hook.methodName), ast.NewIdent("ctx"), holder,
		))}},
	}}
}

func (l *lowerer) optionalWriteHook(name string, value, hookType ast.Expr, method string) ast.Stmt {
	return &ast.IfStmt{
		Init: &ast.AssignStmt{
			Lhs: []ast.Expr{ast.NewIdent(name), ast.NewIdent("ok")}, Tok: token.DEFINE,
			Rhs: []ast.Expr{&ast.TypeAssertExpr{X: emptyInterfaceConversion(value), Type: hookType}},
		},
		Cond: ast.NewIdent("ok"),
		Body: &ast.BlockStmt{List: []ast.Stmt{errorGuard(callExpr(
			selectExpr(ast.NewIdent(name), method), ast.NewIdent("ctx"),
		))}},
	}
}

func emptyInterfaceConversion(value ast.Expr) ast.Expr {
	return callExpr(parseExpr("interface{}"), value)
}

func (l *lowerer) relationLinkStatements(relation *plan.RelationPlan, parent, child ast.Expr) ([]ast.Stmt, error) {
	result := make([]ast.Stmt, 0, len(relation.Links)*2)
	childRecord := l.recordByPlan[relation.Child]
	if childRecord == nil {
		return nil, fmt.Errorf("generated Go relation %q has no compiled child record", relation.Identity)
	}
	for _, link := range relation.Links {
		source := selectExpr(parent, link.Parent.Field)
		target := selectExpr(child, link.Child.Field)
		var value ast.Expr = source
		switch link.Conversion {
		case plan.LinkDirect:
		case plan.LinkAddress:
			value = &ast.UnaryExpr{Op: token.AND, X: source}
		case plan.LinkDereference:
			result = append(result, &ast.IfStmt{
				Cond: &ast.BinaryExpr{X: source, Op: token.EQL, Y: ast.NewIdent("nil")},
				Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(callExpr(
					selectExpr(ast.NewIdent(l.fmtAlias), "Errorf"),
					stringExpr("missing parent key %s for relation %s"),
					stringExpr(link.Parent.Field), stringExpr(relation.Identity),
				))}},
			})
			value = &ast.StarExpr{X: source}
		default:
			return nil, fmt.Errorf("generated Go relation %q has unsupported key conversion %q", relation.Identity, link.Conversion)
		}
		result = append(result, &ast.AssignStmt{Lhs: []ast.Expr{target}, Tok: token.ASSIGN, Rhs: []ast.Expr{value}})
		if relation.Child.TracksPresence(link.Child.Field) {
			marker := selectExpr(child, "Has")
			markerType := parseExpr(childRecord.value.base + "Has")
			result = append(result,
				&ast.IfStmt{
					Cond: &ast.BinaryExpr{X: marker, Op: token.EQL, Y: ast.NewIdent("nil")},
					Body: &ast.BlockStmt{List: []ast.Stmt{&ast.AssignStmt{
						Lhs: []ast.Expr{marker}, Tok: token.ASSIGN,
						Rhs: []ast.Expr{&ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: markerType}}},
					}}},
				},
				&ast.AssignStmt{
					Lhs: []ast.Expr{selectExpr(marker, link.Child.Field)}, Tok: token.ASSIGN,
					Rhs: []ast.Expr{ast.NewIdent("true")},
				},
			)
		}
	}
	return result, nil
}

func (l *lowerer) recordWriteStatements(record *recordLowering, value ast.Expr) ([]ast.Stmt, error) {
	switch l.plan.Operation {
	case plan.OperationPost:
		write, err := l.dmlGuard(record, value, record.plan.Write.Missing)
		if err != nil {
			return nil, err
		}
		return []ast.Stmt{write}, nil
	case plan.OperationPut:
		write, err := l.dmlGuard(record, value, record.plan.Write.Existing)
		if err != nil {
			return nil, err
		}
		return []ast.Stmt{write}, nil
	case plan.OperationPatch:
	default:
		return nil, fmt.Errorf("unsupported generated Go write operation %q", l.plan.Operation)
	}
	keyVariable := fmt.Sprintf("recordKey%d", record.order)
	okVariable := fmt.Sprintf("recordKeyOK%d", record.order)
	existsVariable := fmt.Sprintf("recordExists%d", record.order)
	statements := []ast.Stmt{&ast.AssignStmt{
		Lhs: []ast.Expr{ast.NewIdent(keyVariable), ast.NewIdent(okVariable)}, Tok: token.DEFINE,
		Rhs: []ast.Expr{callExpr(ast.NewIdent(l.recordKeyFunction(record)), value)},
	}}
	statements = append(statements, &ast.IfStmt{
		Cond: &ast.UnaryExpr{Op: token.NOT, X: ast.NewIdent(okVariable)},
		Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(callExpr(
			selectExpr(ast.NewIdent(l.fmtAlias), "Errorf"), stringExpr("missing generated PATCH key for view %s"), stringExpr(record.plan.Identity),
		))}},
	})
	existing, err := l.dmlGuard(record, value, record.plan.Write.Existing)
	if err != nil {
		return nil, err
	}
	missing, err := l.dmlGuard(record, value, record.plan.Write.Missing)
	if err != nil {
		return nil, err
	}
	statements = append(statements, &ast.IfStmt{
		Init: &ast.AssignStmt{
			Lhs: []ast.Expr{ast.NewIdent("_"), ast.NewIdent(existsVariable)}, Tok: token.DEFINE,
			Rhs: []ast.Expr{&ast.IndexExpr{X: ast.NewIdent(l.currentIndexVariable(record)), Index: ast.NewIdent(keyVariable)}},
		},
		Cond: ast.NewIdent(existsVariable),
		Body: &ast.BlockStmt{List: []ast.Stmt{existing}},
		Else: &ast.BlockStmt{List: []ast.Stmt{missing}},
	})
	return statements, nil
}

func (l *lowerer) usesFmt() bool {
	if l.plan.Operation == plan.OperationPatch {
		for _, record := range l.records {
			if record.plan.Current != nil {
				return true
			}
		}
	}
	for _, record := range l.records {
		if record.plan.Auxiliary {
			continue
		}
		for _, relation := range record.plan.Relations {
			if relation.Child.Auxiliary {
				continue
			}
			for _, link := range relation.Links {
				if link.Conversion == plan.LinkDereference {
					return true
				}
			}
		}
	}
	return false
}

func (l *lowerer) dmlGuard(record *recordLowering, value ast.Expr, action plan.Action) (ast.Stmt, error) {
	method := ""
	switch action {
	case plan.ActionInsert:
		method = "Insert"
	case plan.ActionUpdate:
		method = "Update"
	default:
		return nil, fmt.Errorf("unsupported generated Go DML action %q", action)
	}
	if strings.TrimSpace(record.plan.Table) == "" {
		return nil, fmt.Errorf("generated Go DML table is required for record %q", record.plan.Identity)
	}
	return errorGuard(callExpr(
		selectExpr(selectExpr(ast.NewIdent("dependencies"), "DML"), method),
		stringExpr(record.plan.Table), value,
	)), nil
}

func nilContinue(name string) ast.Stmt {
	return &ast.IfStmt{
		Cond: &ast.BinaryExpr{X: ast.NewIdent(name), Op: token.EQL, Y: ast.NewIdent("nil")},
		Body: &ast.BlockStmt{List: []ast.Stmt{&ast.BranchStmt{Tok: token.CONTINUE}}},
	}
}
