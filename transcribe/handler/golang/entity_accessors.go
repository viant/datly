package golang

import (
	"fmt"
	"go/ast"
	"go/token"
)

func (e *entityEmitter) accessorDeclarations(record *recordLowering) ([]ast.Decl, error) {
	metadata := record.plan.Entity
	if !metadata.Owned {
		return nil, nil
	}
	var result []ast.Decl
	for _, field := range metadata.Fields {
		fieldExpression, err := selectPathExpr(ast.NewIdent("entity"), e.entityFieldPath(record, field.Name))
		if err != nil {
			return nil, err
		}
		getterName := "Get" + field.Name
		getterSignature := record.value.base + "." + getterName
		if previous, ok := e.setters[getterSignature]; ok {
			if previous != field.Type.Name {
				return nil, fmt.Errorf("entity getter %s has conflicting field types", getterSignature)
			}
		} else {
			if err := e.l.markExpressionImports(field.Type.Name); err != nil {
				return nil, err
			}
			e.setters[getterSignature] = field.Type.Name
			result = append(result, &ast.FuncDecl{Name: ast.NewIdent(getterName), Recv: &ast.FieldList{List: []*ast.Field{namedField("entity", record.value.pointerExpr())}}, Type: &ast.FuncType{Params: &ast.FieldList{}, Results: &ast.FieldList{List: []*ast.Field{{Type: parseExpr(field.Type.Name)}}}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(fieldExpression)}}})
			e.asset.Methods = append(e.asset.Methods, EntityMethod{Receiver: record.value.base, Name: getterName, ValueType: field.Type.Name, Getter: true})
		}
		// A delete marker is excluded from physical INSERT/UPDATE columns, but it
		// is still intentional mutation input. Hooks and callers need the same
		// marker-aware setter contract as ordinary sparse fields.
		if !field.Writable && !field.DeleteMarker {
			continue
		}
		name := "Set" + field.Name
		signature := record.value.base + "." + name
		if previous, ok := e.setters[signature]; ok {
			if previous != field.Type.Name {
				return nil, fmt.Errorf("entity setter %s has conflicting field types", signature)
			}
			continue
		}
		e.setters[signature] = field.Type.Name
		if err := e.l.markExpressionImports(field.Type.Name); err != nil {
			return nil, err
		}
		receiver := ast.NewIdent("entity")
		marker := selectExpr(receiver, metadata.MarkerField)
		body := []ast.Stmt{assignStmt(fieldExpression, ast.NewIdent("value"))}
		if metadata.MarkerPointer {
			body = append(body, &ast.IfStmt{Cond: &ast.BinaryExpr{X: marker, Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{assignStmt(marker, &ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: parseExpr(metadata.MarkerType.Name)}})}}})
		}
		body = append(body, assignStmt(selectExpr(marker, field.Name), ast.NewIdent("true")))
		result = append(result, &ast.FuncDecl{Name: ast.NewIdent(name), Recv: &ast.FieldList{List: []*ast.Field{namedField("entity", record.value.pointerExpr())}}, Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("value", parseExpr(field.Type.Name))}}}, Body: &ast.BlockStmt{List: body}})
		e.asset.Methods = append(e.asset.Methods, EntityMethod{Receiver: record.value.base, Name: name, ValueType: field.Type.Name})
	}
	return result, nil
}
