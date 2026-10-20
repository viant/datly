package golang

import (
	"fmt"
	"go/ast"
	"go/token"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

// AccessorRecord binds final row/marker facts to their package-owned receiver.
// It carries no physical write or lifecycle execution authority.
type AccessorRecord struct {
	Type   string
	Entity *plan.EntityPlan
}

// EntityAccessors emits only the existing public getter/setter AST. In contrast
// to EntitySupport it needs no Current, keys, allocation or snapshot plan.
func EntityAccessors(records []AccessorRecord, config Config) (*EntityAsset, error) {
	l := &lowerer{config: config, importsByPath: map[string]string{}, pathsByAlias: map[string]string{}, usedImports: map[string]bool{}, reservedNames: map[string]bool{}}
	if !token.IsIdentifier(config.Package) || token.Lookup(config.Package).IsKeyword() {
		return nil, fmt.Errorf("generated Go accessor package %q is invalid", config.Package)
	}
	if err := l.indexImports(); err != nil {
		return nil, err
	}
	e := &entityEmitter{l: l, asset: &EntityAsset{}, setters: map[string]string{}}
	file := &ast.File{Name: ast.NewIdent(config.Package)}
	for _, record := range records {
		if record.Entity == nil || !record.Entity.Owned {
			continue
		}
		value, err := parseRecordShape(record.Type, spec.CardinalityOne)
		if err != nil {
			return nil, err
		}
		if !token.IsIdentifier(value.base) {
			return nil, fmt.Errorf("entity helpers require package-owned setter receivers, got %s", value.base)
		}
		declarations, err := e.accessorDeclarations(&recordLowering{value: value, plan: &plan.RecordPlan{Entity: record.Entity}})
		if err != nil {
			return nil, err
		}
		file.Decls = append(file.Decls, declarations...)
	}
	if len(file.Decls) == 0 {
		return nil, nil
	}
	imports := e.importDeclaration(file).(*ast.GenDecl)
	if len(imports.Specs) != 0 {
		file.Decls = append([]ast.Decl{imports}, file.Decls...)
	}
	e.asset.File = file
	return e.asset, nil
}

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
			e.asset.Methods = append(e.asset.Methods, EntityMethod{Receiver: record.value.base, Name: getterName, ValueType: field.Type.Name, Getter: true, FieldAccessor: true})
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
		e.asset.Methods = append(e.asset.Methods, EntityMethod{Receiver: record.value.base, Name: name, ValueType: field.Type.Name, FieldAccessor: true})
	}
	return result, nil
}
