package golang

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"strings"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

type recordShape struct {
	base    string
	many    bool
	pointer bool
}

func (s recordShape) expression() string {
	result := s.base
	if s.pointer {
		result = "*" + result
	}
	if s.many {
		result = "[]" + result
	}
	return result
}

func parseRecordShape(source string, cardinality spec.Cardinality) (recordShape, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		return recordShape{}, fmt.Errorf("canonical type expression is required")
	}
	expression, err := parser.ParseExpr(source)
	if err != nil {
		return recordShape{}, fmt.Errorf("parse %q: %w", source, err)
	}
	result := recordShape{}
	expression = unwrapParens(expression)
	if array, ok := expression.(*ast.ArrayType); ok {
		result.many = true
		expression = unwrapParens(array.Elt)
	}
	if pointer, ok := expression.(*ast.StarExpr); ok {
		result.pointer = true
		expression = unwrapParens(pointer.X)
	}
	switch expression.(type) {
	case *ast.Ident, *ast.SelectorExpr, *ast.IndexExpr, *ast.IndexListExpr:
	default:
		return recordShape{}, fmt.Errorf("record type %q must resolve to a direct named type", source)
	}
	result.base, err = renderExpr(expression)
	if err != nil {
		return recordShape{}, err
	}
	if result.many != (cardinality == spec.CardinalityMany) {
		return recordShape{}, fmt.Errorf("record type %q does not match %s cardinality", source, cardinality)
	}
	return result, nil
}

func unwrapParens(expression ast.Expr) ast.Expr {
	for {
		paren, ok := expression.(*ast.ParenExpr)
		if !ok {
			return expression
		}
		expression = paren.X
	}
}

func renderExpr(expression ast.Expr) (string, error) {
	var result bytes.Buffer
	if err := format.Node(&result, token.NewFileSet(), expression); err != nil {
		return "", err
	}
	return result.String(), nil
}

func parseExpr(source string) ast.Expr {
	expression, err := parser.ParseExpr(source)
	if err != nil {
		panic(err)
	}
	return expression
}

func (s recordShape) pointerExpr() ast.Expr {
	return parseExpr("*" + s.base)
}

type keyShape struct {
	name     string
	parts    []plan.KeyPart
	types    []string
	compound bool
}

type recordLowering struct {
	plan    *plan.RecordPlan
	value   recordShape
	current recordShape
	key     keyShape
	order   int
	owner   *recordLowering
}

type relationHookLowering struct {
	interfaceName string
	methodName    string
	holderType    string
	order         int
}

func (l *lowerer) compileKey(parts []plan.KeyPart, compoundName string) (keyShape, error) {
	if len(parts) == 0 {
		return keyShape{}, fmt.Errorf("Go PATCH lowering requires a planned key")
	}
	result := keyShape{parts: append([]plan.KeyPart(nil), parts...), compound: len(parts) > 1}
	if result.compound {
		result.name = compoundName
	}
	for _, part := range parts {
		if field := strings.TrimSpace(part.Field); !token.IsIdentifier(field) || token.Lookup(field).IsKeyword() {
			return keyShape{}, fmt.Errorf("Go PATCH key field %q is not a Go identifier", part.Field)
		}
		typeName, err := l.keyType(part.Type)
		if err != nil {
			return keyShape{}, fmt.Errorf("Go PATCH key %s: %w", part.Field, err)
		}
		result.types = append(result.types, typeName)
	}
	return result, nil
}

func (l *lowerer) keyType(ref spec.TypeRef) (string, error) {
	name := strings.TrimSpace(strings.TrimPrefix(ref.Name, "*"))
	if name == "" {
		return "", fmt.Errorf("type is required")
	}
	if strings.Contains(name, ".") {
		if err := l.markExpressionImports(name); err != nil {
			return "", err
		}
		return name, nil
	}
	packageName := strings.TrimSpace(ref.Package)
	if packageName == "" {
		expression, err := parser.ParseExpr(name)
		if err != nil {
			return "", err
		}
		if !syntacticallyComparable(expression) {
			return "", fmt.Errorf("type %q is not comparable and cannot be a generated map key", name)
		}
		return name, nil
	}
	alias := packageName
	if strings.Contains(packageName, "/") {
		alias = l.importsByPath[packageName]
		if alias == "" {
			return "", fmt.Errorf("package %q has no configured import", packageName)
		}
	} else if l.pathsByAlias[alias] == "" {
		return "", fmt.Errorf("package alias %q has no configured import", alias)
	}
	packagePath := l.pathsByAlias[alias]
	if packagePath != "" && packagePath != "#generated" {
		l.usedImports[packagePath] = true
	}
	return alias + "." + name, nil
}

func syntacticallyComparable(expression ast.Expr) bool {
	switch actual := unwrapParens(expression).(type) {
	case *ast.Ident, *ast.SelectorExpr, *ast.ChanType:
		return true
	case *ast.StarExpr:
		return true
	case *ast.ArrayType:
		return actual.Len != nil && syntacticallyComparable(actual.Elt)
	case *ast.StructType:
		if actual.Fields == nil {
			return true
		}
		for _, field := range actual.Fields.List {
			if field == nil || !syntacticallyComparable(field.Type) {
				return false
			}
		}
		return true
	case *ast.IndexExpr:
		return syntacticallyComparable(actual.X)
	case *ast.IndexListExpr:
		return syntacticallyComparable(actual.X)
	default:
		return false
	}
}

func (k keyShape) typeExpr() ast.Expr {
	if k.compound {
		return ast.NewIdent(k.name)
	}
	return parseExpr(k.types[0])
}

func (k keyShape) declaration() ast.Decl {
	if !k.compound {
		return nil
	}
	fields := make([]*ast.Field, 0, len(k.parts))
	for index, part := range k.parts {
		fields = append(fields, &ast.Field{Names: []*ast.Ident{ast.NewIdent(part.Field)}, Type: parseExpr(k.types[index])})
	}
	return &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{
		Name: ast.NewIdent(k.name), Type: &ast.StructType{Fields: &ast.FieldList{List: fields}},
	}}}
}
