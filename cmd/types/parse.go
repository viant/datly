package types

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	typesAst "github.com/viant/sqlx/io/read/cache/ast"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
)

type Parser struct {
	fileContent []byte
}

func newParser(content []byte) *Parser {
	return &Parser{fileContent: content}
}

func Parse(ctx context.Context, loader afs.Service, filePath string, typeName string) (reflect.Type, error) {
	content, err := loader.DownloadWithURL(ctx, filePath)
	if err != nil {
		return nil, err
	}

	return newParser(content).parse(typeName)
}

func (p *Parser) parse(typeName string) (reflect.Type, error) {
	fileSet := token.NewFileSet()

	expr, err := parser.ParseFile(fileSet, "", string(p.fileContent), 0)
	if err != nil {
		return nil, err
	}

	return p.parseObject(expr.Scope, typeName)
}

func (p *Parser) parseObject(scope *ast.Scope, name string) (reflect.Type, error) {
	object := scope.Lookup(name)
	if object == nil {
		return nil, fmt.Errorf("not found type %v", name)
	}

	decl := object.Decl
	typeSpec, ok := asTypeSpec(decl)
	if !ok {
		return nil, fmt.Errorf("expected %v to be type of %T but got %T", name, typeSpec, decl)
	}

	return p.parseType(typeSpec.Type, scope)
}

func asTypeSpec(decl interface{}) (*ast.TypeSpec, bool) {
	typeSpec, ok := decl.(*ast.TypeSpec)
	return typeSpec, ok
}

func (p *Parser) parseType(aType ast.Expr, scope *ast.Scope) (reflect.Type, error) {
	switch actual := aType.(type) {
	case *ast.Ident:
		object := scope.Lookup(actual.Name)
		if object != nil {
			return p.parseObject(scope, actual.Name)
		}

		return typesAst.Parse(actual.Name)
	case *ast.StructType:
		fields := actual.Fields.List
		structFields := make([]reflect.StructField, 0)
		for _, field := range fields {
			aSpec, _ := asTypeSpec(field.Type)
			for _, ident := range field.Names {
				fieldType := field.Type
				if aSpec != nil {
					fieldType = aSpec.Type
				}

				structFieldType, err := p.parseType(fieldType, scope)
				if err != nil {
					return nil, err
				}

				structFields = append(structFields, newStructField(ident.Name, structFieldType))
			}
		}

		return reflect.StructOf(structFields), nil
	case *ast.SelectorExpr:
		selectorContent := p.fileContent[actual.Pos()-1 : actual.End()]
		if parsedType, err := typesAst.Parse(string(selectorContent)); err == nil {
			return parsedType, nil
		}

		return p.loadExternalType(actual)
	case *ast.StarExpr:
		parseType, err := p.parseType(actual.X, scope)
		if err != nil {
			return nil, err
		}

		return reflect.PtrTo(parseType), nil

	case *ast.ArrayType:
		parseType, err := p.parseType(actual.Elt, scope)
		if err != nil {
			return nil, err
		}

		return reflect.SliceOf(parseType), nil

	case *ast.MapType:
		keyType, err := p.parseType(actual.Key, scope)
		if err != nil {
			return nil, err
		}

		valueType, err := p.parseType(actual.Value, scope)
		if err != nil {
			return nil, err
		}

		return reflect.MapOf(keyType, valueType), nil
	default:
		return nil, fmt.Errorf("unsupported type spec type %T", aType)
	}
}

func (p *Parser) loadExternalType(actual *ast.SelectorExpr) (reflect.Type, error) {
	//ident, ok := asIdent(actual.X)
	panic("unusupported yet")
}

func asIdent(x ast.Expr) (*ast.Ident, bool) {
	ident, ok := x.(*ast.Ident)
	return ident, ok
}

func newStructField(name string, fieldType reflect.Type) reflect.StructField {
	pkgPath := ""
	if name[0] > 'Z' || name[0] < 'A' {
		pkgPath = "github.com/viant/datly/cmd/types"
	}

	return reflect.StructField{
		Name:    name,
		Type:    fieldType,
		PkgPath: pkgPath,
	}
}
