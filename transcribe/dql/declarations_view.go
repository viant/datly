package dql

import (
	"fmt"
	"go/ast"
	"go/parser"
	"strings"

	"github.com/viant/datly/spec"
)

type declarationViewOptions struct {
	materialize bool
	used        bool
	firstOffset int
	connector   string
	typeName    string
	dest        string
	cache       string
	limit       *int
	columns     []*declarationColumnOptions
	byColumn    map[string]*declarationColumnOptions
}

func (o *declarationViewOptions) mark(offset int) {
	if !o.used {
		o.used = true
		o.firstOffset = offset
	}
}

type declarationColumnOptions struct {
	name       string
	typeExpr   string
	typeOffset int
	tag        string
	groupable  *bool
}

func (o *declarationViewOptions) column(name string) *declarationColumnOptions {
	name = strings.TrimSpace(name)
	key := strings.ToLower(name)
	if o.byColumn == nil {
		o.byColumn = map[string]*declarationColumnOptions{}
	}
	if existing := o.byColumn[key]; existing != nil {
		return existing
	}
	result := &declarationColumnOptions{name: name}
	o.byColumn[key] = result
	o.columns = append(o.columns, result)
	return result
}

func materializeDeclaredViews(params []*spec.Parameter, options map[string]declarationViewOptions, typeContext *spec.TypeContext) ([]*spec.View, error) {
	var result []*spec.View
	for _, param := range spec.EffectiveParameters(params) {
		if param == nil || !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "view") || strings.TrimSpace(param.DeclarationSQL) == "" {
			continue
		}
		name := strings.TrimSpace(param.Name)
		if name == "" {
			continue
		}
		viewOptions := options[param.Identity()]
		if !viewOptions.materialize {
			continue
		}
		view := &spec.View{
			Key:      spec.Key{Kind: spec.KindView, Name: name},
			Name:     name,
			TypeName: viewOptions.typeName,
			Dest:     viewOptions.dest,
			Source: &spec.ViewSource{
				SQL: strings.TrimSpace(param.DeclarationSQL), URI: strings.TrimSpace(param.ResourceRef),
				Embeds: EmbeddedSQLRefs(param.DeclarationSQL),
			},
		}
		switch strings.ToLower(strings.TrimSpace(param.Cardinality)) {
		case "one":
			view.Cardinality = spec.CardinalityOne
		case "many":
			view.Cardinality = spec.CardinalityMany
		}
		if viewOptions.connector != "" || viewOptions.cache != "" {
			view.Source.Bindings = &spec.ViewBindings{Connector: viewOptions.connector, CacheName: viewOptions.cache}
		}
		if viewOptions.limit != nil {
			limit := *viewOptions.limit
			view.Source.Controls = &spec.ViewControls{Limit: &limit}
		}
		for _, authored := range viewOptions.columns {
			if authored == nil {
				continue
			}
			column := &spec.Column{Name: authored.name, Source: authored.name, Tag: authored.tag}
			if authored.groupable != nil {
				value := *authored.groupable
				column.Groupable = &value
			}
			if authored.typeExpr != "" {
				columnType, err := declarationColumnType(authored.typeExpr, typeContext)
				if err != nil {
					return nil, &sourceError{
						offset: authored.typeOffset, end: authored.typeOffset + 1,
						err: fmt.Errorf("view %s column %s type %q: %w", name, authored.name, authored.typeExpr, err),
					}
				}
				column.Type = columnType
			}
			view.Columns = append(view.Columns, column)
		}
		result = append(result, view)
	}
	return result, nil
}

func declarationColumnType(expression string, context *spec.TypeContext) (spec.TypeRef, error) {
	return ColumnType(expression, context)
}

// ColumnType normalizes authored column type declarations under the component's
// import authority. Fluent ColumnType and projection CAST share this policy.
func ColumnType(expression string, context *spec.TypeContext) (spec.TypeRef, error) {
	parsed, err := parser.ParseExpr(strings.TrimSpace(expression))
	if err != nil {
		return spec.TypeRef{}, err
	}
	return declarationColumnTypeNode(parsed, context, spec.TypeRef{})
}

func declarationColumnTypeNode(expression ast.Expr, context *spec.TypeContext, result spec.TypeRef) (spec.TypeRef, error) {
	switch actual := expression.(type) {
	case *ast.ParenExpr:
		return declarationColumnTypeNode(actual.X, context, result)
	case *ast.StarExpr:
		if result.Pointer || result.SlicePointer {
			return spec.TypeRef{}, fmt.Errorf("multiple pointer layers are not supported")
		}
		result.Pointer = true
		return declarationColumnTypeNode(actual.X, context, result)
	case *ast.ArrayType:
		if actual.Len != nil {
			return spec.TypeRef{}, fmt.Errorf("fixed arrays are not supported")
		}
		if result.Cardinality != "" {
			return spec.TypeRef{}, fmt.Errorf("nested slices are not supported")
		}
		result.Cardinality = spec.CardinalityMany
		result.SlicePointer = result.Pointer
		result.Pointer = false
		return declarationColumnTypeNode(actual.Elt, context, result)
	case *ast.Ident:
		result.Name = actual.Name
		if !isPredeclaredColumnType(actual.Name) && context != nil {
			result.Package = strings.TrimSpace(context.DefaultPackage)
		}
		return result, nil
	case *ast.SelectorExpr:
		alias, ok := actual.X.(*ast.Ident)
		if !ok || alias.Name == "" || actual.Sel == nil || actual.Sel.Name == "" {
			return spec.TypeRef{}, fmt.Errorf("qualified type must use one imported package alias")
		}
		packagePath, ok := declarationImportPath(context, alias.Name)
		if !ok {
			return spec.TypeRef{}, fmt.Errorf("package alias %q is not declared with #import", alias.Name)
		}
		result.Package = packagePath
		result.Name = actual.Sel.Name
		return result, nil
	default:
		return spec.TypeRef{}, fmt.Errorf("unsupported column type expression %T", expression)
	}
}

func declarationImportPath(context *spec.TypeContext, alias string) (string, bool) {
	if context != nil {
		for _, item := range context.Imports {
			if strings.TrimSpace(item.Alias) == alias && strings.TrimSpace(item.Package) != "" {
				return strings.TrimSpace(item.Package), true
			}
		}
	}
	if alias == "time" {
		return "time", true
	}
	return "", false
}

func isPredeclaredColumnType(name string) bool {
	switch name {
	case "any", "bool", "byte", "complex64", "complex128", "error", "float32", "float64",
		"int", "int8", "int16", "int32", "int64", "rune", "string",
		"uint", "uint8", "uint16", "uint32", "uint64", "uintptr":
		return true
	default:
		return false
	}
}
