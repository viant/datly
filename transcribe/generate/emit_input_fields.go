package generate

import (
	"go/ast"
	"go/parser"
	"strings"
	"unicode"

	"github.com/viant/datly/spec"
)

func inputStructFile(packageName string, comment string, typeName string, fields []Field, imports []spec.ImportSpec) string {
	var allFields []Field
	allFields = append(allFields, fields...)
	if markerField, ok := hasMarkerField(typeName, fields); ok {
		allFields = append(allFields, markerField)
	}
	content := structFileWithImports(packageName, comment, typeName, allFields, imports)
	if hasBodyFields(fields) {
		var b strings.Builder
		b.WriteString(content)
		b.WriteString("\n")
		b.WriteString("type ")
		b.WriteString(typeName)
		b.WriteString("Has struct {\n")
		for _, field := range fields {
			if shouldSkipHasMirror(field) {
				continue
			}
			b.WriteString("\t")
			b.WriteString(field.Name)
			b.WriteString(" bool\n")
		}
		b.WriteString("}\n")
		return b.String()
	}
	return content
}

func hasBodyFields(fields []Field) bool {
	for _, field := range fields {
		if field.Source == "output" || shouldSkipHasMirror(field) {
			continue
		}
		return true
	}
	return false
}

func shouldSkipHasMirror(field Field) bool {
	return field.Implementation || field.Name == "Has" || strings.Contains(field.Tag, `setMarker:"true"`)
}

func hasMarkerField(typeName string, fields []Field) (Field, bool) {
	if !hasBodyFields(fields) {
		return Field{}, false
	}
	for _, field := range fields {
		if field.Name == "Has" || strings.Contains(field.Tag, `setMarker:"true"`) {
			return Field{}, false
		}
	}
	return Field{
		Name: "Has",
		Type: "*" + typeName + "Has",
		Tag:  `setMarker:"true" typeName:"` + typeName + `Has" json:"-" sqlx:"-"`,
	}, true
}

func (plan *Plan) referencedPlaceholderTypes() []string {
	if plan == nil {
		return nil
	}
	seen := map[string]bool{}
	helperTypes := map[string]bool{}
	for _, helper := range plan.HelperTypes {
		helperTypes[helper.Name] = true
	}
	var result []string
	appendFromFields := func(fields []Field) {
		for _, field := range fields {
			if field.Implementation {
				continue
			}
			for _, name := range extractPlaceholderTypeNames(field.Type) {
				if shouldSkipPlaceholderType(plan, name) || seen[name] || helperTypes[name] {
					continue
				}
				seen[name] = true
				result = append(result, name)
			}
			for _, name := range extractTagPlaceholderTypeNames(field.Tag) {
				if fieldTypeImportsName(field.Type, name) || shouldSkipPlaceholderType(plan, name) || seen[name] || helperTypes[name] {
					continue
				}
				seen[name] = true
				result = append(result, name)
			}
		}
	}
	if plan.Input.Ownership == ContractGenerated {
		appendFromFields(plan.Input.Fields)
	}
	if plan.Output.Ownership == ContractGenerated {
		appendFromFields(plan.Output.Fields)
	}
	return result
}

func shouldSkipPlaceholderType(plan *Plan, name string) bool {
	if name == "" || strings.Contains(name, ".") || name == plan.Input.Type || name == plan.Output.Type || name == "Component" || name == "Route" {
		return true
	}
	for _, view := range plan.Views {
		if name == view.Name {
			return true
		}
	}
	switch name {
	case "any", "string", "bool", "byte", "rune", "error",
		"int", "int8", "int16", "int32", "int64",
		"uint", "uint8", "uint16", "uint32", "uint64", "uintptr",
		"float32", "float64", "complex64", "complex128":
		return true
	}
	return false
}

func extractPlaceholderTypeNames(typeExpr string) []string {
	expression, err := parser.ParseExpr(strings.TrimSpace(typeExpr))
	if err != nil {
		return nil
	}
	var result []string
	var collect func(ast.Expr)
	collect = func(candidate ast.Expr) {
		switch actual := candidate.(type) {
		case *ast.Ident:
			if runes := []rune(actual.Name); len(runes) > 0 && unicode.IsUpper(runes[0]) {
				result = append(result, actual.Name)
			}
		case *ast.SelectorExpr:
			return
		case *ast.ParenExpr:
			collect(actual.X)
		case *ast.StarExpr:
			collect(actual.X)
		case *ast.ArrayType:
			collect(actual.Elt)
		case *ast.MapType:
			collect(actual.Key)
			collect(actual.Value)
		case *ast.ChanType:
			collect(actual.Value)
		case *ast.Ellipsis:
			collect(actual.Elt)
		case *ast.IndexExpr:
			collect(actual.X)
			collect(actual.Index)
		case *ast.IndexListExpr:
			collect(actual.X)
			for _, index := range actual.Indices {
				collect(index)
			}
		}
	}
	collect(expression)
	return result
}

func extractTagPlaceholderTypeNames(tag string) []string {
	return extractPlaceholderTypeNames(defaultTagTypeName(tag))
}

func fieldTypeImportsName(typeExpr, name string) bool {
	expression, err := parser.ParseExpr(strings.TrimSpace(typeExpr))
	if err != nil || strings.TrimSpace(name) == "" {
		return false
	}
	matched := false
	ast.Inspect(expression, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return !matched
		}
		if selector.Sel != nil && selector.Sel.Name == name {
			matched = true
			return false
		}
		return true
	})
	return matched
}
