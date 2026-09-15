package generate

import (
	"fmt"
	"go/ast"
	"go/parser"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
)

func scaffoldFile(packageName string, comment string, body string) string {
	var b strings.Builder
	b.WriteString("package ")
	b.WriteString(packageName)
	b.WriteString("\n\n")
	b.WriteString(comment)
	b.WriteString("\n")
	b.WriteString(body)
	b.WriteString("\n")
	return b.String()
}

func viewFile(packageName string, plan *Plan) string {
	views := make([]ViewPlan, 0, len(plan.Views))
	for _, view := range plan.Views {
		if view.Ownership == ViewGenerated {
			views = append(views, view)
		}
	}
	return viewFileContent(packageName, plan, views, true)
}

func viewFileForDestination(packageName string, plan *Plan, destination string) string {
	selected := make([]ViewPlan, 0, len(plan.Views))
	for _, view := range plan.Views {
		if view.Ownership == ViewGenerated && plan.localShape(view.Package) && view.Destination == destination {
			selected = append(selected, view)
		}
	}
	return viewFileContent(packageName, plan, selected, destination == plan.ViewDest)
}

func (plan *Plan) hasViewSupport() bool {
	if plan == nil {
		return false
	}
	for _, helper := range plan.HelperTypes {
		if plan.localShape(helper.Package) {
			return true
		}
	}
	return !plan.ShapesOnly && len(plan.referencedPlaceholderTypes()) > 0
}

func viewFileContent(packageName string, plan *Plan, views []ViewPlan, includeSupport bool) string {
	var b strings.Builder
	b.WriteString("package ")
	b.WriteString(packageName)
	var viewFields []Field
	for _, view := range views {
		viewFields = append(viewFields, view.Fields...)
	}
	if includeSupport {
		for _, helper := range plan.HelperTypes {
			if !plan.localShape(helper.Package) {
				continue
			}
			viewFields = append(viewFields, helper.Fields...)
		}
	}
	if used := importsForFields(viewFields, plan.Imports); len(used) > 0 {
		b.WriteString("\n\nimport (\n")
		for _, imp := range used {
			b.WriteString("\t")
			if imp.Alias != "" {
				b.WriteString(imp.Alias)
				b.WriteString(" ")
			}
			b.WriteString(fmt.Sprintf("%q", imp.Package))
			b.WriteString("\n")
		}
		b.WriteString(")\n\n")
	} else {
		b.WriteString("\n\n")
	}
	for index, view := range views {
		if index > 0 {
			b.WriteString("\n")
		}
		b.WriteString("// ")
		b.WriteString(view.Name)
		b.WriteString(" is generated canonical view metadata for ")
		b.WriteString(plan.ComponentName)
		b.WriteString(".\n")
		b.WriteString("type ")
		b.WriteString(view.Name)
		b.WriteString(" struct")
		if len(view.Fields) == 0 {
			b.WriteString("{}\n")
		} else {
			b.WriteString(" {\n")
			appendFields(&b, view.Fields)
			b.WriteString("}\n")
		}
		if len(view.SetMarkerFields) > 0 {
			b.WriteString("\n")
			b.WriteString("type ")
			b.WriteString(view.Name)
			b.WriteString("Has struct {\n")
			for _, field := range view.SetMarkerFields {
				b.WriteString("\t")
				b.WriteString(field)
				b.WriteString(" bool\n")
			}
			b.WriteString("}\n")
		}
	}
	if !includeSupport {
		return b.String()
	}
	for _, helper := range plan.HelperTypes {
		if !plan.localShape(helper.Package) {
			continue
		}
		b.WriteString("\n")
		b.WriteString("type ")
		b.WriteString(helper.Name)
		if len(helper.Fields) == 0 {
			b.WriteString(" struct{}\n")
			continue
		}
		b.WriteString(" struct {\n")
		for _, field := range helper.Fields {
			b.WriteString("\t")
			b.WriteString(field.Name)
			b.WriteString(" ")
			b.WriteString(field.Type)
			b.WriteString("\n")
		}
		b.WriteString("}\n")
	}
	for _, name := range plan.referencedPlaceholderTypes() {
		b.WriteString("\n")
		b.WriteString("type ")
		b.WriteString(name)
		b.WriteString(" struct{}\n")
	}
	return b.String()
}

func appendFields(b *strings.Builder, fields []Field) {
	for _, field := range fields {
		b.WriteString("\t")
		b.WriteString(field.Name)
		b.WriteString(" ")
		b.WriteString(field.Type)
		if field.Tag != "" {
			if strings.ContainsRune(field.Tag, '`') {
				b.WriteString(" ")
				b.WriteString(strconv.Quote(field.Tag))
			} else {
				b.WriteString(" `")
				b.WriteString(field.Tag)
				b.WriteString("`")
			}
		}
		b.WriteString("\n")
	}
}

func structFileWithImports(packageName string, comment string, typeName string, fields []Field, imports []spec.ImportSpec) string {
	var b strings.Builder
	b.WriteString("package ")
	b.WriteString(packageName)
	if used := importsForFields(fields, imports); len(used) > 0 {
		b.WriteString("\n\nimport (\n")
		for _, imp := range used {
			b.WriteString("\t")
			if imp.Alias != "" {
				b.WriteString(imp.Alias)
				b.WriteString(" ")
			}
			b.WriteString(fmt.Sprintf("%q", imp.Package))
			b.WriteString("\n")
		}
		b.WriteString(")\n\n")
	} else {
		b.WriteString("\n\n")
	}
	b.WriteString(comment)
	b.WriteString("\n")
	b.WriteString("type ")
	b.WriteString(typeName)
	b.WriteString(" struct")
	if len(fields) == 0 {
		b.WriteString("{}\n")
		return b.String()
	}
	b.WriteString(" {\n")
	appendFields(&b, fields)
	b.WriteString("}\n")
	return b.String()
}

func importsForFields(fields []Field, imports []spec.ImportSpec) []spec.ImportSpec {
	if len(fields) == 0 || len(imports) == 0 {
		return nil
	}
	seen := map[string]bool{}
	used := usedImportAliases(fields)
	var result []spec.ImportSpec
	for _, imp := range imports {
		if imp.Alias == "" || !used[imp.Alias] || seen[imp.Alias] {
			continue
		}
		seen[imp.Alias] = true
		result = append(result, imp)
	}
	return result
}

func usedImportAliases(fields []Field) map[string]bool {
	result := map[string]bool{}
	for _, field := range fields {
		expression, err := parser.ParseExpr(strings.TrimSpace(field.Type))
		if err != nil {
			continue
		}
		ast.Inspect(expression, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if qualifier, ok := selector.X.(*ast.Ident); ok {
				result[qualifier.Name] = true
			}
			return true
		})
	}
	return result
}
