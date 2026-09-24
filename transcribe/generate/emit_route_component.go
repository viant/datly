package generate

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
)

func componentFileText(packageName string, plan *Plan) (string, error) {
	var b strings.Builder
	anchors := externalLifecycleAnchors(plan)
	imports := plan.holderImports()
	for _, anchor := range anchors {
		imports = append(imports, spec.ImportSpec{Alias: anchor.alias, Package: anchor.path})
	}
	addImport := func(path, preferred string) string {
		for _, item := range imports {
			if item.Package == path {
				if item.Alias == "" {
					return packageAlias(path)
				}
				return item.Alias
			}
		}
		alias := availableImportAlias(imports, preferred)
		imports = append(imports, spec.ImportSpec{Alias: alias, Package: path})
		return alias
	}
	reflectAlias := addImport("reflect", "reflect")
	xdatlyAlias := addImport("github.com/viant/xdatly", "xdatly")
	embedAlias, runtimeAlias, adapterAlias := "", "", ""
	if plan.Resources != nil {
		embedAlias = addImport("embed", "embed")
	}
	if plan.Handler != "" && (plan.MutationHandler != nil || plan.ContractHandler != nil || plan.ExternalHandler != nil) {
		runtimeAlias = addImport("github.com/viant/datly/runtime/handler", "rhandler")
		if plan.MutationHandler != nil {
			adapterAlias = addImport("github.com/viant/datly/runtime/handler/mutation", "mutationhandler")
		} else {
			adapterAlias = addImport("github.com/viant/datly/runtime/handler/custom", "customhandler")
		}
	}
	b.WriteString("package ")
	b.WriteString(packageName)
	b.WriteString("\n\n")
	b.WriteString("import (\n")
	seenImports := map[string]bool{}
	for _, item := range imports {
		key := item.Alias + " " + item.Package
		if seenImports[key] {
			continue
		}
		seenImports[key] = true
		b.WriteString("\t")
		b.WriteString(item.Alias)
		b.WriteString(" ")
		b.WriteString(fmt.Sprintf("%q", item.Package))
		b.WriteString("\n")
	}
	b.WriteString(")\n\n")
	b.WriteString("func init() {}\n\n")
	b.WriteString("// Component is the generated component scaffold for ")
	b.WriteString(plan.ComponentName)
	b.WriteString(".\n")
	b.WriteString("type " + plan.HolderName() + " struct {\n")
	if len(plan.Routes) == 0 {
		writeComponentHolderField(&b, "Contract", plan, "", xdatlyAlias)
	} else {
		for index, route := range plan.Routes {
			routeTag := plan.componentTag(route)
			if err := routeTag.ValidateRoute(); err != nil {
				return "", err
			}
			structTag, err := routeTag.StructTag()
			if err != nil {
				return "", err
			}
			fieldName := "Contract"
			if len(plan.Routes) > 1 {
				fieldName = fmt.Sprintf("Contract%d", index+1)
			}
			writeComponentHolderField(&b, fieldName, plan, structTag, xdatlyAlias)
		}
	}
	b.WriteString("}\n")
	b.WriteString("\n// ")
	b.WriteString(strings.TrimSuffix(plan.HolderName(), "Component"))
	b.WriteString("DatlyType keeps the public component type linked for blank-import discovery.\n")
	b.WriteString("func ")
	b.WriteString(strings.TrimSuffix(plan.HolderName(), "Component"))
	b.WriteString("DatlyType() " + reflectAlias + ".Type { return " + reflectAlias + ".TypeOf((*")
	b.WriteString(plan.HolderName())
	b.WriteString(")(nil)).Elem() }\n")
	b.WriteString("\n// Datly anchors this package's public component contract.\n")
	b.WriteString("var ")
	b.WriteString(strings.TrimSuffix(plan.HolderName(), "Component"))
	b.WriteString("Datly = new(")
	b.WriteString(plan.HolderName())
	b.WriteString(")\n")
	b.WriteString("var ")
	b.WriteString(strings.TrimSuffix(plan.HolderName(), "Component"))
	b.WriteString("DatlyLinkedType = ")
	b.WriteString(strings.TrimSuffix(plan.HolderName(), "Component"))
	b.WriteString("DatlyType()\n")
	for _, anchor := range anchors {
		b.WriteString("\nfunc ")
		b.WriteString(anchor.symbol)
		b.WriteString("DatlyType() " + reflectAlias + ".Type { return " + reflectAlias + ".TypeOf((*")
		b.WriteString(anchor.expression)
		b.WriteString(")(nil)).Elem() }\n")
		b.WriteString("var ")
		b.WriteString(anchor.symbol)
		b.WriteString("DatlyLinkedType = ")
		b.WriteString(anchor.symbol)
		b.WriteString("DatlyType()\n")
	}
	if plan.Handler != "" && (plan.MutationHandler != nil || plan.ContractHandler != nil || plan.ExternalHandler != nil) {
		name := availableImportAlias(imports, "name")
		b.WriteString("\nfunc (")
		b.WriteString(plan.HolderName())
		b.WriteString(") DatlyHandler(" + name + " string) func() (" + runtimeAlias + ".TypedHandler, error) {\n")
		b.WriteString("\tif " + name + " == ")
		b.WriteString(strconv.Quote(plan.Handler))
		b.WriteString(" { return ")
		b.WriteString(adapterAlias + ".Factory[")
		b.WriteString(plan.contractType(plan.Input))
		b.WriteString(", ")
		b.WriteString(plan.contractType(plan.Output))
		b.WriteString("](")
		if plan.ExternalHandler != nil {
			b.WriteString(plan.FactoryExpression)
		} else {
			b.WriteString(plan.Handler)
		}
		b.WriteString(") }\n")
		b.WriteString("\treturn nil\n}\n")
		b.WriteString("\n// ")
		b.WriteString(strings.TrimSuffix(plan.HolderName(), "Component"))
		b.WriteString("Handler keeps the public typed handler capability linked.\n")
		b.WriteString("var ")
		b.WriteString(strings.TrimSuffix(plan.HolderName(), "Component"))
		b.WriteString("Handler = ")
		b.WriteString(plan.HolderName())
		b.WriteString("{}.DatlyHandler\n")
	}
	if plan.Resources != nil {
		b.WriteString("\nfunc (")
		b.WriteString(plan.HolderName())
		b.WriteString(") EmbedFS() *" + embedAlias + ".FS {\n")
		b.WriteString("\treturn &")
		b.WriteString(plan.Resources.Symbol)
		b.WriteString("DatlyResources\n}\n")
		b.WriteString("\nfunc (")
		b.WriteString(plan.HolderName())
		b.WriteString(") EmbedNamespace() string {\n")
		b.WriteString("\treturn ")
		b.WriteString(plan.Resources.Symbol)
		b.WriteString("DatlyResourceNamespace\n}\n")
	}
	return b.String(), nil
}

type lifecycleAnchor struct {
	alias, path, expression, symbol string
}

func externalLifecycleAnchors(plan *Plan) []lifecycleAnchor {
	if plan == nil {
		return nil
	}
	imports := map[string]string{}
	for _, item := range plan.Imports {
		imports[item.Alias] = item.Package
	}
	seen := map[string]bool{}
	var result []lifecycleAnchor
	for _, expression := range plan.LifecycleTypes {
		expression = strings.TrimSpace(strings.TrimPrefix(expression, "*"))
		index := strings.Index(expression, ".")
		if index <= 0 {
			continue
		}
		alias := expression[:index]
		path := imports[alias]
		if path == "" || path == plan.Package || seen[expression] {
			continue
		}
		seen[expression] = true
		result = append(result, lifecycleAnchor{alias: alias, path: path, expression: expression, symbol: upperCamel(alias + "_" + expression[index+1:])})
	}
	return result
}

func (p *Plan) componentTag(route RoutePlan) dtag.Component {
	result := dtag.Component{
		Documentation: p.Documentation.Clone(), Name: p.ComponentName, RouteName: route.Name, Path: route.Path, Method: route.Method,
		Marshaller: route.Marshaller, Handler: p.Handler,
		APIKeyHeader: route.APIKeyHeader, APIKeyValue: route.APIKeyValue,
		Internal:  route.Internal,
		MCP:       route.MCP,
		Connector: p.Connector, View: p.RootViewName, Source: p.RootSource,
		Description: p.Description, Example: p.Example, Settings: p.Settings,
	}
	if p.Report != nil {
		result.ReportCompose = p.Report.Compose.Clone()
		result.Report = p.Report.Enabled
		if p.Report.MCPTool != nil {
			enabled := *p.Report.MCPTool
			result.ReportMCPTool = &enabled
		}
		result.ReportLinkedInputType = p.Report.LinkedInputType
		if layout := p.Report.InputLayout; layout != nil {
			result.ReportDimensions = layout.Dimensions
			result.ReportMeasures = layout.Measures
			result.ReportFilters = layout.Filters
			result.ReportOrderBy = layout.OrderBy
			result.ReportLimit = layout.Limit
			result.ReportOffset = layout.Offset
		}
	}
	return result
}

func writeComponentHolderField(builder *strings.Builder, name string, plan *Plan, structTag, xdatlyAlias string) {
	builder.WriteString("\t")
	builder.WriteString(name)
	builder.WriteString(" " + xdatlyAlias + ".Component[")
	builder.WriteString(plan.contractType(plan.Input))
	builder.WriteString(", ")
	builder.WriteString(plan.contractType(plan.Output))
	builder.WriteString("]")
	if structTag != "" {
		builder.WriteString(" ")
		builder.WriteString(strconv.Quote(structTag))
	}
	builder.WriteString("\n")
}
