package generate

import (
	"fmt"
	"strconv"
	"strings"

	dtag "github.com/viant/datly/tag"
)

func componentFileText(packageName string, plan *Plan) (string, error) {
	var b strings.Builder
	b.WriteString("package ")
	b.WriteString(packageName)
	b.WriteString("\n\n")
	b.WriteString("import (\n")
	if plan.Resources != nil {
		b.WriteString("\t\"embed\"\n\n")
	}
	b.WriteString("\txdatly \"github.com/viant/xdatly\"\n")
	if plan.Handler != "" && (plan.MutationHandler != nil || plan.ContractHandler != nil) {
		b.WriteString("\trhandler \"github.com/viant/datly/runtime/handler\"\n")
		if plan.MutationHandler != nil {
			b.WriteString("\tmutationhandler \"github.com/viant/datly/runtime/handler/mutation\"\n")
		} else {
			b.WriteString("\tcustomhandler \"github.com/viant/datly/runtime/handler/custom\"\n")
		}
	}
	for _, item := range plan.holderImports() {
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
		writeComponentHolderField(&b, "Contract", plan, "")
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
			writeComponentHolderField(&b, fieldName, plan, structTag)
		}
	}
	b.WriteString("}\n")
	if plan.Handler != "" && (plan.MutationHandler != nil || plan.ContractHandler != nil) {
		b.WriteString("\nfunc (")
		b.WriteString(plan.HolderName())
		b.WriteString(") DatlyHandler(name string) func() (rhandler.TypedHandler, error) {\n")
		b.WriteString("\tif name == ")
		b.WriteString(strconv.Quote(plan.Handler))
		b.WriteString(" { return ")
		if plan.MutationHandler != nil {
			b.WriteString("mutationhandler.Factory[")
		} else {
			b.WriteString("customhandler.Factory[")
		}
		b.WriteString(plan.contractType(plan.Input))
		b.WriteString(", ")
		b.WriteString(plan.contractType(plan.Output))
		b.WriteString("](")
		b.WriteString(plan.Handler)
		b.WriteString(") }\n")
		b.WriteString("\treturn nil\n}\n")
	}
	if plan.Resources != nil {
		b.WriteString("\nfunc (")
		b.WriteString(plan.HolderName())
		b.WriteString(") EmbedFS() *embed.FS {\n")
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

func (p *Plan) componentTag(route RoutePlan) dtag.Component {
	result := dtag.Component{
		Documentation: p.Documentation.Clone(), Name: p.ComponentName, RouteName: route.Name, Path: route.Path, Method: route.Method,
		Marshaller: route.Marshaller, Handler: p.Handler,
		APIKeyHeader: route.APIKeyHeader, APIKeyValue: route.APIKeyValue,
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

func writeComponentHolderField(builder *strings.Builder, name string, plan *Plan, structTag string) {
	builder.WriteString("\t")
	builder.WriteString(name)
	builder.WriteString(" xdatly.Component[")
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
