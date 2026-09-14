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
	b.WriteString("import (\n\txdatly \"github.com/viant/xdatly\"\n")
	for _, item := range plan.contractImports() {
		b.WriteString("\t")
		b.WriteString(item.Alias)
		b.WriteString(" ")
		b.WriteString(fmt.Sprintf("%q", item.Package))
		b.WriteString("\n")
	}
	b.WriteString(")\n\n")
	b.WriteString("// Component is the generated component scaffold for ")
	b.WriteString(plan.ComponentName)
	b.WriteString(".\n")
	b.WriteString("type Component struct {\n")
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
	builder.WriteString(plan.Input.Type)
	builder.WriteString(", ")
	builder.WriteString(plan.Output.Type)
	builder.WriteString("]")
	if structTag != "" {
		builder.WriteString(" ")
		builder.WriteString(strconv.Quote(structTag))
	}
	builder.WriteString("\n")
}
