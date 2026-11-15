package report

import (
	"net/http"
	"strings"

	"github.com/viant/datly/spec"
)

// ProjectedComponent describes one derived runtime component produced from a
// grouped reader. It is the catalog-level identity used by hosts and authoring
// tools; compilation remains owned by ProjectCompiler.
type ProjectedComponent struct {
	Name        string
	Method      string
	Path        string
	MCPEnabled  bool
	Description string
	MCP         *spec.MCPExposure
}

// ProjectedComponents returns the cube and optional cube-compose components
// Datly will derive for a grouped reader, using the same identity rules as the
// report compiler.
func ProjectedComponents(component *spec.Component) []ProjectedComponent {
	routes := (Source{Component: component}).eligibleRoutes()
	result := make([]ProjectedComponent, 0, len(routes)*2)
	for _, route := range routes {
		identity, err := (&reportDeriver{}).derivedIdentity(component, route, len(routes))
		if err != nil {
			return nil
		}
		cubeName := identity.key.Name
		exposure := spec.ReportMCPExposure(component.Settings.Report, route, cubeName, component.Description)
		name, description := spec.DerivedMCPToolName(route, cubeName, "Cube"), strings.TrimSpace(component.Description+" cube")
		if exposure != nil {
			name = exposure.Name
			if component.Settings.Report.MCP != nil {
				description = exposure.Description
			}
		}
		result = append(result, ProjectedComponent{Name: name, Method: http.MethodPost, Path: strings.TrimRight(route.Path, "/") + "/cube", MCPEnabled: exposure != nil, Description: description, MCP: exposure})
		compose := component.Settings.Report.Compose
		if compose == nil || !compose.Enabled {
			continue
		}
		composeDescription := strings.TrimSpace(component.Description + " cube composition")
		composeExposure := spec.ComposeMCPExposure(compose, route, cubeName+"Compose", composeDescription)
		composeName := spec.DerivedMCPToolName(route, cubeName+"Compose", "CubeCompose")
		if composeExposure != nil {
			composeName = composeExposure.Name
			if compose.MCP != nil {
				composeDescription = composeExposure.Description
			}
		}
		result = append(result, ProjectedComponent{Name: composeName, Method: http.MethodPost, Path: strings.TrimRight(route.Path, "/") + "/cube/compose", MCPEnabled: composeExposure != nil, Description: composeDescription, MCP: composeExposure})
	}
	return result
}
