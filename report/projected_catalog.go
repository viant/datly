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
		cubeMCP := component.Settings.Report.MCPTool == nil || *component.Settings.Report.MCPTool
		result = append(result, ProjectedComponent{Name: spec.DerivedMCPToolName(route, cubeName, "Cube"), Method: http.MethodPost, Path: strings.TrimRight(route.Path, "/") + "/cube", MCPEnabled: cubeMCP, Description: strings.TrimSpace(component.Description + " cube")})
		compose := component.Settings.Report.Compose
		if compose == nil || !compose.Enabled {
			continue
		}
		composeMCP := compose.MCPTool == nil || *compose.MCPTool
		result = append(result, ProjectedComponent{Name: spec.DerivedMCPToolName(route, cubeName+"Compose", "CubeCompose"), Method: http.MethodPost, Path: strings.TrimRight(route.Path, "/") + "/cube/compose", MCPEnabled: composeMCP, Description: strings.TrimSpace(component.Description + " cube composition")})
	}
	return result
}
