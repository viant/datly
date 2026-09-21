package report

import (
	"net/http"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
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
	if component == nil || component.Settings == nil || component.Settings.Report == nil || !component.Settings.Report.Enabled || component.RootView == nil || component.RootView.Groupable == nil || !*component.RootView.Groupable {
		return nil
	}
	var routes []*spec.Route
	for _, route := range component.Routes {
		if route != nil && strings.EqualFold(route.Method, http.MethodGet) {
			routes = append(routes, route)
		}
	}
	result := make([]ProjectedComponent, 0, len(routes)*2)
	for _, route := range routes {
		suffix := ""
		if len(routes) > 1 {
			suffix = typecatalog.ExportedFieldName(route.Name)
		}
		cubeName := typecatalog.ExportedFieldName(component.Key.Name + suffix + "Cube")
		cubeMCP := component.Settings.Report.MCPTool == nil || *component.Settings.Report.MCPTool
		result = append(result, ProjectedComponent{Name: cubeName, Method: http.MethodPost, Path: strings.TrimRight(route.Path, "/") + "/cube", MCPEnabled: cubeMCP, Description: strings.TrimSpace(component.Description + " cube")})
		compose := component.Settings.Report.Compose
		if compose == nil || !compose.Enabled {
			continue
		}
		composeMCP := compose.MCPTool == nil || *compose.MCPTool
		result = append(result, ProjectedComponent{Name: cubeName + "Compose", Method: http.MethodPost, Path: strings.TrimRight(route.Path, "/") + "/cube/compose", MCPEnabled: composeMCP, Description: strings.TrimSpace(component.Description + " cube composition")})
	}
	return result
}
