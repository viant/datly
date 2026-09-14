package mcp

import (
	"fmt"
	"strings"

	bindresource "github.com/viant/bindly/resource"
	"github.com/viant/datly/spec"
)

func resolveExposure(component *spec.Component, route *spec.Route, authored *spec.MCPExposure, resources *bindresource.Store) (*spec.MCPExposure, error) {
	result := authored.Clone()
	result.Name = strings.TrimSpace(result.Name)
	if result.Name == "" {
		result.Name = strings.TrimSpace(route.Name)
	}
	if result.Name == "" {
		componentName := strings.TrimSpace(component.Name)
		if componentName == "" {
			componentName = component.Key.Name
		}
		result.Name = componentName + "." + routeIdentifier(route)
	}
	result.Description = strings.TrimSpace(result.Description)
	if path := strings.TrimSpace(result.DescriptionPath); path != "" {
		if resources == nil {
			return nil, fmt.Errorf("MCP exposure %q description resource store is required", result.Name)
		}
		content, err := resources.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("MCP exposure %q description: %w", result.Name, err)
		}
		detail := strings.TrimSpace(string(content))
		if result.Description == "" {
			result.Description = detail
		} else if detail != "" {
			result.Description += "\n\n" + detail
		}
	}
	return result, nil
}

// routeIdentifier is the sole formatter for default MCP route identifiers.
func routeIdentifier(route *spec.Route) string {
	input := strings.ToLower(strings.TrimSpace(route.Method)) + "_" + strings.TrimSpace(route.Path)
	var result strings.Builder
	underscore := false
	for _, char := range input {
		valid := char >= 'a' && char <= 'z' || char >= '0' && char <= '9'
		if valid {
			result.WriteRune(char)
			underscore = false
			continue
		}
		if result.Len() > 0 && !underscore {
			result.WriteByte('_')
			underscore = true
		}
	}
	return strings.Trim(result.String(), "_")
}
