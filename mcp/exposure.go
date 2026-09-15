package mcp

import (
	"fmt"
	"strings"

	bindresource "github.com/viant/bindly/resource"
	"github.com/viant/datly/spec"
)

func resolveExposure(component *spec.Component, route *spec.Route, authored *spec.MCPExposure, resources *bindresource.Store) (*spec.MCPExposure, error) {
	result := authored.Clone()
	result.Name = authored.Identity(component, route)
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
