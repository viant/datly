package spec

import "strings"

type MCPExposureKind string

const (
	MCPExposureTool             MCPExposureKind = "tool"
	MCPExposureResource         MCPExposureKind = "resource"
	MCPExposureResourceTemplate MCPExposureKind = "resourceTemplate"
)

// MCPExposure describes one explicit protocol exposure of a component route.
type MCPExposure struct {
	Kind            MCPExposureKind `json:"kind"`
	Name            string          `json:"name,omitempty"`
	Description     string          `json:"description,omitempty"`
	DescriptionPath string          `json:"descriptionPath,omitempty"`
	MIMEType        string          `json:"mimeType,omitempty"`
}

func (e *MCPExposure) Clone() *MCPExposure {
	if e == nil {
		return nil
	}
	result := *e
	return &result
}

// Identity returns the stable protocol name for an exposure before an
// executable component contract is materialized.
func (e *MCPExposure) Identity(component *Component, route *Route) string {
	if e == nil || route == nil {
		return ""
	}
	if name := strings.TrimSpace(e.Name); name != "" {
		return name
	}
	if name := strings.TrimSpace(route.Name); name != "" {
		return name
	}
	name := ""
	if component != nil {
		name = strings.TrimSpace(component.Name)
		if name == "" {
			name = strings.TrimSpace(component.Key.Name)
		}
	}
	return name + "." + routeMCPIdentifier(route)
}

func routeMCPIdentifier(route *Route) string {
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
