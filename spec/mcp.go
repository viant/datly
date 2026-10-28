package spec

import (
	"fmt"
	"strings"

	"github.com/viant/tagly/format/text"
)

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
	// CaseFormat customizes implicit MCP input member names only. Empty preserves
	// legacy Go/JSON spelling, independently of the component's HTTP case format.
	CaseFormat string `json:"caseFormat,omitempty"`
}

func (e *MCPExposure) ValidateCaseFormat() error {
	if e != nil && strings.TrimSpace(e.CaseFormat) != "" && !text.NewCaseFormat(e.CaseFormat).IsDefined() {
		return fmt.Errorf("invalid MCP case format %q", e.CaseFormat)
	}
	return nil
}

// DerivedMCPCaseFormat follows the source tool used for derived naming. An
// unnamed exposure still owns its input format when no named tool exists.
func DerivedMCPCaseFormat(route *Route) string {
	var fallback string
	if route != nil {
		for _, exposure := range route.MCP {
			if exposure == nil || exposure.Kind != MCPExposureTool {
				continue
			}
			if strings.TrimSpace(exposure.Name) != "" {
				return exposure.CaseFormat
			}
			if fallback == "" {
				fallback = exposure.CaseFormat
			}
		}
	}
	return fallback
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

// DerivedMCPToolName derives a public MCP name for a synthetic endpoint such
// as a report cube from the source route's authored tool exposure.
func DerivedMCPToolName(route *Route, fallback, suffix string) string {
	if route != nil {
		for _, exposure := range route.MCP {
			if exposure == nil || exposure.Kind != MCPExposureTool || strings.TrimSpace(exposure.Name) == "" {
				continue
			}
			return strings.TrimSpace(exposure.Name) + suffix
		}
	}
	return fallback
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
