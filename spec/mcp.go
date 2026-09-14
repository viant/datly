package spec

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
