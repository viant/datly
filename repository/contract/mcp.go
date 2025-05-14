package contract

// MCP Model Configuration Protocol path integration
type Meta struct {
	Name        string `json:",omitempty" yaml:"Name"`        // name of the MCP
	Description string `json:",omitempty" yaml:"Description"` // optional description for documentation purposes
}

type ModelContextProtocol struct {
	MCPTool     bool `json:",omitempty" yaml:"MCPTool"`
	MCPResource bool `json:",omitempty" yaml:"MCPResource"`
}

func (m *ModelContextProtocol) HasMCPIntegration() bool {
	return m != nil && (m.MCPTool || m.MCPResource)
}
