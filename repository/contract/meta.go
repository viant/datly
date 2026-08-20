package contract

import (
	"net/http"
	"strings"
)

// MCP Model Configuration Protocol path integration
type Meta struct {
	Name           string `json:",omitempty" yaml:"Name"`        // name of the MCP
	Description    string `json:",omitempty" yaml:"Description"` // optional description for documentation purposes
	DescriptionURI string `json:",omitempty" yaml:"DescriptionURI"`
}

type ModelContextProtocol struct {
	MCPTool             bool `json:",omitempty" yaml:"MCPTool"`
	MCPResource         bool `json:",omitempty" yaml:"MCPResource"`
	MCPTemplateResource bool `json:",omitempty" yaml:"MCPTemplateResource"`
}

func (m *ModelContextProtocol) HasMCPIntegration() bool {
	return m != nil && (m.MCPTool || m.MCPResource || m.MCPTemplateResource)
}

func (m *Meta) Build(name string, from string, aPath *Path) *Meta {
	ret := &Meta{
		Name:        m.Name,
		Description: m.Description,
	}
	if ret.Name == "" {
		ret.Name = name
	}
	if ret.Description == "" {
		if aPath.Method == http.MethodGet {
			ret.Description = "Query data from " + name + " view; source: " + from
		} else {
			ret.Description = "Handles data in " + name + " view; destination: " + from
		}
	}
	ret.Name = strings.ReplaceAll(ret.Name, "#", "")
	return ret

}
