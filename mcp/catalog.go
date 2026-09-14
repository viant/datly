package mcp

import (
	"sort"

	mcpresource "github.com/viant/datly/mcp/resource"
	"github.com/viant/datly/mcp/tool"
)

type Catalog struct {
	tools     map[string]*tool.Plan
	names     []string
	resources *mcpresource.Catalog
}

func (c *Catalog) ResourceCatalog() *mcpresource.Catalog {
	if c == nil {
		return nil
	}
	return c.resources
}

func (c *Catalog) Tool(name string) (*tool.Plan, bool) {
	if c == nil {
		return nil, false
	}
	result, ok := c.tools[name]
	return result, ok
}

func (c *Catalog) ToolNames() []string {
	if c == nil {
		return nil
	}
	return append([]string(nil), c.names...)
}

func newCatalog(plans map[string]*tool.Plan, resources *mcpresource.Catalog) *Catalog {
	names := make([]string, 0, len(plans))
	for name := range plans {
		names = append(names, name)
	}
	sort.Strings(names)
	return &Catalog{tools: plans, names: names, resources: resources}
}
