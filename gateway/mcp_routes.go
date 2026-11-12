package gateway

import (
	"fmt"
	"github.com/viant/datly/repository/contract"
	dpath "github.com/viant/datly/repository/path"
	"strings"
)

func declaredMCPRoute(item *dpath.Item, current *dpath.Path) (*contract.Meta, error) {
	declared := map[string]bool{}
	if item != nil {
		for _, path := range item.Paths {
			if path != nil {
				declared[path.Method+":"+path.URI] = true
			}
		}
	}
	declared[current.Method+":"+current.URI] = true
	names := map[string]bool{}
	for target, meta := range current.MCPRoutes {
		if !declared[target] {
			return nil, fmt.Errorf("MCP target %q is not a declared method/path", target)
		}
		if meta == nil || strings.TrimSpace(meta.Name) == "" {
			return nil, fmt.Errorf("MCP target %q requires a tool name", target)
		}
		if names[meta.Name] {
			return nil, fmt.Errorf("duplicate MCP route tool name %q", meta.Name)
		}
		names[meta.Name] = true
	}
	return current.MCPRoutes[current.Method+":"+current.URI], nil
}
