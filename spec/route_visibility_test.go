package spec

import "testing"

func TestMCPRouteIncludesExplicitMCPOnlyRoute(t *testing.T) {
	route := &Route{Internal: true, MCP: []*MCPExposure{{Kind: MCPExposureTool, Name: "records.read"}}}
	if PublicRoute(route) {
		t.Fatal("internal MCP route leaked into public HTTP routes")
	}
	if !MCPRoute(route) {
		t.Fatal("explicit MCP route missing from MCP catalog")
	}
	if MCPRoute(&Route{Internal: true}) {
		t.Fatal("internal route without MCP exposure entered MCP catalog")
	}
}
