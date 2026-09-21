package dql

import "testing"

func TestParseComponentSource_MCPOnlyRoute(t *testing.T) {
	source := `#package('example.com/demo/records')
#setting($_ = $route('/records', 'GET'))
#setting($_ = $mcp('records.read'))
#setting($_ = $mcpOnly(true))
SELECT 1 AS id`
	component, err := parseComponentSource("example.com/demo/records", "Records", source)
	if err != nil {
		t.Fatal(err)
	}
	if len(component.Routes) != 1 || !component.Routes[0].Internal || len(component.Routes[0].MCP) != 1 || component.Routes[0].MCP[0].Name != "records.read" {
		t.Fatalf("route=%+v", component.Routes)
	}
	if _, err = parseComponentSource("example.com/demo/records", "Records", `#setting($_ = $route('/records', 'GET'))
#setting($_ = $mcpOnly(true))
SELECT 1`); err == nil {
		t.Fatal("mcpOnly without MCP exposure accepted")
	}
}
