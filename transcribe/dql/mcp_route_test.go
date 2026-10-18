package dql

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMCPRouteExactSelectionAndDeclarationOrder(t *testing.T) {
	routes := "#setting($_ = $route('/archive','PATCH'))\n#setting($_ = $route('/unarchive','PATCH'))\n"
	exposure := "#setting($_ = $mcp_route('patch:/archive','ArchiveCampaign','Archive only','docs/archive.md'))\n"
	for _, source := range []string{exposure + routes, routes + exposure} {
		component, err := parseComponentSource("example.com/app", "Campaign", source+"SELECT 1")
		require.NoError(t, err)
		require.Len(t, component.Routes, 2)
		require.Len(t, component.Routes[0].MCP, 1)
		require.Equal(t, "ArchiveCampaign", component.Routes[0].MCP[0].Name)
		require.Equal(t, "Archive only", component.Routes[0].MCP[0].Description)
		require.Equal(t, "docs/archive.md", component.Routes[0].MCP[0].DescriptionPath)
		require.Empty(t, component.Routes[1].MCP)
	}
}

func TestMCPRouteDistinctMethods(t *testing.T) {
	component, err := parseComponentSource("example.com/app", "SiteList", `#setting($_ = $mcp_route('PUT:/list','SiteListPut','Replace list'))
#setting($_ = $route('/list','PATCH','PUT'))
#setting($_ = $mcp_route('PATCH:/list','SiteListPatch','Patch list'))
SELECT 1`)
	require.NoError(t, err)
	require.Len(t, component.Routes, 2)
	for _, route := range component.Routes {
		require.Len(t, route.MCP, 1)
		if route.Method == "PATCH" {
			require.Equal(t, "SiteListPatch", route.MCP[0].Name)
			require.Equal(t, "Patch list", route.MCP[0].Description)
		} else {
			require.Equal(t, "SiteListPut", route.MCP[0].Name)
			require.Equal(t, "Replace list", route.MCP[0].Description)
		}
	}
}

func TestMCPRouteNewFormDiagnosticsAndGlobalCompatibility(t *testing.T) {
	for _, tc := range []struct{ name, source, want string }{
		{"missing name", `#setting($_ = $mcp_route('GET:/a'))`, "mcp_route requires"},
		{"extra args", `#setting($_ = $mcp_route('GET:/a','a','','','extra'))`, "mcp_route requires"},
		{"modifier", `#setting($_ = $mcp_route('GET:/a','a').Extra())`, "mcp_route requires"},
		{"bad ref", `#setting($_ = $mcp_route('/a','a'))`, "must use METHOD:/path"},
		{"empty name", `#setting($_ = $mcp_route('GET:/a',''))`, "empty name"},
		{"missing route", `#setting($_ = $mcp_route('GET:/missing','a'))`, "not a declared route"},
		{"wrong method", `#setting($_ = $mcp_route('POST:/a','a'))`, "not a declared route"},
		{"path exact", `#setting($_ = $mcp_route('GET:/a/','a'))`, "not a declared route"},
		{"conflict", `#setting($_ = $mcp_route('GET:/a','one'))
#setting($_ = $mcp_route('GET:/a','two'))`, "conflicting mcp_route"},
		{"global conflict", `#setting($_ = $mcp('one'))
#setting($_ = $mcp_route('GET:/a','two'))`, "conflicts with global mcp"},
		{"internal", `#setting($_ = $internal(true))
#setting($_ = $mcp_route('GET:/a','a'))`, "cannot expose MCP"},
		{"global ambiguity", `#setting($_ = $route('/b','GET'))
#setting($_ = $mcp('one'))
#setting($_ = $mcp_route('GET:/a','one'))`, "ambiguous across 2 routes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, PrepareSource("#setting($_ = $route('/a','GET'))\n"+tc.source+"\nSELECT 1").Err(), tc.want)
		})
	}
	component, err := parseComponentSource("example.com/app", "A", "#setting($_ = $route('/a','GET'))\n#setting($_ = $mcp('one','original','docs/original.md','previously ignored'))\nSELECT 1")
	require.NoError(t, err)
	require.Equal(t, "one", component.Routes[0].MCP[0].Name)
	require.Equal(t, "docs/original.md", component.Routes[0].MCP[0].DescriptionPath)
}
