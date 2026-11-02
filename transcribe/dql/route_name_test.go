package dql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRouteNamePreservesSharedRouteObservability(t *testing.T) {
	type useCase struct {
		description, input string
		expected           []string
	}
	routes := "#setting($_ = $route('/records/archive','POST'))\n#setting($_ = $route('/records/restore','POST'))\n"
	names := "#setting($_ = $route_name('/records/archive','POST','ArchiveRecords'))\n#setting($_ = $route_name('/records/restore','post','RestoreRecords'))\n"
	for _, tc := range []useCase{
		{description: "existing default", input: routes, expected: []string{"Records", "Records"}},
		{description: "names after routes", input: routes + names, expected: []string{"ArchiveRecords", "RestoreRecords"}},
		{description: "names before routes", input: names + routes, expected: []string{"ArchiveRecords", "RestoreRecords"}},
	} {
		t.Run(tc.description, func(t *testing.T) {
			component, err := parseComponentSource("example.com/records", "Records", tc.input+"SELECT 1")
			require.NoError(t, err)
			require.Len(t, component.Routes, 2)
			for i, route := range component.Routes {
				require.Equal(t, tc.expected[i], route.Name)
				require.Equal(t, "POST", route.Method)
				require.Empty(t, route.MCP)
			}
		})
	}
}

func TestRouteNameDiagnostics(t *testing.T) {
	type useCase struct{ description, input, expected string }
	for _, tc := range []useCase{
		{"missing name", `$route_name('/a','POST')`, "route_name requires"},
		{"empty name", `$route_name('/a','POST','')`, "nonempty name"},
		{"unknown path", `$route_name('/b','POST','B')`, "not a declared route"},
		{"wrong method", `$route_name('/a','GET','A')`, "not a declared route"},
	} {
		t.Run(tc.description, func(t *testing.T) {
			_, err := parseComponentSource("example.com/records", "Records", "#setting($_ = $route('/a','POST'))\n#setting($_ = "+tc.input+")\nSELECT 1")
			require.ErrorContains(t, err, tc.expected)
		})
	}
}
