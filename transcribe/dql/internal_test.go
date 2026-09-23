package dql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseComponentSourceInternal(t *testing.T) {
	for _, tc := range []struct {
		name, settings, wantError string
		internal                  bool
	}{
		{name: "default public"},
		{name: "private", settings: `$internal(true)`, internal: true},
		{name: "explicit public", settings: `$internal(false)`},
		{name: "missing value", settings: `$internal()`, wantError: "internal requires"},
		{name: "invalid value", settings: `$internal('private')`, wantError: "internal requires true or false"},
		{name: "expression", settings: `$internal($Private)`, wantError: "internal requires true or false"},
		{name: "modifier", settings: `$internal(true).Other()`, wantError: "without modifiers"},
		{name: "unsupported private", settings: `$private(true)`, wantError: `unsupported setting "private"`},
		{name: "unsupported visibility", settings: `$visibility('private')`, wantError: `unsupported setting "visibility"`},
		{name: "typo", settings: `$interal(true)`, wantError: `unsupported setting "interal"`},
		{name: "malformed", settings: `$internal`, wantError: "invalid setting"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := "#setting($_ = $route('/private/read', 'GET', 'POST'))\n"
			if tc.settings != "" {
				source += "#setting($_ = " + tc.settings + ")\n"
			}
			component, err := parseComponentSource("example.com/private", "Private", source+"SELECT 1 AS id")
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			require.Len(t, component.Routes, 2)
			for _, route := range component.Routes {
				require.Equal(t, tc.internal, route.Internal)
				require.Empty(t, route.MCP)
			}
		})
	}
}

func TestInternalRejectsConflictingExposure(t *testing.T) {
	for _, settings := range []string{
		"#setting($_ = $internal(true))\n#setting($_ = $internal(false))",
		"#setting($_ = $internal(true))\n#setting($_ = $mcp('private'))",
		"#setting($_ = $mcp('private'))\n#setting($_ = $internal(true))",
		"#setting($_ = $internal(true))\n#setting($_ = $mcpOnly(true))\n#setting($_ = $mcp('private'))",
		"#setting($_ = $internal(true))\n#setting($_ = $static_content('file:///tmp/content', '/content'))",
		"#setting($_ = $internal(true))\n#setting($_ = $cube('', '', '', '', '', '', '', true))",
		"#setting($_ = $internal(true))\n#setting($_ = $cubeCompose(true,true))",
	} {
		_, err := parseComponentSource("example.com/private", "Private",
			"#setting($_ = $route('/private/read', 'GET'))\n"+settings+"\nSELECT 1 AS id")
		require.Error(t, err, settings)
	}
}

func TestInternalDisablesImplicitDerivedMCPExposure(t *testing.T) {
	component, err := parseComponentSource("example.com/private", "Private", `
#setting($_ = $route('/private/read','GET'))
#setting($_ = $cube())
#setting($_ = $cubeCompose(true))
#setting($_ = $internal(true))
SELECT 1 AS id`)
	require.NoError(t, err)
	require.NotNil(t, component.Settings.Report.MCPTool)
	require.False(t, *component.Settings.Report.MCPTool)
	require.NotNil(t, component.Settings.Report.Compose.MCPTool)
	require.False(t, *component.Settings.Report.Compose.MCPTool)
}
