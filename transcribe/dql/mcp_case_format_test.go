package dql

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMCPInputCaseFormatIsIndependentOfHTTP(t *testing.T) {
	type useCase struct{ description, directive, expected, errorText string }
	for _, tc := range []useCase{
		{"legacy default", `$mcp('Orders')`, "", ""},
		{"global opt in", `$mcp('Orders','List orders','','lu')`, "lu", ""},
		{"route opt in", `$mcp_route('GET:/orders','Orders','List orders','','lc')`, "lc", ""},
		{"invalid global format", `$mcp('Orders','','','typo')`, "", "invalid MCP case format"},
		{"invalid route format", `$mcp_route('GET:/orders','Orders','','','typo')`, "", "invalid MCP case format"},
		{"too many global args", `$mcp('Orders','','','lc','extra')`, "", "mcp requires"},
	} {
		t.Run(tc.description, func(t *testing.T) {
			component, err := parseComponentSource("example.com/app", "Orders", "#setting($_ = $route('/orders','GET'))\n#setting($_ = $case_format('lc'))\n#setting($_ = "+tc.directive+")\nSELECT 1")
			if tc.errorText != "" {
				require.ErrorContains(t, err, tc.errorText)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "lc", component.Settings.CaseFormat)
			require.Equal(t, tc.expected, component.Routes[0].MCP[0].CaseFormat)
		})
	}
}
