package dql

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMCPStrictArgumentsOptInAndRouteOverride(t *testing.T) {
	for _, tc := range []struct {
		directive      string
		present, value bool
	}{
		{`$mcp('records')`, false, false},
		{`$mcp('records','','','lc',true)`, true, true},
		{`$mcp('records','','','lc',false)`, true, false},
		{`$mcp_route('GET:/records','records','','','lc',true)`, true, true},
	} {
		source := `#package('example.com/records')
#setting($_ = $route('/records','GET'))
#setting($_ = ` + tc.directive + `)
SELECT 1 AS id`
		component, err := parseComponentSource("example.com/records", "Records", source)
		require.NoError(t, err)
		require.Len(t, component.Routes, 1)
		require.Len(t, component.Routes[0].MCP, 1)
		strict := component.Routes[0].MCP[0].StrictArguments
		require.Equal(t, tc.present, strict != nil)
		if strict != nil {
			require.Equal(t, tc.value, *strict)
		}
	}
	for _, value := range []string{"1", "'extra'", "null"} {
		_, err := parseMCPExposure([]string{"'records'", "''", "''", "'lc'", value})
		require.ErrorContains(t, err, "true or false")
	}
}
