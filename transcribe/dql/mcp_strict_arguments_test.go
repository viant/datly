package dql

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMCPStrictArgumentsOptInAndRouteOverride(t *testing.T) {
	for _, tc := range []struct {
		directive                    string
		present, value, rejectHidden bool
	}{
		{`$mcp('records')`, false, false, false},
		{`$mcp('records','','','lc',true)`, true, true, false},
		{`$mcp('records','','','lc',false)`, true, false, false},
		{`$mcp_route('GET:/records','records','','','lc',true)`, true, true, false},
		{`$mcp('records','','','lc',false,true)`, true, false, true},
		{`$mcp_route('GET:/records','records','','','lc',true,false)`, true, true, false},
	} {
		source := `#package('example.com/records')
#setting($_ = $route('/records','GET'))
#setting($_ = ` + tc.directive + `)
SELECT 1 AS id`
		component, err := parseComponentSource("example.com/records", "Records", source)
		require.NoError(t, err)
		require.Len(t, component.Routes, 1)
		require.Len(t, component.Routes[0].MCP, 1)
		require.Equal(t, tc.rejectHidden, component.Routes[0].MCP[0].RejectHiddenInputs)
		strict := component.Routes[0].MCP[0].StrictArguments
		require.Equal(t, tc.present, strict != nil)
		if strict != nil {
			require.Equal(t, tc.value, *strict)
		}
	}
	for _, value := range []string{"1", "'extra'", "null"} {
		_, err := parseMCPExposure([]string{"'records'", "''", "''", "'lc'", value})
		require.ErrorContains(t, err, "true or false")
		_, err = parseMCPExposure([]string{"'records'", "''", "''", "'lc'", "true", value})
		require.ErrorContains(t, err, "hidden-input rejection requires true or false")
	}
}
