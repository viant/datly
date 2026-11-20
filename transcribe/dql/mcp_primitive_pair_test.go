package dql

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"testing"
)

func TestMCPPrimitivePairExposure(t *testing.T) {
	source := `#package('example.com/records')
#setting($_ = $route('/records','PATCH'))
#setting($_ = $mcp_primitive_pair('data.items','ids','removeIds'))
#setting($_ = $mcp('records'))
SELECT 1 AS id`
	component, err := parseComponentSource("example.com/records", "Records", source)
	require.NoError(t, err)
	exposure := component.Routes[0].MCP[0]
	require.Equal(t, []spec.MCPPrimitivePair{{Path: "data.items", Add: "ids", Remove: "removeIds"}}, exposure.PrimitivePairs)
	require.Nil(t, exposure.StrictArguments)
}
