package spec

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMCPStrictArgumentsCloneAndJSON(t *testing.T) {
	for _, value := range []bool{true, false} {
		exposure := &MCPExposure{Kind: MCPExposureTool, Name: "tool", StrictArguments: &value}
		cloned := exposure.Clone()
		*cloned.StrictArguments = !value
		require.Equal(t, value, *exposure.StrictArguments)
		raw, err := json.Marshal(exposure)
		require.NoError(t, err)
		var restored MCPExposure
		require.NoError(t, json.Unmarshal(raw, &restored))
		require.NotNil(t, restored.StrictArguments)
		require.Equal(t, value, *restored.StrictArguments)
	}
	raw, err := json.Marshal(&MCPExposure{Kind: MCPExposureTool, Name: "tool"})
	require.NoError(t, err)
	require.NotContains(t, string(raw), "strictArguments")
}
