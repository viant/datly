package contract

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestModelContextProtocolSerializesExplicitFalseMCPTool(t *testing.T) {
	jsonEncoded, err := json.Marshal(&ModelContextProtocol{MCPTool: false})
	require.NoError(t, err)
	assert.Contains(t, string(jsonEncoded), `"MCPTool":false`)

	encoded, err := yaml.Marshal(&ModelContextProtocol{MCPTool: false})
	require.NoError(t, err)
	assert.Contains(t, string(encoded), "MCPTool: false")
}
