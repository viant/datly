package invocation

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/stretchr/testify/require"
	"github.com/viant/mcp-protocol/schema"
	"testing"
)

type explicitResult struct {
	Content []schema.CallToolResultContentElem `json:"content"`
	Secret  string                             `json:"secret"`
}

func (*explicitResult) MCPToolResult() {}

func TestTypedToolResultUsesProjectedPayload(t *testing.T) {
	value := &explicitResult{Content: []schema.CallToolResultContentElem{schema.TextContent{Type: "text", Text: "private raw text"}}, Secret: "private field"}
	calls := 0
	execution := &Execution{value: value, encodingContext: t.Context(), encodeOutput: func(_ context.Context, actual any) ([]byte, error) {
		calls++
		require.Same(t, value, actual)
		return []byte(`{"resultType":"complete","content":[{"type":"text","text":"projected text"},{"type":"resource","resource":{"uri":"example://file.csv","mimeType":"text/csv","text":"id\n1\n"}}]}`), nil
	}}
	result := execution.ToolResult()
	require.Equal(t, 1, calls)
	require.Nil(t, result.StructuredContent)
	require.Equal(t, "projected text", result.Content[0].(map[string]any)["text"])
	wire, err := json.Marshal(result)
	require.NoError(t, err)
	require.NotContains(t, string(wire), "private")
	resource := result.Content[1].(map[string]any)["resource"].(map[string]any)
	require.Equal(t, "example://file.csv", resource["uri"])
	require.Equal(t, "id\n1\n", resource["text"])
}

func TestTypedToolResultDoesNotSniffObjectsOrBypassFailures(t *testing.T) {
	ordinary := &Execution{value: map[string]any{"content": []any{map[string]any{"type": "text", "text": "ordinary"}}}}
	result := ordinary.ToolResult()
	require.NotNil(t, result.StructuredContent)
	require.Contains(t, result.Content[0].(schema.TextContent).Text, `"content"`)
	explicit := &Execution{value: &explicitResult{}, encodeOutput: func(context.Context, any) ([]byte, error) { return nil, errors.New("private encoding failure") }}
	result = explicit.ToolResult()
	require.NotNil(t, result.IsError)
	require.True(t, *result.IsError)
	wire, err := json.Marshal(result)
	require.NoError(t, err)
	require.NotContains(t, string(wire), "private encoding failure")
}
