package invocation

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/xdatly/response"
)

func TestExplicitToolErrorRetainsTypedStatusOutsideUnchangedBody(t *testing.T) {
	bodies := []any{map[string]any{"status": "error", "message": "", "empty": nil, "data": []any{}}, json.RawMessage(`null`), json.RawMessage(`[]`), json.RawMessage(`""`)}
	for _, code := range []int{401, 403, 422, 500} {
		for _, body := range bodies {
			expected, err := json.Marshal(body)
			require.NoError(t, err)
			execution := &Execution{err: &response.Error{Code: code, Payload: body, Cause: errors.New("private diagnostic cause")}}
			result := execution.ToolResult()
			require.Equal(t, code, execution.StatusCode())
			wire, err := json.Marshal(result)
			require.NoError(t, err)
			require.NotContains(t, string(wire), "private diagnostic cause")
			var decoded struct {
				Meta    map[string]any `json:"_meta"`
				Content []struct {
					Text string `json:"text"`
				} `json:"content"`
				Body json.RawMessage `json:"structuredContent"`
			}
			require.NoError(t, json.Unmarshal(wire, &decoded))
			require.Equal(t, string(expected), decoded.Content[0].Text)
			if _, object := body.(map[string]any); object {
				require.Equal(t, string(expected), string(decoded.Body))
			} else {
				require.Empty(t, decoded.Body, "non-object bodies must not acquire structuredContent")
			}
			require.Equal(t, float64(code), decoded.Meta["io.viant/http-status"])
		}
	}
	result := (&Execution{value: map[string]any{"status": "ok"}}).ToolResult()
	require.Nil(t, result.Meta)
}
