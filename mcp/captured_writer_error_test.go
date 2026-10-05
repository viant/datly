package mcp

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/capturederror"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/mcp-protocol/schema"
)

func TestCapturedNativeWriterMCPFinalizerBodyPolicy(t *testing.T) {
	for _, remap := range []bool{false, true} {
		t.Run(map[bool]string{false: "original", true: "fresh finalizer body"}[remap], func(t *testing.T) {
			registered, h, err := capturederror.New(t, remap)
			require.NoError(t, err)
			native := mcpclient.New(t, runtimeToolService(t, registered), schema.LatestProtocolVersion)
			result, err := native.CallTool(t.Context(), &schema.CallToolRequestParams{Name: "captured.error", Arguments: map[string]any{"data": []any{map[string]any{"id": 7}}}})
			require.NoError(t, err)
			require.NotNil(t, result)
			require.NotNil(t, result.IsError)
			require.True(t, *result.IsError)
			encoded, err := json.Marshal(result)
			require.NoError(t, err)
			var wire struct {
				Content []struct {
					Text string `json:"text"`
				} `json:"content"`
				Structured json.RawMessage `json:"structuredContent"`
			}
			require.NoError(t, json.Unmarshal(encoded, &wire))
			require.Len(t, wire.Content, 1)
			body := `{"data":[{"id":7}],"status":"business"}`
			if remap {
				body = `{"data":[{"id":7}],"status":"finalized"}`
			}
			require.JSONEq(t, body, wire.Content[0].Text)
			require.JSONEq(t, body, string(wire.Structured))
			require.NotContains(t, string(encoded), "PRIVATE")
			require.NotContains(t, string(encoded), "Logger")
			h.AssertState(t)
			evidence := h.Observe()
			require.Equal(t, 1, evidence.Captures)
			require.Equal(t, 0, evidence.Executes)
			require.Equal(t, 1, evidence.Bridges)
			require.Equal(t, 1, evidence.Finalizes)
			require.Same(t, evidence.Canonical, evidence.Finalized)
			require.NotSame(t, evidence.Payload, evidence.Canonical)
			require.Same(t, evidence.Trusted, evidence.Canonical.Logger)
			require.NotSame(t, evidence.Trusted, evidence.Payload.Logger)
			require.Equal(t, "business", evidence.Payload.Status)
			require.True(t, errors.Is(evidence.Err, capturederror.BusinessCause))
		})
	}
}
