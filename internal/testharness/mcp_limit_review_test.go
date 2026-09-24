package testharness

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	imcp "github.com/viant/datly/internal/client/mcp"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/client"
	xmcp "github.com/viant/xdatly/client/mcp"
)

func TestMCPClientEnforcesConfiguredResponseLimit(t *testing.T) {
	t.Run("json", func(t *testing.T) { testMCPResponseLimit(t, false) })
	t.Run("sse", func(t *testing.T) { testMCPResponseLimit(t, true) })
}

func testMCPResponseLimit(t *testing.T, streaming bool) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Mcp-Session-Id", "bounded-test-session")
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params struct {
				Arguments map[string]any `json:"arguments"`
			} `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(request.ID) == 0 {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		var result any
		switch request.Method {
		case "initialize":
			result = map[string]any{"protocolVersion": "2025-03-26", "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]any{"name": "test", "version": "1"}}
		case "tools/call":
			size := 8192
			if small, _ := request.Params.Arguments["small"].(bool); small {
				size = 1
			}
			result = map[string]any{"content": []any{map[string]any{"type": "text", "text": strings.Repeat("x", size)}}}
		default:
			result = map[string]any{}
		}
		response := map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result}
		if streaming && request.Method == "tools/call" {
			w.Header().Set("Content-Type", "text/event-stream")
			data, _ := json.Marshal(response)
			_, _ = fmt.Fprintf(w, "event: message\ndata: %s\n\n", data)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()
	c, err := imcp.New(xmcp.Options{URL: server.URL, ProtocolVersion: "2025-03-26", Tool: "context", Limits: client.Limits{Timeout: "2s", MaxResponseBytes: 1024}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.CallTool(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "context", Arguments: map[string]any{"small": true}}}, xmcp.CallOptions{}); err != nil {
		t.Fatalf("initial bounded response failed: %v", err)
	}
	_, err = c.CallTool(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "context"}}, xmcp.CallOptions{})
	if err == nil || !strings.Contains(err.Error(), "exceeds 1024 bytes") {
		t.Fatalf("MCP response bound was not enforced: %v", err)
	}
}
