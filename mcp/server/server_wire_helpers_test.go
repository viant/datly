package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

type wireResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *jsonrpc.Error  `json:"error"`
}

type wireServer struct {
	server  *Server
	baseURL string
	done    <-chan error
}

func startWireServer(t *testing.T, service ServerService) *wireServer {
	return startConfiguredWireServer(t, service, TransportConfig{Kind: TransportStreamable})
}

func startConfiguredWireServer(t *testing.T, service ServerService, transport TransportConfig, configure ...func(*http.Server)) *wireServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	transport.Address = listener.Addr().String()
	instance, err := New(Config{
		Service:        service,
		Implementation: schema.Implementation{Name: "datly-test", Version: "1.0"},
		Transport:      transport,
	})
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	httpServer, err := instance.HTTP()
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	for _, apply := range configure {
		apply(httpServer)
	}
	done := make(chan error, 1)
	go func() { done <- httpServer.Serve(listener) }()
	result := &wireServer{server: instance, baseURL: "http://" + listener.Addr().String(), done: done}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = instance.Shutdown(ctx)
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("MCP HTTP server did not stop")
		}
	})
	return result
}

func initializeWireSession(t *testing.T, baseURL string) string {
	t.Helper()
	payload := map[string]interface{}{
		"jsonrpc": "2.0", "id": 1, "method": schema.MethodInitialize,
		"params": map[string]interface{}{
			"protocolVersion": schema.LegacyProtocolVersion,
			"capabilities":    map[string]interface{}{},
			"clientInfo":      map[string]interface{}{"name": "test", "version": "1.0"},
		},
	}
	response, header, status := postWireRequest(t, baseURL+"/mcp", payload, "", "")
	if status != http.StatusOK || response.Error != nil {
		t.Fatalf("initialize status=%d error=%+v result=%s", status, response.Error, response.Result)
	}
	sessionID := header.Get("Mcp-Session-Id")
	if sessionID == "" {
		t.Fatal("initialize response has no MCP session ID")
	}
	return sessionID
}

func postWireRequest(t *testing.T, endpoint string, payload interface{}, sessionID, token string) (*wireResponse, http.Header, int) {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	if message, ok := payload.(map[string]interface{}); ok {
		if params, ok := message["params"].(map[string]interface{}); ok {
			if meta, ok := params["_meta"].(map[string]interface{}); ok {
				if version, ok := meta["io.modelcontextprotocol/protocolVersion"].(string); ok {
					request.Header.Set(schema.HeaderProtocolVersion, version)
					if method, ok := message["method"].(string); ok {
						request.Header.Set(schema.HeaderMethod, method)
					}
				}
			}
		}
	}
	if sessionID != "" {
		request.Header.Set("Mcp-Session-Id", sessionID)
	}
	if token != "" {
		request.Header.Set("Authorization", token)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	result := &wireResponse{}
	if response.StatusCode == http.StatusOK && len(body) > 0 {
		if strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
			var dataLines []string
			for _, line := range strings.Split(string(body), "\n") {
				line = strings.TrimSuffix(line, "\r")
				if strings.HasPrefix(line, "data:") {
					dataLines = append(dataLines, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
				}
			}
			body = []byte(strings.Join(dataLines, "\n"))
		}
		if err := json.Unmarshal(body, result); err != nil {
			t.Fatalf("decode JSON-RPC response: %v body=%s", err, body)
		}
	}
	return result, response.Header.Clone(), response.StatusCode
}
