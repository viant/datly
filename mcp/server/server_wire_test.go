package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

func TestHTTPPreservesToolProtocolErrorsThroughLocalWrapper(t *testing.T) {
	wire := startWireServer(t, newTransportTestService(nil))
	sessionID := initializeWireSession(t, wire.baseURL)
	response, _, status := postWireRequest(t, wire.baseURL+"/mcp", map[string]interface{}{
		"jsonrpc": "2.0", "id": 2, "method": schema.MethodToolsCall,
		"params": map[string]interface{}{"name": "missing", "arguments": map[string]interface{}{}},
	}, sessionID, "")
	if status != http.StatusOK || response.Error == nil || response.Error.Code != jsonrpc.MethodNotFound {
		t.Fatalf("status=%d error=%+v result=%s", status, response.Error, response.Result)
	}
}

func TestStdioLifecycleStopsOnCancellation(t *testing.T) {
	instance, err := New(Config{Service: newTransportTestService(nil)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := instance.HTTP(); err == nil {
		t.Fatal("stdio server exposed HTTP transport")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := instance.Serve(ctx); err != nil {
		t.Fatalf("canceled stdio server returned %v", err)
	}
}

func TestHTTPRootRedirectUsesPreferredTransport(t *testing.T) {
	tests := []struct {
		kind     TransportKind
		location string
	}{
		{kind: TransportSSE, location: "/sse"},
		{kind: TransportStreamable, location: "/mcp"},
	}
	for _, test := range tests {
		t.Run(string(test.kind), func(t *testing.T) {
			wire := startConfiguredWireServer(t, newTransportTestService(nil), TransportConfig{Kind: test.kind, RootRedirect: true})
			client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
			response, err := client.Get(wire.baseURL + "/")
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusTemporaryRedirect || response.Header.Get("Location") != test.location {
				t.Fatalf("status=%d location=%q", response.StatusCode, response.Header.Get("Location"))
			}
		})
	}
}

func TestHTTPServeStopsOnContextCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	instance, err := New(Config{
		Service:   newTransportTestService(nil),
		Transport: TransportConfig{Kind: TransportStreamable, Address: address},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- instance.Serve(ctx) }()
	waitForHTTP(t, "http://"+address+"/")
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve() after cancellation = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve() did not stop after context cancellation")
	}
}

func waitForHTTP(t *testing.T, endpoint string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get(endpoint)
		if err == nil {
			response.Body.Close()
			return
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(10 * time.Millisecond)
		}
	}
	t.Fatalf("MCP HTTP server did not start at %s", endpoint)
}

func TestNewRejectsInvalidTransportConfiguration(t *testing.T) {
	service := newTransportTestService(nil)
	tests := []Config{
		{},
		{Service: service, Transport: TransportConfig{Kind: "invalid"}},
		{Service: service, Transport: TransportConfig{Kind: TransportStreamable, SSEURI: "/same", StreamableURI: "/same"}},
		{Service: service, Transport: TransportConfig{Kind: TransportSSE, SSEURI: "https://example.com/sse"}},
		{Service: service, Transport: TransportConfig{Kind: TransportSSE, SSEURI: "//host/sse"}},
		{Service: service, Transport: TransportConfig{Kind: TransportStreamable, StreamableURI: "//host/mcp"}},
	}
	for index, config := range tests {
		if server, err := New(config); err == nil || server != nil {
			t.Fatalf("case %d server=%v error=%v", index, server, err)
		}
	}
}
