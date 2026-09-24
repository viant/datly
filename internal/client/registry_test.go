package client

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/viant/xdatly/client"
	xhttp "github.com/viant/xdatly/client/http"
	xmcp "github.com/viant/xdatly/client/mcp"
)

func TestRegistryReusesClientsPerConfigurationIdentity(t *testing.T) {
	registry := NewRegistry()
	defer registry.Close()
	ctx := context.Background()
	options := xhttp.Options{URL: "http://127.0.0.1:9/ctx", Method: http.MethodGet, Header: map[string]string{"X-Client": "a"}}
	first, err := registry.HTTP().Client(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.HTTP().Client(ctx, options)
	if err != nil || first != second {
		t.Fatalf("identical options produced distinct clients: %v", err)
	}
	other := options
	other.Limits = client.Limits{Timeout: "3s"}
	third, err := registry.HTTP().Client(ctx, other)
	if err != nil || third == first {
		t.Fatalf("different limits shared a client: %v", err)
	}
	mcpOptions := xmcp.Options{URL: "http://127.0.0.1:9/mcp", Tool: "ctx"}
	mcpFirst, err := registry.MCP().Client(ctx, mcpOptions)
	if err != nil {
		t.Fatal(err)
	}
	mcpSecond, err := registry.MCP().Client(ctx, mcpOptions)
	if err != nil || mcpFirst != mcpSecond {
		t.Fatalf("identical mcp options produced distinct clients: %v", err)
	}
	if registry.Size() != 3 {
		t.Fatalf("size = %d, want 3 distinct configurations", registry.Size())
	}
}

func TestRegistryValidatesOptionsAndHonorsContext(t *testing.T) {
	registry := NewRegistry()
	defer registry.Close()
	ctx := context.Background()
	for name, options := range map[string]xhttp.Options{
		"relative url":     {URL: "/ctx", Method: http.MethodGet},
		"method":           {URL: "http://127.0.0.1:9/ctx", Method: "TRACE"},
		"negative timeout": {URL: "http://127.0.0.1:9/ctx", Method: http.MethodGet, Limits: client.Limits{Timeout: "-1s"}},
		"dup header":       {URL: "http://127.0.0.1:9/ctx", Method: http.MethodGet, Header: map[string]string{"a": "1", "A": "2"}},
		"userinfo url":     {URL: "http://u:p@127.0.0.1:9/ctx", Method: http.MethodGet},
		"missing host":     {URL: "http:///ctx", Method: http.MethodGet},
		"negative bytes":   {URL: "http://127.0.0.1:9/ctx", Method: http.MethodGet, Limits: client.Limits{MaxResponseBytes: -1}},
	} {
		if _, err := registry.HTTP().Client(ctx, options); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	for name, options := range map[string]xmcp.Options{
		"no tool":          {URL: "http://127.0.0.1:9/mcp"},
		"transport":        {URL: "http://127.0.0.1:9/mcp", Tool: "t", Transport: "websocket"},
		"sessions":         {URL: "http://127.0.0.1:9/mcp", Tool: "t", MaxSessions: -1},
		"negative timeout": {URL: "http://127.0.0.1:9/mcp", Tool: "t", Limits: client.Limits{Timeout: "-1s"}},
		"relative url":     {URL: "mcp", Tool: "t"},
	} {
		if _, err := registry.MCP().Client(ctx, options); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	if registry.Size() != 0 {
		t.Fatalf("invalid options were cached: size = %d", registry.Size())
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := registry.HTTP().Client(canceled, xhttp.Options{URL: "http://127.0.0.1:9/ctx", Method: http.MethodGet}); err == nil {
		t.Fatal("canceled context resolved a client")
	}
}

func TestRegistryCloseIsTerminal(t *testing.T) {
	registry := NewRegistry()
	ctx := context.Background()
	if _, err := registry.HTTP().Client(ctx, xhttp.Options{URL: "http://127.0.0.1:9/ctx", Method: http.MethodGet}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.MCP().Client(ctx, xmcp.Options{URL: "http://127.0.0.1:9/mcp", Tool: "t"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if registry.Size() != 0 {
		t.Fatalf("close retained %d clients", registry.Size())
	}
	if _, err := registry.HTTP().Client(ctx, xhttp.Options{URL: "http://127.0.0.1:9/ctx", Method: http.MethodGet}); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("closed registry rebuilt an http client: %v", err)
	}
	if _, err := registry.MCP().Client(ctx, xmcp.Options{URL: "http://127.0.0.1:9/mcp", Tool: "t"}); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("closed registry rebuilt an mcp client: %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatalf("second close failed: %v", err)
	}
}
