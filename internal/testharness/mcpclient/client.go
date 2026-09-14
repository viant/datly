// Package mcpclient runs real viant/mcp client/server transport tests.
package mcpclient

import (
	"context"
	"net"
	"testing"
	"time"

	dserver "github.com/viant/datly/mcp/server"
	upstream "github.com/viant/mcp"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/mcp/client"
	"github.com/viant/mcp/server/auth"
)

// New starts an ephemeral loopback MCP server and initializes the native client.
// Test cleanup owns transport, listener, server and context shutdown.
func New(t testing.TB, service dserver.ServerService, protocolVersion string) *client.Client {
	return (Config{Service: service, ProtocolVersion: protocolVersion}).New(t)
}

// Config selects a fixed service or a generation source for native wire tests.
type Config struct {
	ResourceAuthorizer auth.ResourceAuthorizer
	Service            dserver.ServerService
	Source             dserver.Source
	ProtocolVersion    string
}

func (c Config) New(t testing.TB) *client.Client {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := dserver.New(dserver.Config{Service: c.Service, Source: c.Source, ResourceAuthorizer: c.ResourceAuthorizer, Implementation: schema.Implementation{Name: "datly-test", Version: "1.0"}, Transport: dserver.TransportConfig{Kind: dserver.TransportStreamable, Address: listener.Addr().String()}})
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	httpServer, err := server.HTTP()
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- httpServer.Serve(listener) }()
	ctx, cancel := context.WithCancel(context.Background())
	var native *client.Client
	t.Cleanup(func() {
		if native != nil {
			native.Close()
		}
		cancel()
		// Session protocols keep notification streams open. Force-close test
		// connections before waiting for graceful shutdown of the listener.
		_ = httpServer.Close()
		shutdown, stop := context.WithTimeout(context.Background(), 2*time.Second)
		defer stop()
		if err := server.Shutdown(shutdown); err != nil {
			t.Errorf("MCP shutdown: %v", err)
		}
		select {
		case <-done:
		case <-shutdown.Done():
			t.Error("MCP server did not stop")
		}
	})
	native, err = upstream.NewClientWithContext(ctx, nil, &upstream.ClientOptions{
		Name: "datly-acceptance", Version: "1.0", ProtocolVersion: c.ProtocolVersion,
		Transport: upstream.ClientTransport{Type: "streamable", ClientTransportHTTP: upstream.ClientTransportHTTP{URL: "http://" + listener.Addr().String() + "/mcp"}},
	})
	if err != nil {
		t.Fatalf("initialize native MCP client: %v", err)
	}
	return native
}
