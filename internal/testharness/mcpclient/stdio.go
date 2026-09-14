package mcpclient

import (
	"context"
	upstream "github.com/viant/mcp"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/mcp/client"
	"testing"
	"time"
)

// Stdio starts a trusted fixture binary using the native MCP client (no shell).
func Stdio(t testing.TB, binary string, args ...string) *client.Client {
	return (StdioConfig{Binary: binary, Args: args, ProtocolVersion: schema.LatestProtocolVersion}).New(t)
}

type StdioConfig struct {
	Binary          string
	Args            []string
	ProtocolVersion string
}

func (c StdioConfig) New(t testing.TB) *client.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	native, err := upstream.NewClientWithContext(ctx, nil, &upstream.ClientOptions{Name: "datly-stdio-test", Version: "1", ProtocolVersion: c.ProtocolVersion, Transport: upstream.ClientTransport{Type: "stdio", ClientTransportStdio: upstream.ClientTransportStdio{Command: c.Binary, Arguments: c.Args}}})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { native.Close(); cancel() })
	return native
}
