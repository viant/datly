package gateway

import (
	"context"
	"net/http"
	"strings"
)

type mcpHeadersKey struct{}

// WithMCPHeaders attaches an immutable copy of the current transport headers.
// Native MCP hosts call this at their HTTP boundary before invoking the registry.
// It never replaces the authenticated token carried by the MCP server context.
func WithMCPHeaders(ctx context.Context, headers http.Header) context.Context {
	return context.WithValue(ctx, mcpHeadersKey{}, headers.Clone())
}
func applyMCPHeaders(ctx context.Context, request *http.Request) {
	headers, _ := ctx.Value(mcpHeadersKey{}).(http.Header)
	for name, values := range headers {
		switch strings.ToLower(name) {
		case "content-type", "content-length", "transfer-encoding", "connection", "upgrade", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer":
			continue
		}
		request.Header[name] = append([]string(nil), values...)
	}
}
