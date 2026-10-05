package http

import (
	"fmt"
	stdhttp "net/http"
)

// ValidatePathSemantics rejects unsupported HTTP route policies before serving.
func (c Config) ValidatePathSemantics() error {
	switch c.PathSemantics {
	case "", "escaped", "decoded":
		return nil
	default:
		return fmt.Errorf("PathSemantics must be escaped or decoded")
	}
}

// RoutingPath returns the escaped lookup path under the configured HTTP policy.
// It never changes URL, RawPath, RawQuery or RequestURI on the original request.
func (c Config) RoutingPath(request *stdhttp.Request) string {
	if request == nil || request.URL == nil {
		return ""
	}
	if c.PathSemantics != "decoded" {
		return request.URL.EscapedPath()
	}
	effective := *request.URL
	effective.RawPath = ""
	return effective.EscapedPath()
}

func (h *Handler) routingPath(request *stdhttp.Request) string {
	return (Config{PathSemantics: h.pathSemantics}).RoutingPath(request)
}
