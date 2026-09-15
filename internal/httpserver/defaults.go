// Package httpserver owns shared listener defaults for HTTP and MCP.
package httpserver

import (
	"net/http"
	"time"
)

// Defaults bounds slow headers and idle keep-alives without limiting active
// response duration. Nonzero values, including negative disable values, survive.
func Defaults(server *http.Server) {
	if server.ReadHeaderTimeout == 0 {
		server.ReadHeaderTimeout = 10 * time.Second
	}
	if server.IdleTimeout == 0 {
		server.IdleTimeout = 120 * time.Second
	}
}
