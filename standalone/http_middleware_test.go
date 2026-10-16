package standalone

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
)

func TestHTTPMiddlewareProtectsHTTPAndMCPListeners(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f := fixture.New(t)
	cfg, err := (config.Loader{}).Load(ctx, f.Config)
	require.NoError(t, err)
	cfg.MCP = &config.MCP{Address: "127.0.0.1:0"}
	exports, err := records.Exports()
	require.NoError(t, err)
	var wrappers atomic.Int32
	s, err := New(ctx, Options{Config: cfg, Registry: exports, HTTPMiddleware: func(next http.Handler) http.Handler {
		wrappers.Add(1)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("X-Fixture-Allowed") != "yes" {
				http.Error(w, "denied by host", http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
		})
	}})
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, io.Discard) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, s.Shutdown(context.Background()))
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("middleware host did not stop")
		}
	})
	addresses, err := s.WaitReady(ctx)
	require.NoError(t, err)
	require.Len(t, addresses, 2)
	client := &http.Client{Timeout: 3 * time.Second}
	for _, allowed := range []bool{false, true} {
		for index, address := range addresses {
			path, method, body := "/records/1", "GET", ""
			if index == 1 {
				path, method = "/mcp", "POST"
				body = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2026-07-28","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}}`
			}
			request, err := http.NewRequestWithContext(ctx, method, "http://"+address+path, strings.NewReader(body))
			require.NoError(t, err)
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Accept", "application/json, text/event-stream")
			request.Header.Set("Mcp-Method", "initialize")
			if allowed {
				request.Header.Set("X-Fixture-Allowed", "yes")
			}
			response, err := client.Do(request)
			require.NoError(t, err)
			payload, err := io.ReadAll(response.Body)
			response.Body.Close()
			require.NoError(t, err)
			if !allowed {
				require.Equal(t, http.StatusForbidden, response.StatusCode, string(payload))
			} else {
				require.Equal(t, http.StatusOK, response.StatusCode, string(payload))
			}
		}
	}
	require.EqualValues(t, 2, wrappers.Load(), "middleware must be assembled once per listener")
}
