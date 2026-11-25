package standalone

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
)

func TestStandaloneMCPRequestBodyLimit(t *testing.T) {
	const defaultLimit = 4 << 20
	for _, tc := range []struct {
		name       string
		configured int64
		limit      int
	}{
		{name: "omitted keeps native bound", limit: defaultLimit},
		{name: "trusted opt-in expands native bound", configured: 8 << 20, limit: 8 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fixture.New(t)
			f.WriteConfig(t, func(c map[string]any) {
				mcp := map[string]any{"Address": "127.0.0.1:0"}
				if tc.configured != 0 {
					mcp["MaxRequestBodyBytes"] = tc.configured
				}
				c["MCP"] = mcp
				c["Endpoint"] = map[string]any{"Address": "127.0.0.1:0"}
			})
			cfg, err := (config.Loader{}).Load(t.Context(), f.Config)
			require.NoError(t, err)
			exports, err := records.Exports()
			require.NoError(t, err)
			server, err := New(t.Context(), Options{Config: cfg, Registry: exports})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })
			require.NoError(t, server.prepare(t.Context()))
			require.Len(t, server.servers, 2)

			send := func(size int) *httptest.ResponseRecorder {
				const payload = `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`
				request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(payload+strings.Repeat(" ", size-len(payload))))
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Accept", "application/json, text/event-stream")
				result := httptest.NewRecorder()
				server.servers[1].Handler.ServeHTTP(result, request)
				return result
			}
			require.NotEqual(t, http.StatusRequestEntityTooLarge, send(tc.limit).Code)
			tooLarge := send(tc.limit + 1)
			require.Equal(t, http.StatusRequestEntityTooLarge, tooLarge.Code)
			require.Contains(t, tooLarge.Body.String(), "MCP request body is too large")
		})
	}
}
