package standalone

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/standalone/config"
)

func TestNativeServerStatusEndpoint(t *testing.T) {
	for _, uri := range []string{"", "/service/status"} {
		t.Run(uri, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			root := t.TempDir()
			localRoot, err := os.OpenRoot(root)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, localRoot.Close()) })
			// A static-only server needs no components, connectors, or database.
			require.NoError(t, os.WriteFile(filepath.Join(root, "index.html"), []byte("fixture"), 0600))
			raw, err := json.Marshal(map[string]any{
				"Version":       "native-status-fixture",
				"Endpoint":      map[string]any{"Address": "127.0.0.1:0"},
				"StaticContent": []map[string]any{{"Path": "/ui", "ContentURL": "."}},
				"Meta":          map[string]any{"StatusURI": uri, "AllowedSubnet": []string{"127.0.0.1:"}},
			})
			require.NoError(t, err)
			file := filepath.Join(root, "config.json")
			require.NoError(t, os.WriteFile(file, raw, 0600))
			cfg, err := (config.Loader{StaticLocalRoot: localRoot}).Load(ctx, file)
			require.NoError(t, err)
			server, err := New(ctx, Options{Config: cfg})
			require.NoError(t, err)
			done := make(chan error, 1)
			go func() { done <- server.Serve(ctx, io.Discard) }()
			t.Cleanup(func() {
				cancel()
				require.NoError(t, server.Shutdown(context.Background()))
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					t.Error("native status server did not stop")
				}
			})
			addresses, err := server.WaitReady(ctx)
			require.NoError(t, err)
			require.Len(t, addresses, 1)
			path := uri
			if path == "" {
				path = gateway.DefaultStatusURI
			}
			client := &http.Client{Timeout: 3 * time.Second}
			var initialStart time.Time
			var initialUptime time.Duration
			for attempt := 0; attempt < 2; attempt++ {
				if attempt == 1 {
					require.NoError(t, server.Reload(ctx, server.manager.Revision()+1))
				}
				request, err := http.NewRequestWithContext(ctx, "GET", "http://"+addresses[0]+path, nil)
				require.NoError(t, err)
				response, err := client.Do(request)
				require.NoError(t, err)
				payload, err := io.ReadAll(response.Body)
				response.Body.Close()
				require.NoError(t, err)
				require.Equal(t, 200, response.StatusCode, string(payload))
				require.Equal(t, "application/json", response.Header.Get("Content-Type"))
				var info struct {
					Version, Status, UpTime string
					StartTime               time.Time
				}
				require.NoError(t, json.Unmarshal(payload, &info))
				require.Equal(t, "native-status-fixture", info.Version)
				require.Equal(t, "UP", info.Status)
				require.False(t, info.StartTime.IsZero())
				uptime, err := time.ParseDuration(info.UpTime)
				require.NoError(t, err)
				if attempt == 0 {
					initialStart, initialUptime = info.StartTime, uptime
				} else {
					require.True(t, initialStart.Equal(info.StartTime))
					require.GreaterOrEqual(t, uptime, initialUptime)
				}
			}
		})
	}
}
