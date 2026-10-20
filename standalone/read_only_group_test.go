package standalone

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/readgroups"
	"github.com/viant/mcp-protocol/schema"
)

func TestReadOnlyDependencyGroupHTTPMCP(t *testing.T) {
	for _, mode := range []string{"eager", "indexed", "linked"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			f := fixture.New(t)
			f.WriteConfig(t, func(c map[string]any) {
				c["GoBootstrap"] = map[string]any{"Packages": []string{fixture.Module + "/readgroups"}, "EagerComponents": mode == "eager", "LinkedOnly": mode == "linked"}
			})
			cfg, err := (config.Loader{}).Load(ctx, f.Config)
			require.NoError(t, err)
			if mode == "linked" {
				cfg.BaseDir = filepath.Join(t.TempDir(), "no-source-tree")
			}
			_ = readgroups.DatlyLinkedType
			server, err := New(ctx, Options{Config: cfg})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })
			require.NoError(t, server.Reload(ctx, 1))
			native := (mcpclient.Config{Source: server.manager}).New(t)
			for _, transport := range []string{"http", "mcp"} {
				t.Run(transport, func(t *testing.T) {
					name := mode + "-" + transport
					entered := make(chan string, 2)
					release := make(chan struct{})
					remove := readgroups.Install(name, &readgroups.Probe{Entered: entered, Release: release})
					defer remove()
					type response struct {
						data []byte
						err  error
					}
					done := make(chan response, 1)
					go func() {
						if transport == "http" {
							r := httptest.NewRecorder()
							server.ServeHTTP(r, httptest.NewRequest("GET", "/read-group?"+url.Values{"case": {name}}.Encode(), nil).WithContext(ctx))
							if r.Code != 200 {
								done <- response{err: fmt.Errorf("HTTP %d: %s", r.Code, r.Body.String())}
								return
							}
							done <- response{data: r.Body.Bytes()}
							return
						}
						result, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: "ReadGroup", Arguments: map[string]any{"case": name}})
						if err == nil && result.IsError != nil && *result.IsError {
							err = fmt.Errorf("MCP error: %+v", result)
						}
						if err != nil {
							done <- response{err: err}
							return
						}
						data, err := json.Marshal(result.StructuredContent)
						done <- response{data: data, err: err}
					}()
					var branches []string
					for range 2 {
						select {
						case branch := <-entered:
							branches = append(branches, branch)
						case early := <-done:
							t.Fatalf("request returned before dependencies overlapped: %s %v", early.data, early.err)
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
					}
					require.ElementsMatch(t, []string{"left", "right"}, branches)
					close(release)
					select {
					case result := <-done:
						require.NoError(t, result.err)
						var out struct {
							Left, Right string
							Data        []readgroups.Row
						}
						require.NoError(t, json.Unmarshal(result.data, &out), string(result.data))
						require.Equal(t, "left:"+name+":1", out.Left)
						require.Equal(t, "right:"+name+":1", out.Right)
						require.Equal(t, []readgroups.Row{{ID: 1, Name: "first"}}, out.Data)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				})
			}
			t.Run("concurrent-request-isolation", func(t *testing.T) {
				var wait sync.WaitGroup
				for i := range 8 {
					wait.Add(1)
					go func() {
						defer wait.Done()
						name := fmt.Sprintf("%s-request-%d", mode, i)
						response := httptest.NewRecorder()
						server.ServeHTTP(response, httptest.NewRequest("GET", "/read-group?"+url.Values{"case": {name}}.Encode(), nil).WithContext(ctx))
						var out struct{ Left, Right string }
						if err := json.Unmarshal(response.Body.Bytes(), &out); err != nil || response.Code != 200 || out.Left != "left:"+name+":1" || out.Right != "right:"+name+":1" {
							t.Errorf("request %s crossed state or failed: status=%d body=%s error=%v", name, response.Code, response.Body.String(), err)
						}
					}()
				}
				wait.Wait()
			})
		})
	}
}
