package standalone

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/handlerreport"
	"github.com/viant/mcp-protocol/schema"
)

func TestStandaloneReportScopedOrdering(t *testing.T) {
	for _, eager := range []bool{false, true} {
		t.Run(fmt.Sprintf("eager=%v", eager), func(t *testing.T) {
			ctx := context.Background()
			f := fixture.New(t)
			require.NoError(t, f.DB.ExecStatements(ctx,
				"CREATE TABLE report_facts(country TEXT, region TEXT, site_id INTEGER, amount INTEGER, enabled INTEGER)",
				"INSERT INTO report_facts VALUES ('US','MA',1,2,1),('US','MA',2,3,1),('CA','MA',1,7,1),('ZZ','MA',3,1000,0)",
				"CREATE TABLE report_regions(country TEXT, region TEXT, label TEXT)",
				"CREATE TABLE report_sites(id INTEGER, label TEXT)"))
			f.WriteConfig(t, func(c map[string]any) {
				c["GoBootstrap"] = map[string]any{"Packages": []string{fixture.Module + "/handlerreport"}, "EagerComponents": eager}
			})
			cfg, err := (config.Loader{}).Load(ctx, f.Config)
			require.NoError(t, err)
			exports, err := handlerreport.Exports()
			require.NoError(t, err)
			server, err := New(ctx, Options{Config: cfg, Registry: exports})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })
			require.NoError(t, server.Reload(ctx, 1))
			native := (mcpclient.Config{Source: server.manager}).New(t)
			target := exec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: fixture.Module + "/handlerreport", Name: "NativeReport"}, Route: spec.RouteRef{Method: "GET", Path: "/native-report"}}
			wrongTarget := target
			wrongTarget.Route.Path = "/unrelated"
			_, err = server.InvokeComponent(ctx, exec.ComponentRequest{
				Target: target, Input: &handlerreport.Input{Permit: true},
				ReportOrdering: exec.NewReportOrdering(wrongTarget, "facts", "amount"),
			})
			require.ErrorContains(t, err, "does not match target")
			call := func(method, path, body string) *httptest.ResponseRecorder {
				r := httptest.NewRequest(method, path, strings.NewReader(body))
				r.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				server.ServeHTTP(w, r)
				return w
			}
			body := `{"dimensions":{"country":true},"measures":{"amount":true},"orderBy":["amount DESC"],"limit":1,"filters":{"permit":true}}`
			for _, endpoint := range []struct{ path, tool string }{
				{"/native-report", "NativeReport"}, {"/linked-report", "LinkedReport"}, {"/registry-report", "RegistryReport"},
			} {
				t.Run(endpoint.tool, func(t *testing.T) {
					w := call("POST", endpoint.path+"/cube", body)
					require.Equal(t, 200, w.Code, w.Body.String())
					assertRankedReport(t, w.Body.Bytes())
					var args map[string]any
					require.NoError(t, json.Unmarshal([]byte(body), &args))
					result, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: endpoint.tool + "Cube", Arguments: args})
					require.NoError(t, err)
					require.False(t, result.IsError != nil && *result.IsError, "%+v", result)
					payload, err := json.Marshal(result.StructuredContent)
					require.NoError(t, err)
					assertRankedReport(t, payload)
					w = call("GET", endpoint.path+"?permit=true&orderBy=amount%20DESC", "")
					require.Equal(t, 400, w.Code, w.Body.String())
					for _, order := range []string{"region DESC", "unknown DESC", "amount DESC; SELECT 1"} {
						invalid := strings.Replace(body, "amount DESC", order, 1)
						w = call("POST", endpoint.path+"/cube", invalid)
						require.Equal(t, 400, w.Code, w.Body.String())
						args["orderBy"] = []string{order}
						result, err = native.CallTool(ctx, &schema.CallToolRequestParams{Name: endpoint.tool + "Cube", Arguments: args})
						require.NoError(t, err)
						require.True(t, result.IsError != nil && *result.IsError)
						failure, err := json.Marshal(result.StructuredContent)
						require.NoError(t, err)
						require.NotContains(t, string(failure), "Internal Server Error")
						require.Contains(t, string(failure), "order by")
					}
				})
			}
			// The handler forwards the same selectors, but not the permission.
			w := call("POST", "/unforwarded-report/cube", body)
			require.Equal(t, 400, w.Code, w.Body.String())
			w = call("GET", "/private-report?permit=true", "")
			require.Equal(t, 404, w.Code)
			w = call("POST", "/linked-report/cube", strings.Replace(body, `"permit":true`, `"permit":false`, 1))
			require.Equal(t, 403, w.Code)
			// Offset remains subject to reader policy; a report ordering grant is not
			// blanket selector permission. Existing pagination error status is unchanged.
			w = call("POST", "/native-report/cube", strings.Replace(body, `"limit":1`, `"limit":1,"offset":1`, 1))
			require.NotEqual(t, 200, w.Code)
			var wg sync.WaitGroup
			for i := 0; i < 12; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					method, path, payload, want := "POST", "/linked-report/cube", body, 200
					if i%2 == 0 {
						// Both routes delegate to the same private-reader plan.
						method, path, payload, want = "GET", "/linked-report?permit=true&orderBy=amount%20DESC", "", 400
					}
					w := call(method, path, payload)
					if w.Code != want {
						t.Errorf("concurrent %s: got %d, want %d: %s", path, w.Code, want, w.Body.String())
					}
				}(i)
			}
			wg.Wait()
			// Internal SQL failures remain 500 and do not disclose database details.
			require.NoError(t, f.DB.ExecStatements(ctx, "DROP TABLE report_facts"))
			w = call("POST", "/native-report/cube", body)
			require.Equal(t, 500, w.Code)
			require.Contains(t, w.Body.String(), "Internal Server Error")
			require.NotContains(t, w.Body.String(), "report_facts")
		})
	}
}

func assertRankedReport(t *testing.T, payload []byte) {
	t.Helper()
	var result struct {
		Data []map[string]any `json:"data"`
	}
	require.NoError(t, json.Unmarshal(payload, &result))
	require.Len(t, result.Data, 1)
	require.Equal(t, "CA", result.Data[0]["country"])
	require.Equal(t, float64(7), result.Data[0]["amount"])
	require.NotContains(t, result.Data[0], "region")
	require.NotContains(t, result.Data[0], "siteId")
}
