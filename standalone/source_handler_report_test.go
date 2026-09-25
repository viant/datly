package standalone

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/handlerreport"
	"github.com/viant/mcp-protocol/schema"
)

func TestStandaloneHandlerReportLookupMetadata(t *testing.T) {
	for _, eager := range []bool{false, true} {
		t.Run(fmt.Sprintf("eager=%v", eager), func(t *testing.T) {
			ctx := context.Background()
			f := fixture.New(t)
			require.NoError(t, f.DB.ExecStatements(ctx,
				"CREATE TABLE report_facts(country TEXT, region TEXT, site_id INTEGER, amount INTEGER, enabled INTEGER)",
				"INSERT INTO report_facts VALUES ('US','MA',1,2,1),('US','MA',2,3,1),('CA','MA',1,7,1)",
				"CREATE TABLE report_regions(country TEXT, region TEXT, label TEXT)",
				"INSERT INTO report_regions VALUES ('US','MA','Massachusetts'),('CA','MA','Canadian region')",
				"CREATE TABLE report_sites(id INTEGER, label TEXT)",
				"INSERT INTO report_sites VALUES (1,'First'),(2,'Second')"))
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
			list, err := native.ListTools(ctx, nil)
			require.NoError(t, err)
			encoded, err := json.Marshal(list)
			require.NoError(t, err)
			require.Contains(t, string(encoded), "LinkedReportCube")
			require.Contains(t, string(encoded), "RegistryReportCube")
			require.NotContains(t, string(encoded), "PrivateReport")
			private := httptest.NewRecorder()
			server.ServeHTTP(private, httptest.NewRequest("GET", "/private-report?permit=true", nil))
			require.Equal(t, 404, private.Code)

			for _, shape := range []struct{ name, dims string }{
				{"region", `{"country":true,"region":true}`},
				{"site", `{"siteId":true}`},
				{"measure", `{}`},
			} {
				if shape.name == "measure" {
					// Prove omitted lookups are not executed, not merely hidden.
					require.NoError(t, f.DB.ExecStatements(ctx, "DROP TABLE report_regions", "DROP TABLE report_sites"))
				}
				for _, linkage := range []struct{ path, tool string }{{"/registry-report", "RegistryReportCube"}, {"/linked-report", "LinkedReportCube"}} {
					body := `{"dimensions":` + shape.dims + `,"measures":{"amount":true},"filters":{"permit":true}}`
					t.Run(shape.name+linkage.path, func(t *testing.T) {
						request := httptest.NewRequest("POST", linkage.path+"/cube", strings.NewReader(body))
						request.Header.Set("Content-Type", "application/json")
						response := httptest.NewRecorder()
						server.ServeHTTP(response, request)
						require.Equal(t, 200, response.Code, response.Body.String())
						assertHandlerReportRows(t, response.Body.Bytes(), shape.name)
						var args map[string]any
						require.NoError(t, json.Unmarshal([]byte(body), &args))
						result, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: linkage.tool, Arguments: args})
						require.NoError(t, err)
						require.False(t, result.IsError != nil && *result.IsError, "%+v", result)
						payload, err := json.Marshal(result.StructuredContent)
						require.NoError(t, err)
						assertHandlerReportRows(t, payload, shape.name)
					})
				}
			}
			for _, path := range []string{"/registry-report", "/linked-report"} {
				request := httptest.NewRequest("POST", path+"/cube", strings.NewReader(`{"measures":{"amount":true},"filters":{"permit":false}}`))
				request.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				server.ServeHTTP(response, request)
				require.Equal(t, 403, response.Code, response.Body.String())
			}
		})
	}
}

func assertHandlerReportRows(t *testing.T, payload []byte, shape string) {
	t.Helper()
	var result struct {
		Data    []map[string]any `json:"data"`
		Handled bool             `json:"handled"`
	}
	require.NoError(t, json.Unmarshal(payload, &result))
	require.True(t, result.Handled, string(payload))
	if shape == "measure" {
		require.Equal(t, []map[string]any{{"amount": float64(12)}}, result.Data)
		return
	}
	require.Len(t, result.Data, 2, string(payload))
	for _, row := range result.Data {
		if shape == "region" {
			require.NotContains(t, row, "site")
			lookup, ok := row["countryRegion"].(map[string]any)
			require.True(t, ok, string(payload))
			require.Equal(t, row["country"], lookup["country"])
			require.Equal(t, row["region"], lookup["region"])
			if row["country"] == "US" {
				require.Equal(t, float64(5), row["amount"])
				require.Equal(t, "Massachusetts", lookup["label"])
			} else {
				require.Equal(t, float64(7), row["amount"])
				require.Equal(t, "Canadian region", lookup["label"])
			}
		} else {
			require.NotContains(t, row, "countryRegion")
			lookup, ok := row["site"].(map[string]any)
			require.True(t, ok, string(payload))
			require.Equal(t, row["siteId"], lookup["id"])
			if row["siteId"] == float64(1) {
				require.Equal(t, float64(9), row["amount"])
				require.Equal(t, "First", lookup["label"])
			} else {
				require.Equal(t, float64(3), row["amount"])
				require.Equal(t, "Second", lookup["label"])
			}
		}
	}
}
