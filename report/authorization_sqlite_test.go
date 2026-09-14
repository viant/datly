package report

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	httpgateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/spec"
)

func TestDerivedCubeAuthorizationSQLite(t *testing.T) {
	for _, protected := range []bool{false, true} {
		name := "public"
		if protected {
			name = "protected"
		}
		t.Run(name, func(t *testing.T) {
			h := (groupedReportHarnessConfig{
				skipMCP: true,
				report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) {
					s.Compose = &spec.CubeComposeSettings{Enabled: true}
				}},
				route: func(r *spec.Route) {
					if protected {
						r.APIKeyHeader, r.APIKeyValue = "X-Report-Key", "report-secret"
					}
				},
			}).build(t)
			for _, route := range []struct {
				name, suffix, body string
			}{
				{"cube", "/cube", `{"dimensions":{"accountID":true},"measures":{"totalSpend":true},"filters":{"accountIDs":"1","tenant":"acme","region":"EU","channel":"web","status":"active"}}`},
				{"compose", "/cube/compose", `{"cubes":[{"filters":{"accountIDs":"1","tenant":"acme","region":"EU","channel":"web","status":"active"}}],"sql":"SELECT t1.AccountID, t1.TotalSpend FROM $CubeSQL1 AS t1"}`},
			} {
				for _, credential := range []struct{ name, header, value string }{
					{"missing", "", ""}, {"wrong", "X-Report-Key", "wrong"},
					{"wrong_header", "X-API-Key", "report-secret"}, {"valid", "X-Report-Key", "report-secret"},
				} {
					t.Run(route.name+"/"+credential.name, func(t *testing.T) {
						request := httptest.NewRequest(http.MethodPost, "/spend/acme"+route.suffix, strings.NewReader(route.body))
						request.Header.Set("Content-Type", "application/json")
						if credential.header != "" {
							request.Header.Set(credential.header, credential.value)
						}
						response := httptest.NewRecorder()
						httpgateway.NewHandler(h.runtime, nil, "").ServeHTTP(response, request)
						if protected && credential.name != "valid" {
							if response.Code != http.StatusForbidden || response.Body.String() != `{"status":"error","message":"forbidden","error":"forbidden"}` {
								t.Fatalf("unauthorized response = %d %s", response.Code, response.Body.String())
							}
							return
						}
						if response.Code != http.StatusOK {
							t.Fatalf("authorized response = %d %s", response.Code, response.Body.String())
						}
						var result struct {
							Rows []struct {
								AccountID  int
								TotalSpend float64
							} `json:"rows"`
							Data []struct {
								AccountID  int
								TotalSpend float64
							} `json:"data"`
						}
						if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
							t.Fatal(err)
						}
						rows := result.Rows
						if route.name == "compose" {
							rows = result.Data
						}
						if len(rows) != 1 || rows[0].AccountID != 1 || rows[0].TotalSpend != 150 {
							t.Fatalf("SQLite report result = %s", response.Body.String())
						}
					})
				}
			}
		})
	}
}
