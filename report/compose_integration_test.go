package report

import (
	"context"
	"encoding/json"
	httpgateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCubeComposeHTTPAndMCP(t *testing.T) {
	h := newGroupedReportHarness(t, false, func(s *spec.ReportSettings) { s.Compose = &spec.CubeComposeSettings{Enabled: true} })
	request := map[string]any{
		"cubes": []any{
			map[string]any{"filters": map[string]any{"accountIDs": "1,2", "tenant": "acme", "region": "EU", "channel": "web", "status": "active"}},
			map[string]any{"inheritFrom": 1, "filters": map[string]any{"channel": "store"}},
		},
		"sql": "SELECT t1.AccountID, t1.TotalSpend AS web, COALESCE(t2.TotalSpend, 0) AS store FROM $CubeSQL1 AS t1 LEFT JOIN $CubeSQL2 AS t2 ON t1.AccountID = t2.AccountID AND t1.Region = t2.Region ORDER BY t1.AccountID LIMIT 8",
	}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	httpRequest := httptest.NewRequest("POST", "/spend/acme/cube/compose?accountIDs=999", strings.NewReader(string(body)))
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("X-Region", "US")
	httpgateway.NewHandler(h.runtime, nil, "").ServeHTTP(recorder, httpRequest)
	if recorder.Code != 200 {
		t.Fatalf("HTTP %d: %s", recorder.Code, recorder.Body.String())
	}
	var result struct {
		Data []struct {
			AccountID int
			Web       float64 `json:"web"`
			Store     float64 `json:"store"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Data) != 2 || result.Data[0].Web != 150 || result.Data[0].Store != 2000 || result.Data[1].Web != 70 || result.Data[1].Store != 30 {
		t.Fatalf("unexpected composition: %s", recorder.Body.String())
	}
	tool, ok := h.service.Registry().ToolRegistry.Get("SpendCubeCompose")
	if !ok {
		t.Fatal("composition MCP tool missing")
	}
	actual, protocolErr := tool.Handler(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: request}})
	if protocolErr != nil || actual == nil || actual.IsError != nil && *actual.IsError {
		t.Fatalf("MCP result=%+v error=%v", actual, protocolErr)
	}
}

func TestCubeComposeCombinedDialectBudgetTransports(t *testing.T) {
	h := newGroupedReportHarness(t, false, func(s *spec.ReportSettings) { s.Compose = &spec.CubeComposeSettings{Enabled: true} })
	dialect, err := h.sql.Dialect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Each frame fits independently; two exceed the real source dialect cap.
	ids := strings.Repeat("1,", dialect.MaxPlaceholderCount()/2) + "1"
	frame := map[string]any{"filters": map[string]any{"accountIDs": ids, "tenant": "acme"}}
	for _, test := range []struct {
		name  string
		count int
		fail  bool
	}{
		{"one frame fits", 1, false}, {"two frames exceed", 2, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			frames := []any{frame}
			SQL := "SELECT t1.AccountID FROM $CubeSQL1 AS t1"
			if test.count == 2 {
				frames = append(frames, frame)
				SQL += " JOIN $CubeSQL2 AS t2 ON t1.AccountID = t2.AccountID"
			}
			arguments := map[string]any{"cubes": frames, "sql": SQL}
			body, err := json.Marshal(arguments)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest("POST", "/spend/acme/cube/compose", strings.NewReader(string(body)))
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			httpgateway.NewHandler(h.runtime, nil, "").ServeHTTP(recorder, request)
			if test.fail {
				if recorder.Code != 500 || strings.Contains(recorder.Body.String(), "exceeding dialect limit") || !strings.Contains(recorder.Body.String(), "Internal Server Error") {
					t.Fatalf("expected composed budget error, HTTP %d: %s", recorder.Code, recorder.Body.String())
				}
			} else if recorder.Code != 200 {
				t.Fatalf("valid single frame HTTP %d: %s", recorder.Code, recorder.Body.String())
			}
			for _, protocol := range []struct{ name, version string }{
				{"MCP session", schema.LegacyProtocolVersion},
				{"MCP stateless", schema.LatestProtocolVersion},
			} {
				t.Run(protocol.name, func(t *testing.T) {
					client := mcpclient.New(t, h.service, protocol.version)
					actual, callErr := client.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: arguments})
					failed := callErr != nil || actual != nil && actual.IsError != nil && *actual.IsError
					if test.fail {
						if callErr != nil || actual == nil || !failed {
							t.Fatalf("expected tool error, result=%+v error=%v", actual, callErr)
						}
						encoded, err := json.Marshal(actual.StructuredContent)
						if err != nil {
							t.Fatal(err)
						}
						// MCP intentionally sanitizes internal reader diagnostics;
						// do not weaken that policy to expose the raw budget error.
						var failure struct {
							Error   bool
							Status  int
							Message string
							Data    json.RawMessage
						}
						if err := json.Unmarshal(encoded, &failure); err != nil {
							t.Fatal(err)
						}
						if !failure.Error || failure.Status != 500 || failure.Message != "Internal Server Error" || len(failure.Data) != 0 {
							t.Fatalf("unexpected failure payload: %s", encoded)
						}
						return
					}
					if failed || actual == nil {
						t.Fatalf("result=%+v error=%v", actual, callErr)
					}
					encoded, err := json.Marshal(actual.StructuredContent)
					if err != nil {
						t.Fatal(err)
					}
					var rows struct{ Data []struct{ AccountID int } }
					if err := json.Unmarshal(encoded, &rows); err != nil {
						t.Fatal(err)
					}
					if len(rows.Data) != 1 || rows.Data[0].AccountID != 1 {
						t.Fatalf("unexpected composed rows: %s", encoded)
					}
				})
			}
		})
	}
}

func TestCubeComposeOptInAndLimits(t *testing.T) {
	base := newGroupedReportHarness(t, false)
	if _, ok := base.service.Registry().ToolRegistry.Get("SpendCubeCompose"); ok {
		t.Fatal("compose must be opt-in")
	}
	h := newGroupedReportHarness(t, false, func(s *spec.ReportSettings) {
		s.Compose = &spec.CubeComposeSettings{Enabled: true, MaxCubes: 1, MaxLimit: 5}
	})
	for _, body := range []string{
		`{"cubes":[],"sql":"SELECT t1.AccountID FROM $CubeSQL1 AS t1"}`,
		`{"cubes":[{},{}],"sql":"SELECT t1.AccountID FROM $CubeSQL1 AS t1"}`,
		`{"cubes":[{"inheritFrom":1}],"sql":"SELECT t1.AccountID FROM $CubeSQL1 AS t1"}`,
		`{"cubes":[{}],"sql":"SELECT t1.AccountID FROM $CubeSQL1 AS t1 LIMIT 6"}`,
		`{"cubes":[{}],"sql":"SELECT * FROM report_spend"}`,
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest("POST", "/spend/acme/cube/compose", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		httpgateway.NewHandler(h.runtime, nil, "").ServeHTTP(recorder, request)
		if recorder.Code == 200 {
			t.Fatalf("accepted invalid composition %s", body)
		}
	}
}

func TestCubeComposeOmittedFilterMasksAmbientRequest(t *testing.T) {
	h := newGroupedReportHarness(t, false, func(s *spec.ReportSettings) { s.Compose = &spec.CubeComposeSettings{Enabled: true} })
	body := `{"cubes":[{"filters":{"accountIDs":"1","tenant":"acme","channel":"web","status":"active"}}],"sql":"SELECT t1.AccountID, t1.TotalSpend AS amount FROM $CubeSQL1 AS t1"}`
	request := httptest.NewRequest("POST", "/spend/acme/cube/compose", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Region", "US")
	recorder := httptest.NewRecorder()
	httpgateway.NewHandler(h.runtime, nil, "").ServeHTTP(recorder, request)
	if recorder.Code != 200 {
		t.Fatalf("HTTP %d: %s", recorder.Code, recorder.Body.String())
	}
	var result struct {
		Data []struct {
			Amount float64 `json:"amount"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Data) != 1 || result.Data[0].Amount != 1150 {
		t.Fatalf("ambient region leaked: %s", recorder.Body.String())
	}
}
