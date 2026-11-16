package report

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"

	httpgateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
)

// Parser rejection has the same public status with or without a values contract.
func TestComposeParserStatusIndependentOfValuesContract(t *testing.T) {
	for _, withValues := range []bool{false, true} {
		label := "without-values"
		if withValues {
			label = "with-values"
		}
		t.Run(label, func(t *testing.T) {
			h := (groupedReportHarnessConfig{report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) {
				s.Compose = &spec.CubeComposeSettings{Enabled: true}
				if withValues {
					s.Compose.ValuesType = authoredWrapperControls
				}
			}}}).build(t)
			tool, ok := h.service.Registry().ToolRegistry.Get("SpendCubeCompose")
			if !ok {
				t.Fatal("compose missing")
			}
			for _, sql := range []string{
				"SELECT t1.TotalSpend FROM $CubeSQL1 AS t1; SELECT 1",
				"SELECT t1.TotalSpend FROM foreign_table AS t1",
				"SELECT t1.TotalSpend FROM $CubeSQL2 AS t1",
			} {
				args := map[string]any{
					"cubes": []any{map[string]any{"filters": map[string]any{"tenant": "acme", "accountIDs": "1", "region": "ZZ", "channel": "web", "status": "active"}}},
					"sql":   sql,
				}
				result, protocolErr := tool.Handler(t.Context(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: args}})
				if protocolErr != nil || result == nil {
					t.Fatalf("transport: %v", protocolErr)
				}
				raw, err := json.Marshal(result.StructuredContent)
				if err != nil {
					t.Fatal(err)
				}
				var body struct {
					Status int `json:"status"`
				}
				if err = json.Unmarshal(raw, &body); err != nil {
					t.Fatal(err)
				}
				t.Logf("sql=%q status=%d error=%v sourceReads=%d", sql, body.Status, result.IsError != nil && *result.IsError, h.codec.calls.Load())
				if result.IsError == nil || !*result.IsError {
					t.Error("malformed query accepted")
				}
				if body.Status != 400 {
					t.Errorf("parser rejection status=%d; want400", body.Status)
				}
				requestBody, err := json.Marshal(args)
				if err != nil {
					t.Fatal(err)
				}
				req := httptest.NewRequest("POST", "/spend/acme/cube/compose", bytes.NewReader(requestBody))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				httpgateway.NewHandler(h.runtime, nil, "").ServeHTTP(rec, req)
				if rec.Code != 400 {
					t.Errorf("HTTP parser rejection status=%d; want400", rec.Code)
				}
				if h.codec.calls.Load() != 0 {
					t.Error("parser rejection reached source preparation")
				}
			}
		})
	}
}
