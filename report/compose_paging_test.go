package report

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	httpgateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/xeipuuv/gojsonschema"
)

func TestCubeComposePagingCompletenessHTTPMCP(t *testing.T) {
	h := newGroupedReportHarness(t, false, func(s *spec.ReportSettings) { s.Compose = &spec.CubeComposeSettings{Enabled: true, MaxLimit: 3} })
	tool, ok := h.service.Registry().ToolRegistry.Get("SpendCubeCompose")
	if !ok {
		t.Fatal("missing compose tool")
	}
	schemaJSON, _ := json.Marshal(tool.Metadata.OutputSchema)
	for _, field := range []string{"hasMore", "complete", "truncated", "limit", "offset"} {
		if !strings.Contains(string(schemaJSON), `"`+field+`"`) {
			t.Fatalf("missing schema field %s: %s", field, schemaJSON)
		}
	}
	const union = "SELECT 'root' AS key, t1.TotalSpend AS amount FROM $CubeSQL1 AS t1 UNION ALL SELECT t2.Region AS key, t2.TotalSpend AS amount FROM $CubeSQL2 AS t2 ORDER BY key"
	overflow := strings.Replace(union, " ORDER BY", " UNION ALL SELECT 'second-root' AS key, t3.TotalSpend AS amount FROM $CubeSQL3 AS t3 ORDER BY", 1)
	const join = "SELECT t1.Region AS key, t1.TotalSpend AS amount FROM $CubeSQL1 AS t1 ORDER BY key"
	for _, tc := range []struct {
		name, sql, region   string
		limit, offset, want int
		more, complete      bool
	}{
		{"union below cap", union, "EU", 3, 0, 2, false, true},
		{"union exact cap", union, "", 3, 0, 3, false, true},
		{"union over limit", union, "", 2, 0, 2, true, false},
		{"union beyond hard cap", overflow, "", 3, 0, 3, true, false},
		{"union offset exact", union, "", 2, 1, 2, false, false},
		{"union offset over", union, "", 1, 1, 1, true, false},
		{"union offset below", union, "EU", 2, 1, 1, false, false},
		{"join below cap", join, "", 3, 0, 2, false, true},
		{"join exact limit", join, "", 2, 0, 2, false, true},
		{"join over limit", join, "", 1, 0, 1, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filters := map[string]any{"accountIDs": "1", "tenant": "acme", "channel": "web", "status": "active"}
			if tc.region != "" {
				filters["region"] = tc.region
			}
			frames := []any{map[string]any{"filters": filters}}
			if strings.Contains(tc.sql, "UNION ALL") {
				frames = append(frames, map[string]any{"inheritFrom": 1})
			}
			if strings.Contains(tc.sql, "$CubeSQL3") {
				frames = append(frames, map[string]any{"inheritFrom": 1})
			}
			sql := tc.sql + fmt.Sprintf(" LIMIT %d", tc.limit)
			if tc.offset > 0 {
				sql += fmt.Sprintf(" OFFSET %d", tc.offset)
			}
			args := map[string]any{"cubes": frames, "sql": sql}
			body, err := json.Marshal(args)
			if err != nil {
				t.Fatal(err)
			}
			for _, protocol := range []string{"HTTP", "MCP"} {
				t.Run(protocol, func(t *testing.T) {
					var payload []byte
					if protocol == "HTTP" {
						req := httptest.NewRequest("POST", "/spend/acme/cube/compose", strings.NewReader(string(body)))
						req.Header.Set("Content-Type", "application/json")
						response := httptest.NewRecorder()
						httpgateway.NewHandler(h.runtime, nil, "").ServeHTTP(response, req)
						if response.Code != 200 {
							t.Fatalf("HTTP %d: %s", response.Code, response.Body.String())
						}
						payload = response.Body.Bytes()
					} else {
						result, e := tool.Handler(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: args}})
						if e != nil || result == nil || result.IsError != nil && *result.IsError {
							t.Fatalf("MCP: %v %+v", e, result)
						}
						payload, err = json.Marshal(result.StructuredContent)
						if err != nil {
							t.Fatal(err)
						}
						checked, schemaErr := gojsonschema.Validate(gojsonschema.NewGoLoader(tool.Metadata.OutputSchema), gojsonschema.NewBytesLoader(payload))
						if schemaErr != nil || !checked.Valid() {
							t.Fatalf("wire/schema mismatch: %v %v", schemaErr, checked)
						}
					}
					var output struct {
						Data      []json.RawMessage `json:"data"`
						More      *bool             `json:"hasMore"`
						Complete  *bool             `json:"complete"`
						Truncated *bool             `json:"truncated"`
						Limit     *int              `json:"limit"`
						Offset    *int              `json:"offset"`
					}
					if err = json.Unmarshal(payload, &output); err != nil {
						t.Fatal(err)
					}
					if len(output.Data) != tc.want || output.More == nil || *output.More != tc.more || output.Complete == nil || *output.Complete != tc.complete || output.Truncated == nil || *output.Truncated == tc.complete || output.Limit == nil || *output.Limit != tc.limit || output.Offset == nil || *output.Offset != tc.offset {
						t.Fatalf("paging metadata lost: %s", payload)
					}
				})
			}
		})
	}
}
