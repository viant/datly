package report

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
)

func TestReportMCPMetadataNativeTransport(t *testing.T) {
	for _, version := range []string{schema.LegacyProtocolVersion, schema.LatestProtocolVersion} {
		for _, explicit := range []bool{false, true} {
			label := "legacy"
			if explicit {
				label = "explicit"
			}
			t.Run(version+"/"+label, func(t *testing.T) {
				strict := false
				description := "Aggregate spend, with \"quoted\" guidance.\nSelect dimensions and measures."
				h := newGroupedReportHarness(t, false, func(s *spec.ReportSettings) {
					if explicit {
						s.MCP = &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "aggregate_spend", Description: description, CaseFormat: "lc", StrictArguments: &strict}
					}
				})
				native := mcpclient.New(t, h.service, version)
				list, err := native.ListTools(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				wire, err := json.Marshal(list)
				if err != nil {
					t.Fatal(err)
				}
				var listing struct {
					Tools []struct {
						Name        string
						Description string
						InputSchema json.RawMessage
					}
				}
				if err = json.Unmarshal(wire, &listing); err != nil {
					t.Fatal(err)
				}
				name := "SpendCube"
				if explicit {
					name = "aggregate_spend"
				}
				found := false
				for _, tool := range listing.Tools {
					if explicit && tool.Name == "SpendCube" {
						t.Fatal("old name remained registered")
					}
					if tool.Name == name {
						found = true
						if explicit && tool.Description != description {
							t.Fatalf("description %q", tool.Description)
						}
						for _, field := range []string{"dimensions", "measures", "filters"} {
							if !strings.Contains(string(tool.InputSchema), `"`+field+`"`) {
								t.Fatalf("missing %s: %s", field, tool.InputSchema)
							}
						}
					}
				}
				if !found {
					t.Fatalf("tool missing: %s", wire)
				}
				args := groupedReportRequest{Dimensions: []string{"Tenant", "AccountID", "Region"}, Measures: []string{"TotalSpend", "OrderCount"}, Filters: groupedReportFilters{AccountIDs: "1,2", Tenant: "acme", Region: "EU", Channel: "web", Status: "active"}, OrderBy: []string{"TotalSpend DESC"}}.mcpArguments()
				for i := 0; i < 2; i++ {
					result, err := native.CallTool(context.Background(), &schema.CallToolRequestParams{Name: name, Arguments: args})
					if err != nil || result == nil || result.IsError != nil && *result.IsError {
						t.Fatalf("query %v %+v", err, result)
					}
					content, err := json.Marshal(result.Content[0])
					if err != nil {
						t.Fatal(err)
					}
					var text struct{ Text string }
					if err = json.Unmarshal(content, &text); err != nil {
						t.Fatal(err)
					}
					var output groupedSpendOutput
					if err = json.Unmarshal([]byte(text.Text), &output); err != nil {
						t.Fatal(err)
					}
					if len(output.Rows) != 2 || output.Rows[0].TotalSpend != 150 || output.Rows[1].TotalSpend != 70 {
						t.Fatalf("actual query output %s", text.Text)
					}
					if explicit && (!strings.Contains(text.Text, `"totalSpend"`) || strings.Contains(text.Text, `"TotalSpend"`)) {
						t.Fatalf("output case %s", text.Text)
					}
				}
				if calls := h.codec.calls.Load(); calls != 2 {
					t.Fatalf("warm invocation codec count %d", calls)
				}
				if explicit {
					result, err := native.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "SpendCube", Arguments: args})
					if err == nil && (result == nil || result.IsError == nil || !*result.IsError) {
						t.Fatalf("old name resolved: %+v", result)
					}
					if h.codec.calls.Load() != 2 {
						t.Fatal("unknown name reached source query")
					}
				}
			})
		}
	}
}
