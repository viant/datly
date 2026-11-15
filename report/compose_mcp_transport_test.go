package report

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
)

func TestComposeMCPMetadataNativeTransport(t *testing.T) {
	for _, version := range []string{schema.LegacyProtocolVersion, schema.LatestProtocolVersion} {
		for _, explicit := range []bool{false, true} {
			t.Run(version+"/"+map[bool]string{false: "legacy", true: "explicit"}[explicit], func(t *testing.T) {
				description := "Compose spend, with \"quoted\" guidance.\nSupply cubes and sql."
				h := newGroupedReportHarness(t, false, func(s *spec.ReportSettings) {
					s.Compose = &spec.CubeComposeSettings{Enabled: true}
					if explicit {
						s.Compose.MCP = &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "compose_spend", Description: description, CaseFormat: "lc"}
					}
				})
				native := mcpclient.New(t, h.service, version)
				list, err := native.ListTools(context.Background(), nil)
				require.NoError(t, err)
				wire, err := json.Marshal(list)
				require.NoError(t, err)
				var listing struct {
					Tools []struct {
						Name, Description string
						InputSchema       json.RawMessage
					}
				}
				require.NoError(t, json.Unmarshal(wire, &listing))
				name := "SpendCubeCompose"
				if explicit {
					name = "compose_spend"
				}
				found := false
				for _, tool := range listing.Tools {
					if explicit {
						require.NotEqual(t, "SpendCubeCompose", tool.Name)
					}
					if tool.Name == name {
						found = true
						if explicit {
							require.Equal(t, description, tool.Description)
						}
						for _, field := range []string{"cubes", "sql", "page", "resultLimit"} {
							require.True(t, strings.Contains(string(tool.InputSchema), `"`+field+`"`), string(tool.InputSchema))
						}
					}
				}
				require.True(t, found, string(wire))
				args := map[string]any{"cubes": []any{map[string]any{"filters": map[string]any{"accountIDs": "1,2", "tenant": "acme", "region": "EU", "channel": "web", "status": "active"}}}, "sql": "SELECT t1.AccountID,t1.TotalSpend FROM $CubeSQL1 AS t1 ORDER BY t1.AccountID LIMIT 8"}
				for i := 0; i < 2; i++ {
					result, err := native.CallTool(context.Background(), &schema.CallToolRequestParams{Name: name, Arguments: args})
					require.NoError(t, err)
					require.NotNil(t, result)
					require.False(t, result.IsError != nil && *result.IsError, "%+v", result)
					content, err := json.Marshal(result.Content)
					require.NoError(t, err)
					require.Contains(t, string(content), "150")
					require.Contains(t, string(content), "70")
				}
				calls := h.codec.calls.Load()
				require.EqualValues(t, 2, calls)
				if explicit {
					result, err := native.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: args})
					require.True(t, err != nil || result != nil && result.IsError != nil && *result.IsError)
					require.Equal(t, calls, h.codec.calls.Load(), "unknown name entered source query")
				}
			})
		}
	}
}
