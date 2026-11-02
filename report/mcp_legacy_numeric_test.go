package report

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/mcpclient"
	dserver "github.com/viant/datly/mcp/server"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
)

// Actual Datly native HTTP calls demonstrate the accepted legacy rounding:
// generic arguments/results use float64 before and after authored int64 binding.
// Distinct rounded values still exercise concurrent response correlation.
type exactPrecisionSource struct{ service dserver.ServerService }

func (s exactPrecisionSource) Pin(ctx context.Context) (context.Context, dserver.ServerService, error) {
	return ctx, s.service, nil
}

func TestComposeScalarValuesLegacyNumericHTTPMCP(t *testing.T) {
	h := (groupedReportHarnessConfig{report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) {
		s.Compose = &spec.CubeComposeSettings{Enabled: true, ValuesType: authoredWrapperControls}
	}}}).build(t)
	if _, err := h.sql.DB.ExecContext(context.Background(), `INSERT INTO report_spend VALUES ('acme',1,'ZZ','web','active',600),('acme',1,'ZZ','web','active',600)`); err != nil {
		t.Fatal(err)
	}
	for _, protocol := range []struct{ name, version string }{{"session", schema.LegacyProtocolVersion}, {"stateless", schema.LatestProtocolVersion}} {
		t.Run(protocol.name, func(t *testing.T) {
			native := (mcpclient.Config{Source: exactPrecisionSource{service: h.service}, ProtocolVersion: protocol.version}).New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var group sync.WaitGroup
			failures := make(chan error, 2)
			for _, want := range []string{"9007199254740993", "9007199254740995"} {
				group.Add(1)
				go func() {
					defer group.Done()
					rounded, _ := strconv.ParseFloat(want, 64)
					encodedRounded, _ := json.Marshal(rounded)
					args := map[string]any{
						"cubes":  []any{map[string]any{"filters": map[string]any{"tenant": "acme", "accountIDs": "1", "region": "ZZ", "channel": "web", "status": "active"}}},
						"sql":    `SELECT t1.TotalSpend AS amount,$Values.threshold AS exact FROM $CubeSQL1 AS t1 WHERE t1.TotalSpend < $Values.threshold ORDER BY amount DESC LIMIT 10`,
						"values": map[string]any{"threshold": json.Number(want), "metric": json.Number("1"), "label": "alpha"},
					}
					result, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: args})
					if err != nil {
						failures <- fmt.Errorf("%s request: %w", want, err)
						return
					}
					if result == nil || result.IsError != nil && *result.IsError {
						failures <- fmt.Errorf("%s error result: %+v", want, result)
						return
					}
					object, ok := result.StructuredContent.(map[string]any)
					if !ok {
						failures <- fmt.Errorf("%s structured type %T", want, result.StructuredContent)
						return
					}
					rows, ok := object["data"].([]any)
					if !ok || len(rows) != 1 {
						failures <- fmt.Errorf("%s rows: %+v", want, object)
						return
					}
					row := rows[0].(map[string]any)
					if row["exact"] != rounded || row["amount"] != float64(1200) {
						failures <- fmt.Errorf("%s legacy rounding or response correlation changed: %+v", want, row)
						return
					}
					wire, err := json.Marshal(result)
					if err != nil {
						failures <- err
						return
					}
					var envelope struct {
						Structured json.RawMessage `json:"structuredContent"`
					}
					if err = json.Unmarshal(wire, &envelope); err != nil {
						failures <- err
						return
					}
					var out struct {
						Data []struct {
							Exact json.Number `json:"exact"`
						} `json:"data"`
					}
					if err = json.Unmarshal(envelope.Structured, &out); err != nil || len(out.Data) != 1 || out.Data[0].Exact.String() != string(encodedRounded) {
						failures <- fmt.Errorf("%s reserialized rounded result changed: %s, %v", want, wire, err)
						return
					}
				}()
			}
			group.Wait()
			close(failures)
			for err := range failures {
				t.Error(err)
			}
		})
	}
}
