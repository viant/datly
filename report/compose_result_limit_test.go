package report

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	httpgateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/x"
)

func TestSemanticResultPrefixOverNativeHTTPAndMCP(t *testing.T) {
	h := (groupedReportHarnessConfig{report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) { s.Compose = &spec.CubeComposeSettings{Enabled: true, MaxLimit: 20} }}, source: func(c *spec.Component, types *typecatalog.Catalog) {
		if err := types.Register(typecatalog.TypeOriginPackage, &x.Type{Type: reflect.TypeOf(composeChannelPredicate{}), PkgPath: "example.com/acme/reporting", Name: "ComposeChannelPredicate"}); err != nil {
			t.Fatal(err)
		}
		for _, p := range c.Parameters {
			if p.Name == "AccountIDs" {
				p.Required = boolPointer(true)
			}
			if p.Name == "Channel" {
				p.Predicates = []*spec.Predicate{{Name: "handler", Args: []string{"example.com/acme/reporting.ComposeChannelPredicate"}}}
			}
		}
	}}).build(t)
	for i := 1; i <= 7; i++ {
		if _, err := h.sql.DB.ExecContext(context.Background(), `INSERT INTO report_spend VALUES('acme',1,?,'web','active',?)`, fmt.Sprintf("R%d", i), i*10); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.sql.DB.ExecContext(context.Background(), `INSERT INTO report_spend VALUES('other',1,'DENIED','web','active',99999),('acme',1,'Tie','web','active',50)`); err != nil {
		t.Fatal(err)
	}
	// More eligible groups than the transport budget must participate in source
	// ordering; an early/default frame cap would hide US before the prefix stage.
	for i := 0; i < 150; i++ {
		if _, err := h.sql.DB.ExecContext(context.Background(), `INSERT INTO report_spend VALUES('acme',1,?,'web','active',1)`, fmt.Sprintf("Early%03d", i)); err != nil {
			t.Fatal(err)
		}
	}
	tool, ok := h.service.Registry().ToolRegistry.Get("SpendCubeCompose")
	if !ok {
		t.Fatal("compose tool missing")
	}
	// The exact optional field is exposed through the normal derived schema.
	encodedSchema, err := json.Marshal(tool.Metadata.InputSchema)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encodedSchema), "resultLimit") {
		t.Fatal("semantic limit missing from tool metadata")
	}
	var schemaObject map[string]any
	if err := json.Unmarshal(encodedSchema, &schemaObject); err != nil {
		t.Fatal(err)
	}
	if required, ok := schemaObject["required"].([]any); ok {
		for _, name := range required {
			if name == "resultLimit" {
				t.Fatal("optional semantic prefix became required")
			}
		}
	}
	for _, protocol := range []string{"HTTP", "MCP"} {
		t.Run(protocol, func(t *testing.T) {
			invoke := func(prefix *int, limit, offset int, tenant string) (ComposeResponse, bool) {
				sql := fmt.Sprintf("SELECT t1.Region AS region,t1.TotalSpend AS amount FROM $CubeSQL1 AS t1 ORDER BY amount DESC,region ASC LIMIT %d", limit)
				if offset != 0 {
					sql += fmt.Sprintf(" OFFSET %d", offset)
				}
				args := map[string]any{"sql": sql, "cubes": []any{map[string]any{"filters": map[string]any{"tenant": tenant, "accountIDs": "1", "channel": "web", "status": "active"}}}}
				if prefix != nil {
					args["resultLimit"] = *prefix
				}
				ctx := context.WithValue(context.Background(), composeObservationKey{}, &composeObservation{})
				var payload []byte
				failed := false
				if protocol == "HTTP" {
					body, _ := json.Marshal(args)
					req := httptest.NewRequest("POST", "/spend/acme/cube/compose", strings.NewReader(string(body))).WithContext(ctx)
					req.Header.Set("Content-Type", "application/json")
					rec := httptest.NewRecorder()
					httpgateway.NewHandler(h.runtime, nil, "").ServeHTTP(rec, req)
					failed = rec.Code != 200
					payload = rec.Body.Bytes()
				} else {
					result, err := tool.Handler(ctx, &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: args}})
					failed = err != nil || result == nil || result.IsError != nil && *result.IsError
					if !failed {
						payload = []byte(result.Content[0].(schema.TextContent).Text)
					}
				}
				var out ComposeResponse
				if !failed {
					if err := json.Unmarshal(payload, &out); err != nil {
						t.Fatal(err)
					}
				}
				return out, failed
			}
			prefix := 5
			full, failed := invoke(&prefix, 20, 0, "acme")
			if failed {
				t.Fatal("prefix request failed")
			}
			rows := full.Data.([]any)
			if len(rows) != 5 || full.HasMore || !full.Complete || full.Truncated || full.ResultLimit == nil || *full.ResultLimit != 5 {
				t.Fatalf("semantic prefix not complete: %+v", full)
			}
			for i, want := range []string{"US", "EU", "R7", "R6", "R5"} {
				if rows[i].(map[string]any)["region"] != want {
					t.Fatalf("source filters/order/prefix changed: %+v", rows)
				}
			}
			var combined []any
			for _, offset := range []int{0, 2, 4} {
				out, failed := invoke(&prefix, 2, offset, "acme")
				if failed {
					t.Fatal("page failed")
				}
				if out.HasMore != (offset < 4) || out.Complete || !out.Truncated {
					t.Fatalf("page completeness wrong: %+v", out)
				}
				combined = append(combined, out.Data.([]any)...)
			}
			if !reflect.DeepEqual(rows, combined) {
				t.Fatalf("paged export membership differs from complete prefix: %+v / %+v", rows, combined)
			}
			short := 3
			small, failed := invoke(&short, 20, 0, "acme")
			if failed || len(small.Data.([]any)) != 3 {
				t.Fatal("prefix input reused prior result")
			}
			all, failed := invoke(nil, 20, 0, "acme")
			if failed || len(all.Data.([]any)) != 20 || all.Complete || !all.HasMore || !all.Truncated || all.ResultLimit != nil {
				t.Fatalf("omitted prefix changed legacy result: %+v", all)
			}
			for _, bad := range []int{0, -1, 21} {
				if _, failed := invoke(&bad, 20, 0, "acme"); !failed {
					t.Fatalf("invalid semantic bound %d accepted", bad)
				}
			}
			if _, failed := invoke(&prefix, 20, 0, "other"); !failed {
				t.Fatal("semantic prefix bypassed custom source predicate")
			}
		})
	}
}
