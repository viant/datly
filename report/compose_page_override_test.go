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
	"github.com/viant/datly/report/cubecompose"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/x"
	"github.com/xeipuuv/gojsonschema"
)

func TestComposePageOverrideNativeHTTPMCP(t *testing.T) {
	h := (groupedReportHarnessConfig{report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) { s.Compose = &spec.CubeComposeSettings{Enabled: true, MaxLimit: 1000} }}, source: func(c *spec.Component, types *typecatalog.Catalog) {
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
	if _, err := h.sql.DB.ExecContext(context.Background(), `INSERT INTO report_spend VALUES('other',1,'DENIED','web','active',99999),('acme',2,'DENIED','web','active',99999)`); err != nil {
		t.Fatal(err)
	}
	tool, ok := h.service.Registry().ToolRegistry.Get("SpendCubeCompose")
	if !ok {
		t.Fatal("missing compose tool")
	}
	const authored = "SELECT t1.Region AS key,t1.TotalSpend AS amount FROM $CubeSQL1 AS t1 ORDER BY amount DESC,key ASC LIMIT 10"
	for _, protocol := range []string{"HTTP", "MCP"} {
		t.Run(protocol, func(t *testing.T) {
			invoke := func(sql string, page any, prefix *int, poison int) (ComposeResponse, bool) {
				frames := []any{map[string]any{"filters": map[string]any{"tenant": "acme", "accountIDs": "1", "channel": "web", "status": "active"}}}
				if strings.Contains(sql, "UNION ALL") {
					frames = append(frames, map[string]any{"inheritFrom": 1})
				}
				if poison >= 0 {
					frames[poison] = map[string]any{"filters": map[string]any{"tenant": "other", "accountIDs": "1", "channel": "web", "status": "active"}}
				}
				args := map[string]any{"cubes": frames, "sql": sql}
				if page != nil {
					args["page"] = page
				}
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
					payload = rec.Body.Bytes()
					failed = rec.Code != 200
				} else {
					result, err := tool.Handler(ctx, &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: args}})
					failed = err != nil || result == nil || result.IsError != nil && *result.IsError
					if !failed {
						payload, _ = json.Marshal(result.StructuredContent)
						checked, err := gojsonschema.Validate(gojsonschema.NewGoLoader(tool.Metadata.OutputSchema), gojsonschema.NewBytesLoader(payload))
						if err != nil || !checked.Valid() {
							t.Fatalf("schema mismatch: %v %+v", err, checked)
						}
					}
				}
				var out ComposeResponse
				if !failed {
					if err := json.Unmarshal(payload, &out); err != nil {
						t.Fatal(err)
					}
				}
				if !failed && args["sql"] != sql {
					t.Fatal("authored SQL mutated")
				}
				return out, failed
			}
			full, failed := invoke(authored, nil, nil, -1)
			if failed || len(full.Data.([]any)) != 9 || !full.Complete || full.HasMore || full.Limit != 10 || full.Offset != 0 {
				t.Fatalf("omission changed authored behavior: %+v failed=%v", full, failed)
			}
			nullPage, failed := invoke(authored, json.RawMessage("null"), nil, -1)
			if failed || !reflect.DeepEqual(full, nullPage) {
				t.Fatalf("null page changed authored behavior: %+v failed=%v", nullPage, failed)
			}
			for _, prefix := range []*int{nil, intOverridePointer(5)} {
				total := 9
				if prefix != nil {
					total = *prefix
				}
				var combined []any
				for offset := 0; offset < total; offset += 2 {
					out, failed := invoke(authored, cubecompose.Page{Limit: 2, Offset: offset}, prefix, -1)
					if failed {
						t.Fatalf("page %d failed", offset)
					}
					want := 2
					if total-offset < want {
						want = total - offset
					}
					if len(out.Data.([]any)) != want || out.HasMore != (offset+2 < total) || out.Complete || !out.Truncated || out.Limit != 2 || out.Offset != offset {
						t.Fatalf("page metadata wrong: %+v", out)
					}
					combined = append(combined, out.Data.([]any)...)
				}
				expected := full.Data.([]any)[:total]
				if !reflect.DeepEqual(combined, expected) {
					t.Fatalf("membership differs: %+v / %+v", combined, expected)
				}
				exact, failed := invoke(authored, cubecompose.Page{Limit: total}, prefix, -1)
				if failed || exact.HasMore || !exact.Complete || exact.Truncated {
					t.Fatalf("exact completion wrong: %+v", exact)
				}
				empty, failed := invoke(authored, cubecompose.Page{Limit: 2, Offset: total}, prefix, -1)
				if failed || len(empty.Data.([]any)) != 0 || empty.HasMore || empty.Complete || !empty.Truncated {
					t.Fatalf("empty offset wrong: %+v", empty)
				}
			}
			for _, page := range []any{map[string]any{}, cubecompose.Page{}, cubecompose.Page{Limit: -1}, cubecompose.Page{Limit: 1, Offset: -1}, cubecompose.Page{Limit: 1001}, cubecompose.Page{Limit: 1, Offset: int(^uint(0) >> 1)}, cubecompose.Page{Limit: int(^uint(0) >> 1), Offset: 1}} {
				if _, failed := invoke(authored, page, nil, -1); !failed {
					t.Fatalf("invalid page accepted: %+v", page)
				}
			}
			if _, failed := invoke(strings.Replace(authored, " ORDER BY amount DESC,key ASC", "", 1), cubecompose.Page{Limit: 2, Offset: 2}, nil, -1); !failed {
				t.Fatal("unordered offset accepted")
			}
			if _, failed := invoke(authored, cubecompose.Page{Limit: 2, Offset: 2}, nil, 0); !failed {
				t.Fatal("poisoned source accepted")
			}
			union := "SELECT t1.Region AS key,t1.TotalSpend AS amount FROM $CubeSQL1 AS t1 UNION ALL SELECT t2.Region AS key,t2.TotalSpend AS amount FROM $CubeSQL2 AS t2 ORDER BY key LIMIT 10 OFFSET 1"
			old, failed := invoke(union, nil, nil, -1)
			if failed || old.Offset != 1 || old.Limit != 10 || !old.HasMore {
				t.Fatalf("legacy UNION changed: %+v", old)
			}
			next, failed := invoke(union, cubecompose.Page{Limit: 2, Offset: 16}, nil, -1)
			if failed || len(next.Data.([]any)) != 2 || next.HasMore || next.Offset != 16 {
				t.Fatalf("UNION override failed: %+v", next)
			}
			for frame := 0; frame < 2; frame++ {
				if _, failed := invoke(union, cubecompose.Page{Limit: 2}, nil, frame); !failed {
					t.Fatalf("poisoned frame %d accepted", frame)
				}
			}
		})
	}
}

func intOverridePointer(value int) *int { return &value }
