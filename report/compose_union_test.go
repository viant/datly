package report

import (
	"context"
	"encoding/json"
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

func TestCubeComposeUnionNativeAuthorityAndCombinedPaging(t *testing.T) {
	h := (groupedReportHarnessConfig{
		report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) { s.Compose = &spec.CubeComposeSettings{Enabled: true, MaxLimit: 10} }},
		source: func(component *spec.Component, types *typecatalog.Catalog) {
			// Authored key projection has a disjoint leaf namespace; labels need not
			// be used as hierarchy identities. Region is non-null in this fixture.
			component.RootView.Source.SQL = strings.Replace(component.RootView.Source.SQL, "s.region,", "'leaf:' || s.region AS region,", 1)

			if err := types.Register(typecatalog.TypeOriginPackage, &x.Type{Type: reflect.TypeOf(composeChannelPredicate{}), PkgPath: "example.com/acme/reporting", Name: "ComposeChannelPredicate"}); err != nil {
				t.Fatal(err)
			}
			for _, p := range component.Parameters {
				if p.Name == "AccountIDs" {
					p.Required = boolPointer(true)
				}
				if p.Name == "Channel" {
					p.Predicates = []*spec.Predicate{{Name: "handler", Args: []string{"example.com/acme/reporting.ComposeChannelPredicate"}}}
				}
			}
		},
	}).build(t)
	const sql = "SELECT 'root' AS key, NULL AS parent, t1.TotalSpend AS amount FROM $CubeSQL1 AS t1 UNION ALL SELECT t2.Region AS key, 'root' AS parent, t2.TotalSpend AS amount FROM $CubeSQL2 AS t2 ORDER BY key LIMIT 1 OFFSET 2"
	for _, scenario := range []struct {
		name    string
		frames  []any
		invalid bool
	}{
		{"inherited authorized filters", []any{map[string]any{"filters": map[string]any{"accountIDs": "1", "tenant": "acme", "channel": "web", "status": "active"}}, map[string]any{"inheritFrom": 1}}, false},
		{"scope override denied", []any{map[string]any{"filters": map[string]any{"accountIDs": "1", "tenant": "acme", "channel": "web"}}, map[string]any{"inheritFrom": 1, "filters": map[string]any{"tenant": "other"}}}, true},
		{"missing required scope", []any{map[string]any{"filters": map[string]any{"tenant": "acme", "channel": "web"}}, map[string]any{"inheritFrom": 1}}, true},
		{"forward inheritance", []any{map[string]any{"inheritFrom": 2}, map[string]any{}}, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			args := map[string]any{"cubes": scenario.frames, "sql": sql}
			body, err := json.Marshal(args)
			if err != nil {
				t.Fatal(err)
			}
			for _, protocol := range []string{"HTTP", "MCP"} {
				t.Run(protocol, func(t *testing.T) {
					observation := &composeObservation{}
					ctx := context.WithValue(context.Background(), composeObservationKey{}, observation)
					var payload []byte
					failed := false
					if protocol == "HTTP" {
						req := httptest.NewRequest("POST", "/spend/acme/cube/compose", strings.NewReader(string(body))).WithContext(ctx)
						req.Header.Set("Content-Type", "application/json")
						req.Header.Set("X-Region", "FORBIDDEN-AMBIENT")
						response := httptest.NewRecorder()
						httpgateway.NewHandler(h.runtime, nil, "").ServeHTTP(response, req)
						failed = response.Code != 200
						payload = response.Body.Bytes()
					} else {
						tool, ok := h.service.Registry().ToolRegistry.Get("SpendCubeCompose")
						if !ok {
							t.Fatal("missing compose tool")
						}
						result, protocolErr := tool.Handler(ctx, &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: args}})
						failed = protocolErr != nil || result == nil || result.IsError != nil && *result.IsError
						if !failed {
							payload = []byte(result.Content[0].(schema.TextContent).Text)
						}
					}
					if failed != scenario.invalid {
						t.Fatalf("invalid=%v failed=%v payload=%s", scenario.invalid, failed, payload)
					}
					if failed {
						return
					}
					var output struct {
						Data []struct {
							Key    string  `json:"key"`
							Parent *string `json:"parent"`
							Amount float64 `json:"amount"`
						}
					}
					if err := json.Unmarshal(payload, &output); err != nil {
						t.Fatal(err)
					}
					if len(output.Data) != 1 || output.Data[0].Key != "root" || output.Data[0].Parent != nil || output.Data[0].Amount != 1150 {
						t.Fatalf("union grain/paging/scope: %s", payload)
					}
					if len(observation.frames) != 2 || !observation.frames[0].Snapshot.Equal(observation.frames[1].Snapshot) {
						t.Fatalf("frames not independently prepared: %+v", observation.frames)
					}
				})
			}
		})
	}
}

func TestCubeComposeUnionRejectsBranchWhereOverNativeTransports(t *testing.T) {
	h := newGroupedReportHarness(t, false, func(s *spec.ReportSettings) { s.Compose = &spec.CubeComposeSettings{Enabled: true} })
	for _, sql := range []string{
		"SELECT t1.TotalSpend AS amount FROM $CubeSQL1 AS t1 WHERE t1.TotalSpend > 0 UNION ALL SELECT t2.TotalSpend AS amount FROM $CubeSQL2 AS t2",
		"SELECT t1.TotalSpend AS amount FROM $CubeSQL1 AS t1 UNION ALL SELECT t2.TotalSpend AS amount FROM $CubeSQL2 AS t2 WHERE t2.TotalSpend > 0",
	} {
		args := map[string]any{"cubes": []any{map[string]any{"filters": map[string]any{"tenant": "acme"}}, map[string]any{"inheritFrom": 1}}, "sql": sql}
		body, err := json.Marshal(args)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest("POST", "/spend/acme/cube/compose", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		httpgateway.NewHandler(h.runtime, nil, "").ServeHTTP(response, req)
		if response.Code == 200 {
			t.Fatalf("HTTP silently dropped branch WHERE: %s", response.Body.String())
		}
		tool, ok := h.service.Registry().ToolRegistry.Get("SpendCubeCompose")
		if !ok {
			t.Fatal("missing tool")
		}
		result, protocolErr := tool.Handler(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: args}})
		if protocolErr == nil && result != nil && (result.IsError == nil || !*result.IsError) {
			t.Fatal("MCP silently dropped branch WHERE")
		}
	}
}
