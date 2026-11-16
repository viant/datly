package report

import (
	"encoding/json"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/selectorcompanion/model"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/x"
	"github.com/viant/xdatly"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

type preparationReportSelectors struct {
	Fields []string `parameter:"Fields,kind=query,in=fields,required=false" querySelector:"view=spend,property=fields"`
	Page   int      `parameter:"Page,kind=query,in=page,required=false" querySelector:"view=spend,property=page"`
}
type PreparationReportHolder struct {
	Contract xdatly.Component[groupedSpendInput, sourcePolicyOutput] `component:"Spend,path=/spend/{tenant},method=GET"`
}

func (PreparationReportHolder) DatlyQuerySelectors() preparationReportSelectors {
	panic("metadata only")
}
func TestComposePreparationSelectedSourceContractCompanion(t *testing.T) {
	h := (groupedReportHarnessConfig{output: reflect.TypeFor[sourcePolicyOutput](), report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) { s.Compose = &spec.CubeComposeSettings{Enabled: true} }}, source: func(c *spec.Component, _ *typecatalog.Catalog) {
		c.Key.Scope = reflect.TypeFor[PreparationReportHolder]().PkgPath()
		association, err := model.New(x.NewType(reflect.TypeFor[PreparationReportHolder]()), "Contract", x.NewType(reflect.TypeFor[preparationReportSelectors]()))
		if err != nil {
			t.Fatal(err)
		}
		c.SelectorCompanion = association
		c.RootView.Selector.AllowPage = true
	}}).build(t)
	tool, ok := h.service.Registry().ToolRegistry.Get("SpendCubeCompose")
	if !ok {
		t.Fatal("compose missing")
	}
	result, protocolErr := tool.Handler(t.Context(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: map[string]any{"cubes": []any{map[string]any{"filters": map[string]any{"accountIDs": "1,2", "tenant": "acme", "region": "EU", "channel": "web", "status": "active"}}}, "sql": "SELECT t1.AccountID FROM $CubeSQL1 AS t1 ORDER BY t1.AccountID LIMIT 8"}}})
	raw, _ := json.Marshal(result)
	t.Logf("combined companion/source contract: %s protocol=%v", raw, protocolErr)
	if protocolErr != nil || result == nil || result.IsError != nil && *result.IsError {
		t.Fatalf("compose failed: %s protocol=%v", raw, protocolErr)
	}
	for _, tc := range []struct {
		sql    string
		frames int
		denied bool
	}{
		{"SELECT t1.AccountID FROM $CubeSQL1 AS t1 ORDER BY t1.AccountID LIMIT 1", 1, false},
		{"SELECT t1.AccountID FROM $CubeSQL1 AS t1 JOIN $CubeSQL2 AS t2 ON t1.AccountID=t2.AccountID ORDER BY t1.AccountID LIMIT 1", 2, false},
		{"SELECT t1.TotalSpend FROM $CubeSQL1 AS t1 LIMIT 1", 1, true},
	} {
		for run := 0; run < 2; run++ {
			before := h.codec.calls.Load()
			frames := make([]any, tc.frames)
			for i := range frames {
				frames[i] = map[string]any{"filters": map[string]any{"accountIDs": "1,2", "tenant": "acme", "region": "EU", "channel": "web", "status": "active"}}
			}
			result, protocolErr := tool.Handler(t.Context(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: map[string]any{"cubes": frames, "sql": tc.sql}}})
			raw, _ := json.Marshal(result)
			if protocolErr != nil || result == nil {
				t.Fatalf("transport failed: %s %v", raw, protocolErr)
			}
			failed := result.IsError != nil && *result.IsError
			if failed != tc.denied {
				t.Fatalf("result=%s", raw)
			}
			calls := h.codec.calls.Load() - before
			if tc.denied {
				if calls != 0 {
					t.Fatalf("denied projection prepared source: %d", calls)
				}
			} else {
				if calls != int32(tc.frames) {
					t.Fatalf("frame binding count=%d want=%d", calls, tc.frames)
				}
				var value struct {
					Data     []any `json:"data"`
					HasMore  bool  `json:"hasMore"`
					Complete bool  `json:"complete"`
					Limit    int   `json:"limit"`
				}
				data, _ := json.Marshal(result.StructuredContent)
				if err := json.Unmarshal(data, &value); err != nil {
					t.Fatal(err)
				}
				if len(value.Data) != 1 || !value.HasMore || value.Complete || value.Limit != 1 {
					t.Fatalf("wrapper paging: %s", data)
				}
			}
		}
	}

}

// A Runtime-selected input policy retains the exact prepared source route.
// Its native preparation proof admits this handoff without caller seed authority.
type PreparationSelectedInputHolder struct {
	Contract xdatly.Component[sourceInputPolicy, sourcePolicyOutput] `component:"Spend,path=/spend/{tenant},method=GET"`
}

func (PreparationSelectedInputHolder) DatlyQuerySelectors() preparationReportSelectors {
	panic("metadata only")
}
func TestComposePreparationSelectedInputContractBoundary(t *testing.T) {
	h := (groupedReportHarnessConfig{input: reflect.TypeFor[sourceInputPolicy](), output: reflect.TypeFor[sourcePolicyOutput](), report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) { s.Compose = &spec.CubeComposeSettings{Enabled: true} }}, source: func(c *spec.Component, _ *typecatalog.Catalog) {
		c.Key.Scope = reflect.TypeFor[PreparationSelectedInputHolder]().PkgPath()
		association, err := model.New(x.NewType(reflect.TypeFor[PreparationSelectedInputHolder]()), "Contract", x.NewType(reflect.TypeFor[preparationReportSelectors]()))
		if err != nil {
			t.Fatal(err)
		}
		c.SelectorCompanion = association
		c.RootView.Selector.AllowPage = true
	}}).build(t)
	tool, _ := h.service.Registry().ToolRegistry.Get("SpendCubeCompose")
	result, protocolErr := tool.Handler(t.Context(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: map[string]any{"cubes": []any{map[string]any{"filters": map[string]any{"accountIDs": "1,2", "tenant": "acme", "region": "EU", "channel": "web"}}}, "sql": "SELECT t1.AccountID FROM $CubeSQL1 AS t1 LIMIT 8"}}})
	raw, _ := json.Marshal(result)
	t.Logf("selected input boundary result: %s protocol=%v", raw, protocolErr)
	if protocolErr != nil || result == nil || result.IsError != nil && *result.IsError {
		t.Fatalf("selected input rejected: %s", raw)
	}
	request := httptest.NewRequest("POST", "/spend/acme/cube/compose", strings.NewReader(`{"cubes":[{"filters":{"accountIDs":"1,2","tenant":"acme","region":"EU","channel":"web"}}],"sql":"SELECT t1.AccountID FROM $CubeSQL1 AS t1 LIMIT 8"}`))
	request.Header.Set("Content-Type", "application/json")
	providers, err := requestprovider.New(request)
	if err != nil {
		t.Fatal(err)
	}
	defer providers.Close()
	target, ok := h.runtime.ComponentTargetByMethodPath("POST", "/spend/{tenant}/cube/compose")
	if !ok {
		t.Fatal("target missing")
	}
	_, err = h.runtime.InvokeComponent(t.Context(), exec.ComponentRequest{Target: target, Providers: providers.Providers()})
	t.Logf("direct selected input boundary cause: %T %v", err, err)
	if err != nil {
		t.Fatalf("selected input preparation failed: %v", err)
	}
}
