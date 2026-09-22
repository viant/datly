package index

import (
	"net/http"
	"testing"

	"github.com/viant/datly/spec"
)

func TestExpandReportEntriesPropagatesInternalRouteVisibility(t *testing.T) {
	groupable := true
	component := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/reports", Name: "Performance"},
		Routes: []*spec.Route{{
			Method: http.MethodGet, Path: "/performance", Internal: true,
			APIKeyHeader: "X-Key", APIKeyValue: "secret",
		}},
		Settings: &spec.Settings{Report: &spec.ReportSettings{
			Enabled: true,
			Compose: &spec.CubeComposeSettings{
				Enabled: true,
			},
		}},
		RootView: &spec.View{Name: "performance", Groupable: &groupable},
	}
	expanded := expandReportEntries([]*Entry{{Component: component, Fingerprint: "base"}})
	if len(expanded) != 3 {
		t.Fatalf("expanded entries = %d, want source, cube, compose", len(expanded))
	}
	for _, entry := range expanded[1:] {
		if entry.Component == nil || len(entry.Component.Routes) != 1 {
			t.Fatalf("expanded entry = %+v", entry)
		}
		route := entry.Component.Routes[0]
		if !route.Internal {
			t.Fatalf("derived route did not inherit internal visibility: %+v", route)
		}
		if route.APIKeyHeader != "X-Key" || route.APIKeyValue != "secret" {
			t.Fatalf("derived route did not preserve API key settings: %+v", route)
		}
	}
}

func TestExpandReportEntriesUsesAuthoredMCPNameForDerivedReports(t *testing.T) {
	groupable := true
	component := &spec.Component{
		Key:         spec.Key{Kind: spec.KindComponent, Scope: "example.com/reports", Name: "spo_performance"},
		Name:        "spo_performance",
		Description: "supply optimization performance",
		Routes: []*spec.Route{{
			Method: http.MethodGet,
			Path:   "/spo/performance",
			MCP: []*spec.MCPExposure{{
				Kind: spec.MCPExposureTool,
				Name: "SupplyOptimizationPerformance",
			}},
		}},
		Settings: &spec.Settings{Report: &spec.ReportSettings{
			Enabled: true,
			Compose: &spec.CubeComposeSettings{
				Enabled: true,
			},
		}},
		RootView: &spec.View{Name: "spo_performance", Groupable: &groupable},
	}

	expanded := expandReportEntries([]*Entry{{Component: component, Fingerprint: "base"}})
	if len(expanded) != 3 {
		t.Fatalf("expanded entries = %d, want source, cube, compose", len(expanded))
	}

	cube := expanded[1].Component
	if cube.Key.Name != "SpoPerformanceCube" {
		t.Fatalf("cube key = %q, want internal key from component name", cube.Key.Name)
	}
	if got := cube.Routes[0].MCP[0].Name; got != "SupplyOptimizationPerformanceCube" {
		t.Fatalf("cube MCP name = %q", got)
	}

	compose := expanded[2].Component
	if compose.Key.Name != "SpoPerformanceCubeCompose" {
		t.Fatalf("compose key = %q, want internal key from cube component name", compose.Key.Name)
	}
	if got := compose.Routes[0].MCP[0].Name; got != "SupplyOptimizationPerformanceCubeCompose" {
		t.Fatalf("compose MCP name = %q", got)
	}
}

func TestExpandReportEntriesMCPNameFallbackAndDisabledExposure(t *testing.T) {
	groupable := true
	component := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/reports", Name: "plain_report"},
		Routes: []*spec.Route{{
			Method: http.MethodGet,
			Path:   "/plain",
		}},
		Settings: &spec.Settings{Report: &spec.ReportSettings{Enabled: true}},
		RootView: &spec.View{Name: "plain_report", Groupable: &groupable},
	}
	expanded := expandReportEntries([]*Entry{{Component: component, Fingerprint: "base"}})
	if len(expanded) != 2 {
		t.Fatalf("expanded entries = %d, want source and cube", len(expanded))
	}
	if got := expanded[1].Component.Routes[0].MCP[0].Name; got != "PlainReportCube" {
		t.Fatalf("fallback MCP name = %q", got)
	}

	disabled := false
	component.Settings.Report.MCPTool = &disabled
	expanded = expandReportEntries([]*Entry{{Component: component, Fingerprint: "base"}})
	if got := expanded[1].Component.Routes[0].MCP; len(got) != 0 {
		t.Fatalf("disabled report MCP exposure = %+v", got)
	}
}
