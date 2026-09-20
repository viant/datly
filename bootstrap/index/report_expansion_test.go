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
