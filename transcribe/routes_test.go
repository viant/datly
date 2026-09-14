package transcribe

import (
	"context"
	routecompiler "github.com/viant/datly/bootstrap/routes"
	"github.com/viant/datly/spec"
	"testing"
)

func TestWithURIMCPAlternatives(t *testing.T) {
	for _, uri := range []string{"/{id}", "/things/{id}"} {
		source := &Source{Name: "Things", Scope: "test", Text: `#setting($_ = $route('/things','GET'))
#setting($_ = $mcp('Things'))
#define($_ = $Id<int>(path/id).WithURI('` + uri + `'))
SELECT id FROM things`}
		result, err := NewCompiler().Compile(context.Background(), source)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Component.Routes) != 2 || result.Component.Routes[1].Path != "/things/{id}" || result.Component.Routes[1].MCP[0].Name != "ThingsById" {
			t.Fatalf("routes: %+v", result.Component.Routes)
		}
		if result.Component.Parameters[0].Activation.URI != uri {
			t.Fatal("authored activation was mutated")
		}
	}
}
func TestWithURIMCPVisibilityAndSuffix(t *testing.T) {
	disabled := false
	component := &spec.Component{Name: "Things", Routes: []*spec.Route{{Name: "Things", Method: "GET", Path: "/things", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "Things"}}}}, Parameters: []*spec.Parameter{{Name: "Id", Source: spec.BindSource{Kind: "path", Name: "id"}, Activation: &spec.RouteActivation{URI: "/{id}"}, MCP: &disabled}}}
	if err := (routecompiler.Compiler{Component: component}).Compile(); err != nil {
		t.Fatal(err)
	}
	if len(component.Routes[0].MCP) != 0 || len(component.Routes[1].MCP) != 1 {
		t.Fatal("base visibility affected alternate")
	}
	for _, tc := range []struct{ base, path, want string }{{"/things", "/things/{id}", "ById"}, {"/advertisers/{advertiserId}", "/advertisers/{advertiserId}/{campaignId}", "ByCampaignId"}, {"/providers", "/providers/third-party", "ByThirdParty"}} {
		got, err := (routecompiler.Compiler{}).Suffix(tc.base, tc.path)
		if err != nil || got != tc.want {
			t.Fatalf("suffix=%s err=%v want=%s", got, err, tc.want)
		}
	}
}

func TestWithURIPreservesActivationAcrossBaseRoutes(t *testing.T) {
	parameter := &spec.Parameter{Name: "Id", Source: spec.BindSource{Kind: "path", Name: "id"}, Activation: &spec.RouteActivation{URI: "/{id}"}}
	component := &spec.Component{Name: "Things", Routes: []*spec.Route{{Method: "GET", Path: "/things"}, {Method: "GET", Path: "/other"}}, Parameters: []*spec.Parameter{parameter}}
	if err := (routecompiler.Compiler{Component: component}).Compile(); err != nil {
		t.Fatal(err)
	}
	if len(component.Routes) != 4 || parameter.Activation.URI != "/{id}" {
		t.Fatal("lost expanded activation")
	}
	for _, r := range component.Routes[2:] {
		if !parameter.Activation.Matches(r.Path) {
			t.Fatalf("activation does not match %s", r.Path)
		}
	}
}

func TestAbsoluteWithURIHidesOnlyAssociatedBase(t *testing.T) {
	disabled := false
	component := &spec.Component{Routes: []*spec.Route{
		{Method: "GET", Path: "/things", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "Things"}}},
		{Method: "GET", Path: "/other", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "Other"}}},
	}, Parameters: []*spec.Parameter{{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}, Activation: &spec.RouteActivation{URI: "/things/{id}"}, MCP: &disabled}}}
	compiler := routecompiler.Compiler{Component: component}
	if err := compiler.Compile(); err != nil {
		t.Fatal(err)
	}
	if len(component.Routes) != 3 || len(component.Routes[0].MCP) != 0 || len(component.Routes[1].MCP) != 1 || component.Routes[2].MCP[0].Name != "ThingsById" {
		t.Fatal("visibility leaked to unrelated base")
	}
	if err := compiler.Compile(); err != nil {
		t.Fatal(err)
	}
	if len(component.Routes) != 3 || len(component.Routes[2].MCP) != 1 {
		t.Fatal("expansion is not idempotent")
	}
}
