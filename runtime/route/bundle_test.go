package route

import (
	"testing"

	"github.com/viant/assertly"
	"github.com/viant/datly/spec"
)

func TestNewBundleAndRouteLookup(t *testing.T) {
	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/forecasting", Name: "Total"},
		Name: "Total",
		Routes: []*spec.Route{
			{Method: "GET", Path: "/v1/api/forecasting/total", Name: "total"},
		},
	}

	bundle, err := NewBundle([]*spec.Component{component})
	if err != nil {
		t.Fatalf("unexpected bundle error: %v", err)
	}

	got, ok := bundle.ComponentByRoute("GET", "/v1/api/forecasting/total")
	if !ok {
		t.Fatalf("expected route lookup to succeed")
	}

	assertly.AssertValues(t, component.Key.String(), got.Key.String())
}

func TestNewBundleRejectsDuplicateRoute(t *testing.T) {
	a := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo", Name: "A"},
		Routes: []*spec.Route{
			{Method: "GET", Path: "/v1/items"},
		},
	}
	b := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo", Name: "B"},
		Routes: []*spec.Route{
			{Method: "GET", Path: "/v1/items"},
		},
	}

	if _, err := NewBundle([]*spec.Component{a, b}); err == nil {
		t.Fatalf("expected duplicate route error")
	}
}

func TestBundleRouteTemplateLookup(t *testing.T) {
	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/forecasting", Name: "ByID"},
		Name: "ByID",
		Routes: []*spec.Route{
			{Method: "GET", Path: "/v1/api/vendors/{vendorID}", Name: "by-id"},
		},
	}

	bundle, err := NewBundle([]*spec.Component{component})
	if err != nil {
		t.Fatalf("unexpected bundle error: %v", err)
	}

	got, params, ok := bundle.ComponentByRouteWithParams("GET", "/v1/api/vendors/17")
	if !ok {
		t.Fatalf("expected route template lookup to succeed")
	}
	assertly.AssertValues(t, component.Key.String(), got.Key.String())
	assertly.AssertValues(t, map[string]string{"vendorID": "17"}, params)
}

func TestBundleRouteTemplateLookupPreservesEncodedSlash(t *testing.T) {
	component := &spec.Component{
		Key:    spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo", Name: "ByID"},
		Routes: []*spec.Route{{Method: "GET", Path: "/v1/items/{id}"}},
	}
	bundle, err := NewBundle([]*spec.Component{component})
	if err != nil {
		t.Fatal(err)
	}
	_, params, ok := bundle.ComponentByRouteWithParams("GET", "/v1/items/a%2Fb")
	if !ok || params["id"] != "a/b" {
		t.Fatalf("ComponentByRouteWithParams() params=%v ok=%v", params, ok)
	}
}

func TestNewBundleRejectsConflictingRouteTemplates(t *testing.T) {
	a := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo", Name: "A"},
		Routes: []*spec.Route{
			{Method: "GET", Path: "/v1/items/{itemID}"},
		},
	}
	b := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo", Name: "B"},
		Routes: []*spec.Route{
			{Method: "GET", Path: "/v1/items/{name}"},
		},
	}

	if _, err := NewBundle([]*spec.Component{a, b}); err == nil {
		t.Fatalf("expected duplicate route template error")
	}
}

func TestRouteMetadataLookupPrefersExactRouteOverTemplate(t *testing.T) {
	template := &spec.Route{Method: "GET", Path: "/v1/items/{id}", APIKeyHeader: "X-API-Key", APIKeyValue: "secret"}
	exact := &spec.Route{Method: "GET", Path: "/v1/items/me"}
	bundle, err := NewBundle([]*spec.Component{
		{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo", Name: "Template"}, Routes: []*spec.Route{template}},
		{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo", Name: "Exact"}, Routes: []*spec.Route{exact}},
	})
	if err != nil {
		t.Fatalf("NewBundle() error = %v", err)
	}
	actual, ok := bundle.RouteByMethodPath("GET", "/v1/items/me")
	if !ok || actual != exact {
		t.Fatalf("RouteByMethodPath() = %#v, %v; want exact route", actual, ok)
	}
	component, _, ok := bundle.ComponentByRouteWithParams("GET", "/v1/items/me")
	if !ok || component.Key.Name != "Exact" {
		t.Fatalf("ComponentByRouteWithParams() = %#v, %v; want exact component", component, ok)
	}
}
