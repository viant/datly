package resource

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

type resourceInput struct {
	ID    string
	Limit int
	Tags  []int
}

func TestCompilerAndCatalogResolveTemplate(t *testing.T) {
	contract := newResourceContract(t, "/orders/{id}", []bindly.BindingSpec{
		{Path: "ID", Location: bindstate.Location{Kind: "path", In: "id"}},
		{Path: "Limit", Location: bindstate.Location{Kind: "query", In: "limit"}},
		{Path: "Tags", Location: bindstate.Location{Kind: "query", In: "tag"}},
	})
	compiler, err := NewCompiler("datly://localhost")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := compiler.Compile(Input{
		Component: spec.Key{Kind: spec.KindComponent, Name: "Order"},
		Route:     &spec.Route{Method: "GET", Path: "/orders/{id}"},
		Exposure:  &spec.MCPExposure{Kind: spec.MCPExposureResourceTemplate, Name: "orders", MIMEType: "application/json"},
		Contract:  contract,
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata, ok := plan.Template()
	if !ok || metadata.UriTemplate != "datly://localhost/orders/{id}{?limit,tag}" {
		t.Fatalf("Template()=%+v ok=%v", metadata, ok)
	}
	catalog, err := NewCatalog([]*Plan{plan})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := catalog.Resolve("datly://localhost/orders/a%2Fb?tag=2&tag=3&limit=5")
	if err != nil {
		t.Fatal(err)
	}
	pathValue, found, err := resolved.Scope().Path().Locate(nil).Value(context.Background(), reflect.TypeOf(""), "id")
	if err != nil || !found || pathValue != "a/b" {
		t.Fatalf("path=%#v found=%v err=%v", pathValue, found, err)
	}
	queryValue, found, err := resolved.Scope().Query().Locate(nil).Value(context.Background(), reflect.TypeOf([]int{}), "tag")
	if err != nil || !found || !reflect.DeepEqual(queryValue, []string{"2", "3"}) {
		t.Fatalf("query=%#v found=%v err=%v", queryValue, found, err)
	}
}

func TestCompilerStaticResource(t *testing.T) {
	contract := newResourceContract(t, "/status", nil)
	compiler, _ := NewCompiler("datly://localhost/api")
	plan, err := compiler.Compile(Input{
		Component: spec.Key{Kind: spec.KindComponent, Name: "Status"},
		Route:     &spec.Route{Method: "GET", Path: "/status"},
		Exposure:  &spec.MCPExposure{Kind: spec.MCPExposureResource, Name: "status"},
		Contract:  contract,
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata, ok := plan.Resource()
	if !ok || metadata.Uri != "datly://localhost/api/status" {
		t.Fatalf("Resource()=%+v ok=%v", metadata, ok)
	}
}

func TestCatalogRejectsInvalidConcreteURI(t *testing.T) {
	contract := newResourceContract(t, "/orders/{id}", []bindly.BindingSpec{
		{Path: "ID", Location: bindstate.Location{Kind: "path", In: "id"}},
		{Path: "Limit", Location: bindstate.Location{Kind: "query", In: "limit"}},
	})
	compiler, _ := NewCompiler("datly://localhost")
	plan, err := compiler.Compile(Input{
		Component: spec.Key{Kind: spec.KindComponent, Name: "Order"}, Route: &spec.Route{Method: "GET", Path: "/orders/{id}"},
		Exposure: &spec.MCPExposure{Kind: spec.MCPExposureResourceTemplate, Name: "orders"}, Contract: contract,
	})
	if err != nil {
		t.Fatal(err)
	}
	catalog, _ := NewCatalog([]*Plan{plan})
	for _, uri := range []string{
		"datly://localhost/orders/%zz", "datly://localhost/orders/1?other=1", "datly://localhost/orders/1?limit=1&limit=2",
		"datly://other/orders/1", "datly://user@localhost/orders/1", "datly://localhost/orders/1#fragment",
	} {
		if _, err := catalog.Resolve(uri); err == nil {
			t.Fatalf("Resolve(%q) expected error", uri)
		}
	}
}

func TestCompilerRejectsContractMismatch(t *testing.T) {
	compiler, _ := NewCompiler("datly://localhost")
	for _, testCase := range []struct {
		name     string
		route    *spec.Route
		exposure *spec.MCPExposure
		bindings []bindly.BindingSpec
	}{
		{name: "non GET", route: &spec.Route{Method: "POST", Path: "/orders/{id}"}, exposure: &spec.MCPExposure{Kind: spec.MCPExposureResourceTemplate, Name: "orders"}, bindings: []bindly.BindingSpec{{Path: "ID", Location: bindstate.Location{Kind: "path", In: "id"}}}},
		{name: "missing path", route: &spec.Route{Method: "GET", Path: "/orders/{id}"}, exposure: &spec.MCPExposure{Kind: spec.MCPExposureResourceTemplate, Name: "orders"}},
		{name: "static with query", route: &spec.Route{Method: "GET", Path: "/orders"}, exposure: &spec.MCPExposure{Kind: spec.MCPExposureResource, Name: "orders"}, bindings: []bindly.BindingSpec{{Path: "Limit", Location: bindstate.Location{Kind: "query", In: "limit"}}}},
		{name: "path query collision", route: &spec.Route{Method: "GET", Path: "/orders/{id}"}, exposure: &spec.MCPExposure{Kind: spec.MCPExposureResourceTemplate, Name: "orders"}, bindings: []bindly.BindingSpec{{Path: "ID", Location: bindstate.Location{Kind: "path", In: "id"}}, {Path: "Limit", Location: bindstate.Location{Kind: "query", In: "id"}}}},
		{name: "required header", route: &spec.Route{Method: "GET", Path: "/orders"}, exposure: &spec.MCPExposure{Kind: spec.MCPExposureResource, Name: "orders"}, bindings: []bindly.BindingSpec{{Path: "ID", Location: bindstate.Location{Kind: "header", In: "X-ID"}, Required: boolPointer(true)}}},
		{name: "required cookie", route: &spec.Route{Method: "GET", Path: "/orders"}, exposure: &spec.MCPExposure{Kind: spec.MCPExposureResource, Name: "orders"}, bindings: []bindly.BindingSpec{{Path: "ID", Location: bindstate.Location{Kind: "cookie", In: "id"}, Required: boolPointer(true)}}},
		{name: "required form", route: &spec.Route{Method: "GET", Path: "/orders"}, exposure: &spec.MCPExposure{Kind: spec.MCPExposureResource, Name: "orders"}, bindings: []bindly.BindingSpec{{Path: "ID", Location: bindstate.Location{Kind: "form", In: "id"}, Required: boolPointer(true)}}},
		{name: "required body", route: &spec.Route{Method: "GET", Path: "/orders"}, exposure: &spec.MCPExposure{Kind: spec.MCPExposureResource, Name: "orders"}, bindings: []bindly.BindingSpec{{Path: "ID", Location: bindstate.Location{Kind: "body"}, Required: boolPointer(true)}}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			projectionBindings := testCase.bindings
			if testCase.name == "path query collision" {
				projectionBindings = []bindly.BindingSpec{
					{Path: "ID", Name: "ID", Location: bindstate.Location{Kind: "param"}},
					{Path: "Limit", Name: "Limit", Location: bindstate.Location{Kind: "param"}},
				}
			}
			contract := newResourceContractWithProjection(t, testCase.route.Path, testCase.bindings, projectionBindings)
			_, err := compiler.Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Name: "Order"}, Route: testCase.route, Exposure: testCase.exposure, Contract: contract})
			if err == nil {
				t.Fatal("Compile() expected error")
			}
		})
	}
}

func boolPointer(value bool) *bool {
	return &value
}

func TestCatalogUsesQueryDeclarationsToDisambiguateTemplates(t *testing.T) {
	compiler, _ := NewCompiler("datly://localhost")
	first := compileResourcePlan(t, compiler, "first", "/items/{id}", []bindly.BindingSpec{
		{Path: "ID", Location: bindstate.Location{Kind: "path", In: "id"}},
		{Path: "Limit", Location: bindstate.Location{Kind: "query", In: "limit"}},
	})
	second := compileResourcePlan(t, compiler, "second", "/items/{id}", []bindly.BindingSpec{
		{Path: "ID", Location: bindstate.Location{Kind: "path", In: "id"}},
		{Path: "Tags", Location: bindstate.Location{Kind: "query", In: "tag"}},
	})
	catalog, err := NewCatalog([]*Plan{first, second})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := catalog.Resolve("datly://localhost/items/1?limit=2")
	if err != nil || resolved.Plan() != first {
		t.Fatalf("Resolve() plan=%p err=%v, want first", resolved.Plan(), err)
	}
	if _, err := catalog.Resolve("datly://localhost/items/1"); err == nil {
		t.Fatal("Resolve() expected ambiguous template error")
	}
}

func compileResourcePlan(t *testing.T, compiler *Compiler, name, path string, bindings []bindly.BindingSpec) *Plan {
	t.Helper()
	contract := newResourceContract(t, path, bindings)
	plan, err := compiler.Compile(Input{
		Component: spec.Key{Kind: spec.KindComponent, Name: name}, Route: &spec.Route{Method: "GET", Path: path},
		Exposure: &spec.MCPExposure{Kind: spec.MCPExposureResourceTemplate, Name: name}, Contract: contract,
	})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func newResourceContract(t *testing.T, path string, bindings []bindly.BindingSpec) *registry.RouteInputContract {
	return newResourceContractWithProjection(t, path, bindings, bindings)
}

func newResourceContractWithProjection(t *testing.T, path string, bindings, projectionBindings []bindly.BindingSpec) *registry.RouteInputContract {
	t.Helper()
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := injector.CompilePlan(reflect.TypeOf(resourceInput{}), bindings...)
	if err != nil {
		t.Fatal(err)
	}
	route := spec.RouteRef{Method: "GET", Path: path}
	projectionPlan, err := injector.CompilePlan(reflect.TypeOf(resourceInput{}), projectionBindings...)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := projectionPlan.Projection()
	if err != nil {
		t.Fatal(err)
	}
	contract, err := registry.NewInputContract(reflect.TypeOf(resourceInput{}), projection, registry.RouteInput{Route: route, Plan: plan, Bindings: bindings})
	if err != nil {
		t.Fatal(err)
	}
	result, ok := contract.ForRoute(route)
	if !ok {
		t.Fatal("route contract missing")
	}
	return result
}
