package mcp

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/viant/bindly"
	bindresource "github.com/viant/bindly/resource"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
)

type serviceInput struct {
	Query string `json:"query"`
}

type serviceResourceInput struct {
	ID string
}

type serviceInvoker struct {
	request exec.ComponentRequest
}

func (i *serviceInvoker) InvokeComponent(_ context.Context, request exec.ComponentRequest) (interface{}, error) {
	i.request = request
	return map[string]interface{}{"query": "ok"}, nil
}

func TestNewBuildsAtomicToolCatalogAndRegistry(t *testing.T) {
	resources := bindresource.New()
	if err := resources.Register("", fstest.MapFS{"docs/search.md": &fstest.MapFile{Data: []byte("Detailed search help.")}}); err != nil {
		t.Fatal(err)
	}
	component := serviceComponent(t, "Search", &spec.MCPExposure{
		Kind: spec.MCPExposureTool, Name: "search.run", Description: "Search", DescriptionPath: "docs/search.md",
	})
	invoker := &serviceInvoker{}
	service, err := New(Config{Components: []*registry.RegisteredComponent{component}, Invoker: invoker, Resources: resources})
	if err != nil {
		t.Fatal(err)
	}
	if names := service.Catalog().ToolNames(); len(names) != 1 || names[0] != "search.run" {
		t.Fatalf("tool names = %v", names)
	}
	entry, ok := service.Registry().ToolRegistry.Get("search.run")
	if !ok || entry.Metadata.Description == nil || *entry.Metadata.Description != "Search\n\nDetailed search help." {
		t.Fatalf("tool entry = %+v", entry)
	}
	result, protocolErr := entry.Handler(context.Background(), &schema.CallToolRequest{
		Method: schema.MethodToolsCall,
		Params: schema.CallToolRequestParams{Name: "search.run", Arguments: map[string]interface{}{"query": "active"}},
	})
	if protocolErr != nil || testharness.StructuredObject(t, result.StructuredContent)["query"] != "ok" || invoker.request.Input != nil || len(invoker.request.Providers) != 1 {
		t.Fatalf("result=%+v error=%v request=%+v", result, protocolErr, invoker.request)
	}
}

func TestNewRejectsDuplicateToolsBeforePublishing(t *testing.T) {
	first := serviceComponent(t, "First", &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "same"})
	second := serviceComponent(t, "Second", &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "same"})
	service, err := New(Config{Components: []*registry.RegisteredComponent{first, second}, Invoker: &serviceInvoker{}})
	if err == nil || service != nil || !strings.Contains(err.Error(), "duplicate MCP tool") {
		t.Fatalf("service=%+v error=%v", service, err)
	}
}

func TestNewBuildsAtomicResourceCatalogAndRegistry(t *testing.T) {
	template := serviceResourceComponent(t, "Order", &spec.MCPExposure{
		Kind: spec.MCPExposureResourceTemplate, Name: "orders", MIMEType: "application/json",
	})
	static := buildServiceComponent(t, serviceComponentFixture{
		name: "Status", inputType: reflect.TypeOf(struct{}{}),
		route: &spec.Route{Method: "GET", Path: "/status", MCP: []*spec.MCPExposure{{
			Kind: spec.MCPExposureResource, Name: "status", MIMEType: "text/plain",
		}}},
	})
	invoker := &serviceInvoker{}
	service, err := New(Config{
		Components: []*registry.RegisteredComponent{template, static}, Invoker: invoker,
		ResourceBaseURI: "datly://catalog.example/api",
	})
	if err != nil {
		t.Fatal(err)
	}
	resourceCatalog := service.Catalog().ResourceCatalog()
	if names := resourceCatalog.Names(); !reflect.DeepEqual(names, []string{"orders", "status"}) {
		t.Fatalf("resource names = %v", names)
	}
	if _, ok := service.Registry().ResourceRegistry.Get("datly://catalog.example/api/status"); !ok {
		t.Fatal("static resource was not registered")
	}
	templateURI := "datly://catalog.example/api/orders/{id}"
	if _, ok := service.Registry().ResourceTemplateRegistry.Get(templateURI); !ok {
		t.Fatal("resource template was not registered")
	}
	result, protocolErr := service.ReadResource(context.Background(), &schema.ReadResourceRequest{
		Method: schema.MethodResourcesRead,
		Params: schema.ReadResourceRequestParams{Uri: "datly://catalog.example/api/orders/a%2Fb"},
	})
	if protocolErr != nil || len(result.Contents) != 1 || result.Contents[0].Text != `{"query":"ok"}` {
		t.Fatalf("result=%+v error=%+v", result, protocolErr)
	}
	if invoker.request.Target.Component.Name != "Order" || invoker.request.Input != nil || len(invoker.request.Providers) != 1 {
		t.Fatalf("request = %+v", invoker.request)
	}
}

func TestNewRejectsDuplicateResourcesBeforePublishing(t *testing.T) {
	first := serviceResourceComponent(t, "First", &spec.MCPExposure{Kind: spec.MCPExposureResourceTemplate, Name: "same"})
	second := serviceResourceComponent(t, "Second", &spec.MCPExposure{Kind: spec.MCPExposureResourceTemplate, Name: "same"})
	service, err := New(Config{Components: []*registry.RegisteredComponent{first, second}, Invoker: &serviceInvoker{}})
	if err == nil || service != nil || !strings.Contains(err.Error(), "duplicate MCP resource") {
		t.Fatalf("service=%+v error=%v", service, err)
	}
}

func TestNewTemplateOnlyEnablesResourceRead(t *testing.T) {
	component := serviceResourceComponent(t, "Order", &spec.MCPExposure{
		Kind: spec.MCPExposureResourceTemplate, Name: "orders",
	})
	service, err := New(Config{Components: []*registry.RegisteredComponent{component}, Invoker: &serviceInvoker{}})
	if err != nil {
		t.Fatal(err)
	}
	if enabled, ok := service.Registry().Methods.Get(schema.MethodResourcesRead); !ok || !enabled {
		t.Fatal("template-only service did not enable resources/read")
	}
}

func TestNewUsesCanonicalDefaultToolName(t *testing.T) {
	component := serviceComponent(t, "Users", &spec.MCPExposure{Kind: spec.MCPExposureTool})
	component.Component.Routes[0].Name = ""
	service, err := New(Config{Components: []*registry.RegisteredComponent{component}, Invoker: &serviceInvoker{}})
	if err != nil {
		t.Fatal(err)
	}
	if names := service.Catalog().ToolNames(); len(names) != 1 || names[0] != "Users.post_tools" {
		t.Fatalf("tool names = %v", names)
	}
}

func TestNewFailsClosedForUnsupportedExposureAndAPIKey(t *testing.T) {
	tests := []struct {
		name      string
		exposure  *spec.MCPExposure
		configure func(*registry.RegisteredComponent)
		match     string
	}{
		{name: "unknown exposure", exposure: &spec.MCPExposure{Kind: "unknown", Name: "users"}, match: "unknown MCP exposure kind"},
		{name: "API key", exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "users"}, configure: func(component *registry.RegisteredComponent) { component.Component.Routes[0].APIKeyHeader = "X-Key" }, match: "HTTP API key"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			component := serviceComponent(t, "Users", test.exposure)
			if test.configure != nil {
				test.configure(component)
			}
			service, err := New(Config{Components: []*registry.RegisteredComponent{component}, Invoker: &serviceInvoker{}})
			if err == nil || service != nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("service=%+v error=%v", service, err)
			}
		})
	}
}

func serviceComponent(t *testing.T, name string, exposure *spec.MCPExposure) *registry.RegisteredComponent {
	t.Helper()
	required := true
	binding := bindly.BindingSpec{
		Path: "Query", Name: "Query", Location: bindstate.Location{Kind: "query", In: "q"}, Required: &required,
	}
	return buildServiceComponent(t, serviceComponentFixture{
		name: name, inputType: reflect.TypeOf(serviceInput{}), bindings: []bindly.BindingSpec{binding},
		route: &spec.Route{Method: "POST", Path: "/tools", MCP: []*spec.MCPExposure{exposure}},
	})
}

func serviceResourceComponent(t *testing.T, name string, exposure *spec.MCPExposure) *registry.RegisteredComponent {
	t.Helper()
	binding := bindly.BindingSpec{Path: "ID", Location: bindstate.Location{Kind: "path", In: "id"}}
	return buildServiceComponent(t, serviceComponentFixture{
		name: name, inputType: reflect.TypeOf(serviceResourceInput{}), bindings: []bindly.BindingSpec{binding},
		route: &spec.Route{Method: "GET", Path: "/orders/{id}", MCP: []*spec.MCPExposure{exposure}},
	})
}

type serviceComponentFixture struct {
	name      string
	inputType reflect.Type
	route     *spec.Route
	bindings  []bindly.BindingSpec
}

func buildServiceComponent(t *testing.T, fixture serviceComponentFixture) *registry.RegisteredComponent {
	t.Helper()
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := injector.CompilePlan(fixture.inputType, fixture.bindings...)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := plan.Projection()
	if err != nil {
		t.Fatal(err)
	}
	input, err := registry.NewInputContract(fixture.inputType, projection, registry.RouteInput{
		Route: spec.RouteRef{Method: fixture.route.Method, Path: fixture.route.Path}, Plan: plan, Bindings: fixture.bindings,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &registry.RegisteredComponent{
		Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/app", Name: fixture.name}, Name: fixture.name, Routes: []*spec.Route{fixture.route}},
		Input:     input,
	}
}

func TestSharedHTTPRouteKeepsComponentToolInputs(t *testing.T) {
	var entries []*registry.RegisteredComponent
	for _, name := range []string{"first", "second"} {
		entries = append(entries, buildServiceComponent(t, serviceComponentFixture{name: name, inputType: reflect.TypeOf(struct{ Query string }{}), bindings: []bindly.BindingSpec{{Path: "Query", Name: name, Location: bindstate.Location{Kind: "query", In: name}}}, route: &spec.Route{Method: "POST", Path: "/shared", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: name}}}}))
	}
	service, err := New(Config{Components: entries, Invoker: &serviceInvoker{}})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"first", "second"} {
		tool, ok := service.Catalog().Tool(name)
		if !ok || len(tool.Metadata().InputSchema.Properties) != 1 || tool.Metadata().InputSchema.Properties[name] == nil {
			t.Fatalf("wrong component schema for %s", name)
		}
	}
	inputs, err := registry.NewInputCatalog(entries)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inputs.Fields(spec.RouteRef{Method: "POST", Path: "/shared"}); err == nil {
		t.Fatal("ambiguous route-only input lookup accepted")
	}
}

func TestToolOnlyServiceRejectsMalformedRoutes(t *testing.T) {
	for _, path := range []string{"records", "/records?x=1", "/records/{", "/records/{id}/{id}"} {
		t.Run(path, func(t *testing.T) {
			entry := buildServiceComponent(t, serviceComponentFixture{name: "Invalid", inputType: reflect.TypeOf(struct{}{}), route: &spec.Route{Method: "POST", Path: path, MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "invalid"}}}})
			service, err := New(Config{Components: []*registry.RegisteredComponent{entry}, Invoker: &serviceInvoker{}})
			if err == nil || service != nil {
				t.Fatalf("malformed route published: %s", path)
			}
		})
	}
}
