package mcp

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/exec"
	mcpserver "github.com/viant/datly/mcp/server"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/jsonrpc"
)

type protocolQueryInput struct {
	Limit int
}

type protocolCombinedInput struct {
	ID   string
	Tags []int
}

type protocolQueryInvoker struct {
	request exec.ComponentRequest
}

func (i *protocolQueryInvoker) InvokeComponent(_ context.Context, request exec.ComponentRequest) (interface{}, error) {
	i.request = request
	return map[string]interface{}{"status": "ready"}, nil
}

func TestProtocolHandlerBindsResourceQueryTemplates(t *testing.T) {
	invoker := &protocolQueryInvoker{}
	service, err := New(Config{
		Components: []*registry.RegisteredComponent{
			protocolQueryResource(t),
			protocolCombinedResource(t),
		},
		Invoker: invoker,
	})
	if err != nil {
		t.Fatal(err)
	}
	factory, err := mcpserver.NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := factory(context.Background(), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("query only", func(t *testing.T) {
		readProtocolResource(t, handler, "datly://localhost/search?limit=5")
		if actual := protocolProviderValue(t, invoker.request, reflect.TypeOf(0), "limit"); actual != "5" {
			t.Fatalf("limit=%#v", actual)
		}
	})

	t.Run("combined path and repeated query", func(t *testing.T) {
		readProtocolResource(t, handler, "datly://localhost/filtered/a%2Fb?tag=2&tag=3")
		if actual := protocolProviderValue(t, invoker.request, reflect.TypeOf(""), "id"); actual != "a/b" {
			t.Fatalf("id=%#v", actual)
		}
		actual := protocolProviderValue(t, invoker.request, reflect.TypeOf([]int{}), "tag")
		if !reflect.DeepEqual(actual, []string{"2", "3"}) {
			t.Fatalf("tags=%#v", actual)
		}
	})

	for _, uri := range []string{
		"datly://localhost/search?limit=1&limit=2",
		"datly://localhost/filtered/%zz?tag=1",
	} {
		result, protocolErr := handler.ReadResource(context.Background(), protocolResourceRequest(uri))
		if result != nil || protocolErr == nil || protocolErr.Code != jsonrpc.InvalidParams {
			t.Fatalf("URI=%q result=%+v error=%+v", uri, result, protocolErr)
		}
	}
}

func protocolQueryResource(t *testing.T) *registry.RegisteredComponent {
	t.Helper()
	return buildServiceComponent(t, serviceComponentFixture{
		name: "Search", inputType: reflect.TypeOf(protocolQueryInput{}),
		bindings: []bindly.BindingSpec{{Path: "Limit", Location: bindstate.Location{Kind: "query", In: "limit"}}},
		route: &spec.Route{Method: "GET", Path: "/search", MCP: []*spec.MCPExposure{{
			Kind: spec.MCPExposureResourceTemplate, Name: "search",
		}}},
	})
}

func protocolCombinedResource(t *testing.T) *registry.RegisteredComponent {
	t.Helper()
	return buildServiceComponent(t, serviceComponentFixture{
		name: "FilteredOrder", inputType: reflect.TypeOf(protocolCombinedInput{}),
		bindings: []bindly.BindingSpec{
			{Path: "ID", Location: bindstate.Location{Kind: "path", In: "id"}},
			{Path: "Tags", Location: bindstate.Location{Kind: "query", In: "tag"}},
		},
		route: &spec.Route{Method: "GET", Path: "/filtered/{id}", MCP: []*spec.MCPExposure{{
			Kind: spec.MCPExposureResourceTemplate, Name: "filtered-order",
		}}},
	})
}

func protocolProviderValue(t *testing.T, request exec.ComponentRequest, targetType reflect.Type, name string) interface{} {
	t.Helper()
	for _, provider := range request.Providers {
		actual, found, err := provider.Locate(nil).Value(context.Background(), targetType, name)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			return actual
		}
	}
	t.Fatalf("provider value %q was not found", name)
	return nil
}
