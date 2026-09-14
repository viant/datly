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
	"github.com/viant/mcp-protocol/schema"
)

type protocolSlugInput struct {
	Slug string
}

type rejectingProtocolInvoker struct {
	calls int
}

func (i *rejectingProtocolInvoker) InvokeComponent(context.Context, exec.ComponentRequest) (interface{}, error) {
	i.calls++
	return map[string]interface{}{"unexpected": true}, nil
}

func TestProtocolHandlerRejectsInvalidAndAmbiguousResourceURIs(t *testing.T) {
	invoker := &rejectingProtocolInvoker{}
	service, err := New(Config{
		Components: []*registry.RegisteredComponent{
			protocolQueryResource(t),
			protocolAmbiguousResource(t, "ByID", "ID", "id", reflect.TypeOf(protocolCombinedInput{})),
			protocolAmbiguousResource(t, "BySlug", "Slug", "slug", reflect.TypeOf(protocolSlugInput{})),
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

	tests := []struct {
		name    string
		request *jsonrpc.TypedRequest[*schema.ReadResourceRequest]
		code    int
	}{
		{name: "missing URI", request: protocolResourceRequest(""), code: jsonrpc.InvalidParams},
		{name: "undeclared query", request: protocolResourceRequest("datly://localhost/search?other=1"), code: jsonrpc.InvalidParams},
		{name: "malformed escape", request: protocolResourceRequest("datly://localhost/search?limit=%zz"), code: jsonrpc.InvalidParams},
		{name: "fragment", request: protocolResourceRequest("datly://localhost/search?limit=1#private"), code: jsonrpc.InvalidParams},
		{name: "ambiguous template", request: protocolResourceRequest("datly://localhost/lookup/value"), code: jsonrpc.InternalError},
		{name: "nil request", request: nil, code: jsonrpc.InvalidParams},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, protocolErr := handler.ReadResource(context.Background(), test.request)
			if result != nil || protocolErr == nil || protocolErr.Code != test.code {
				t.Fatalf("result=%+v error=%+v, want code %d", result, protocolErr, test.code)
			}
		})
	}
	if invoker.calls != 0 {
		t.Fatalf("invalid resource requests reached invocation %d times", invoker.calls)
	}
}

func protocolAmbiguousResource(t *testing.T, componentName, field, placeholder string, inputType reflect.Type) *registry.RegisteredComponent {
	t.Helper()
	path := "/lookup/{" + placeholder + "}"
	return buildServiceComponent(t, serviceComponentFixture{
		name: componentName, inputType: inputType,
		bindings: []bindly.BindingSpec{{Path: field, Location: bindstate.Location{Kind: "path", In: placeholder}}},
		route: &spec.Route{Method: "GET", Path: path, MCP: []*spec.MCPExposure{{
			Kind: spec.MCPExposureResourceTemplate, Name: componentName,
		}}},
	})
}
