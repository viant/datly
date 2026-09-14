package mcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/viant/datly/exec"
	mcpserver "github.com/viant/datly/mcp/server"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/response"
)

type protocolResourceInvoker struct {
	request exec.ComponentRequest
}

func (i *protocolResourceInvoker) InvokeComponent(_ context.Context, request exec.ComponentRequest) (interface{}, error) {
	i.request = request
	switch request.Target.Component.Name {
	case "Order":
		return map[string]interface{}{"status": "ready"}, nil
	case "Status":
		return response.NewBuffered(response.WithBytes([]byte("ready"))), nil
	case "Logo":
		return response.NewBuffered(response.WithBytes([]byte{0, 1, 2})), nil
	default:
		return nil, nil
	}
}

func TestProtocolHandlerExecutesDatlyResources(t *testing.T) {
	invoker := &protocolResourceInvoker{}
	service, err := New(Config{
		Components: []*registry.RegisteredComponent{
			serviceResourceComponent(t, "Order", &spec.MCPExposure{
				Kind: spec.MCPExposureResourceTemplate, Name: "orders", MIMEType: "application/json",
			}),
			staticProtocolResource(t, "Status", "/status", "status", "text/plain"),
			staticProtocolResource(t, "Logo", "/logo", "logo", "image/png"),
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

	t.Run("escaped template JSON", func(t *testing.T) {
		content := readProtocolResource(t, handler, "datly://localhost/orders/a%2Fb")
		assertProtocolContent(t, content, `{"status":"ready"}`, "")
		if invoker.request.Target.Component.Name != "Order" {
			t.Fatalf("target = %+v", invoker.request.Target)
		}
	})

	t.Run("text", func(t *testing.T) {
		content := readProtocolResource(t, handler, "datly://localhost/status")
		assertProtocolContent(t, content, "ready", "")
	})

	t.Run("binary", func(t *testing.T) {
		content := readProtocolResource(t, handler, "datly://localhost/logo")
		assertProtocolContent(t, content, "", base64.StdEncoding.EncodeToString([]byte{0, 1, 2}))
	})

	t.Run("unknown", func(t *testing.T) {
		result, protocolErr := handler.ReadResource(context.Background(), protocolResourceRequest("datly://localhost/missing"))
		if result != nil || protocolErr == nil || protocolErr.Code != schema.ResourceNotFound {
			t.Fatalf("result=%+v error=%+v", result, protocolErr)
		}
	})
}

func staticProtocolResource(t *testing.T, componentName, path, resourceName, mimeType string) *registry.RegisteredComponent {
	t.Helper()
	return buildServiceComponent(t, serviceComponentFixture{
		name: componentName, inputType: reflect.TypeOf(struct{}{}),
		route: &spec.Route{Method: http.MethodGet, Path: path, MCP: []*spec.MCPExposure{{
			Kind: spec.MCPExposureResource, Name: resourceName, MIMEType: mimeType,
		}}},
	})
}

func readProtocolResource(t *testing.T, handler interface {
	ReadResource(context.Context, *jsonrpc.TypedRequest[*schema.ReadResourceRequest]) (*schema.ReadResourceResult, *jsonrpc.Error)
}, uri string) schema.ReadResourceResultContentsElem {
	t.Helper()
	result, protocolErr := handler.ReadResource(context.Background(), protocolResourceRequest(uri))
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	if result == nil || len(result.Contents) != 1 {
		t.Fatalf("result = %+v", result)
	}
	return result.Contents[0]
}

func protocolResourceRequest(uri string) *jsonrpc.TypedRequest[*schema.ReadResourceRequest] {
	return &jsonrpc.TypedRequest[*schema.ReadResourceRequest]{Request: &schema.ReadResourceRequest{
		Method: schema.MethodResourcesRead,
		Params: schema.ReadResourceRequestParams{Uri: uri},
	}}
}

func assertProtocolContent(t *testing.T, content schema.ReadResourceResultContentsElem, text, blob string) {
	t.Helper()
	payload, err := json.Marshal(content)
	if err != nil {
		t.Fatal(err)
	}
	actual := struct {
		Text *string `json:"text"`
		Blob *string `json:"blob"`
	}{}
	if err := json.Unmarshal(payload, &actual); err != nil {
		t.Fatal(err)
	}
	if actual.Text == nil || *actual.Text != text || actual.Blob == nil || *actual.Blob != blob {
		t.Fatalf("serialized content=%s, want text=%q blob=%q", payload, text, blob)
	}
}
