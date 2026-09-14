package server

import (
	"context"
	"testing"

	"github.com/viant/jsonrpc"
	protocolclient "github.com/viant/mcp-protocol/client"
	"github.com/viant/mcp-protocol/schema"
	protocolserver "github.com/viant/mcp-protocol/server"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

type handlerService struct {
	registry *protocolserver.Registry
	uri      string
	context  xmcp.Context
}

func (s *handlerService) Registry() *protocolserver.Registry { return s.registry }

func (s *handlerService) ReadResource(ctx context.Context, request *schema.ReadResourceRequest) (*schema.ReadResourceResult, *jsonrpc.Error) {
	s.context, _ = xmcp.LookupContext(ctx)
	if request != nil {
		s.uri = request.Params.Uri
	}
	return &schema.ReadResourceResult{}, nil
}

type clientOperations struct {
	protocolclient.Operations
	methods       map[string]bool
	elicitParams  *schema.ElicitRequestParams
	elicitMethod  string
	elicitError   *jsonrpc.Error
	messageParams *schema.CreateMessageRequestParams
	messageMethod string
	messageError  *jsonrpc.Error
}

func (o *clientOperations) Implements(method string) bool { return o.methods[method] }

func (o *clientOperations) Elicit(_ context.Context, request *jsonrpc.TypedRequest[*schema.ElicitRequest]) (*schema.ElicitResult, *jsonrpc.Error) {
	o.elicitParams = &request.Request.Params
	o.elicitMethod = request.Request.Method
	return &schema.ElicitResult{}, o.elicitError
}

func (o *clientOperations) CreateMessage(_ context.Context, request *jsonrpc.TypedRequest[*schema.CreateMessageRequest]) (*schema.CreateMessageResult, *jsonrpc.Error) {
	o.messageParams = &request.Request.Params
	o.messageMethod = request.Request.Method
	return &schema.CreateMessageResult{}, o.messageError
}

func TestHandlerRoutesConcreteResourceThroughDatlyService(t *testing.T) {
	registry := protocolserver.NewRegistry()
	registry.Methods.Put(schema.MethodResourcesRead, true)
	service := &handlerService{registry: registry}
	operations := &clientOperations{methods: map[string]bool{schema.MethodElicitationCreate: true}}
	factory, err := NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := factory(context.Background(), nil, nil, operations)
	if err != nil {
		t.Fatal(err)
	}
	handler := actual.(*Handler)
	uri := "datly://localhost/orders/a%2Fb"
	_, protocolErr := handler.ReadResource(context.Background(), &jsonrpc.TypedRequest[*schema.ReadResourceRequest]{
		Request: &schema.ReadResourceRequest{Method: schema.MethodResourcesRead, Params: schema.ReadResourceRequestParams{Uri: uri}},
	})
	if protocolErr != nil || service.uri != uri {
		t.Fatalf("uri=%q error=%v", service.uri, protocolErr)
	}
	if service.context == nil || service.context.Client() == nil || !service.context.Client().CanElicit() {
		t.Fatalf("MCP context = %#v", service.context)
	}
}

func TestHandlerPropagatesClientCapabilitiesToTools(t *testing.T) {
	registry := protocolserver.NewRegistry()
	operations := &clientOperations{methods: map[string]bool{
		schema.MethodElicitationCreate:     true,
		schema.MethodSamplingCreateMessage: true,
	}}
	registry.RegisterTool(&protocolserver.ToolEntry{
		Metadata: schema.Tool{Name: "capabilities"},
		Handler: func(ctx context.Context, _ *schema.CallToolRequest) (*schema.CallToolResult, *jsonrpc.Error) {
			actual, ok := xmcp.LookupContext(ctx)
			if !ok || !actual.Client().CanElicit() || !actual.Client().CanGenerateContent() {
				t.Fatalf("MCP context = %#v", actual)
			}
			return &schema.CallToolResult{}, nil
		},
	})
	factory, err := NewHandler(&handlerService{registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	actual, _ := factory(context.Background(), nil, nil, operations)
	_, protocolErr := actual.CallTool(context.Background(), &jsonrpc.TypedRequest[*schema.CallToolRequest]{
		Request: &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "capabilities"}},
	})
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
}

func TestProtocolClientDelegatesRequests(t *testing.T) {
	operations := &clientOperations{methods: map[string]bool{}}
	client := &protocolClient{operations: operations}
	elicit := &schema.ElicitRequestParams{Message: "choose"}
	if _, err := client.Elicit(context.Background(), elicit); err != nil || operations.elicitMethod != schema.MethodElicitationCreate || operations.elicitParams.Message != elicit.Message {
		t.Fatalf("elicit method=%q want=%q message=%q wantMessage=%q params=%+v error=%v", operations.elicitMethod, schema.MethodElicitationCreate, operations.elicitParams.Message, elicit.Message, operations.elicitParams, err)
	}
	message := &schema.CreateMessageRequestParams{MaxTokens: 17}
	if _, err := client.GenerateContent(context.Background(), message); err != nil || operations.messageMethod != schema.MethodSamplingCreateMessage || operations.messageParams.MaxTokens != message.MaxTokens {
		t.Fatalf("message method=%q want=%q params=%+v error=%v", operations.messageMethod, schema.MethodSamplingCreateMessage, operations.messageParams, err)
	}
	if (&protocolClient{}).CanElicit() || (&protocolClient{}).CanGenerateContent() {
		t.Fatal("unavailable client reported capabilities")
	}
}

func TestProtocolClientTranslatesProtocolErrorsWithoutTypedNil(t *testing.T) {
	operations := &clientOperations{methods: map[string]bool{}}
	client := &protocolClient{operations: operations}
	if _, err := client.Elicit(context.Background(), nil); err != nil {
		t.Fatalf("nil protocol error became non-nil error: %v", err)
	}
	want := jsonrpc.NewInvalidParamsError("invalid elicitation", nil)
	operations.elicitError = want
	if _, err := client.Elicit(context.Background(), nil); err != want {
		t.Fatalf("error=%v, want %v", err, want)
	}
	if _, err := (&protocolClient{}).GenerateContent(context.Background(), nil); err == nil {
		t.Fatal("unavailable client expected an error")
	}
}

func TestNewHandlerRejectsMissingService(t *testing.T) {
	if handler, err := NewHandler(nil); err == nil || handler != nil {
		t.Fatalf("handler=%v error=%v", handler, err)
	}
	if handler, err := NewHandler(&handlerService{}); err == nil || handler != nil {
		t.Fatalf("handler=%v error=%v", handler, err)
	}
}
