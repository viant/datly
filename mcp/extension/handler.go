package extension

import (
	"context"

	"github.com/viant/datly/view/state"
	"github.com/viant/jsonrpc"
	"github.com/viant/jsonrpc/transport"
	"github.com/viant/mcp-protocol/client"
	"github.com/viant/mcp-protocol/logger"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/mcp-protocol/server"
)

type (
	Handler struct {
		*server.DefaultHandler
	}
)

type mcpContext struct {
	client state.MCPClient
}

func (m *mcpContext) Client() state.MCPClient {
	if m == nil {
		return nil
	}
	return m.client
}

type mcpClient struct {
	operations client.Operations
}

func (m *mcpClient) CanElicit() bool {
	return m != nil && m.operations != nil && m.operations.Implements(schema.MethodElicitationCreate)
}

func (m *mcpClient) CanGenerateContent() bool {
	return m != nil && m.operations != nil && m.operations.Implements(schema.MethodSamplingCreateMessage)
}

func (m *mcpClient) Elicit(ctx context.Context, params *schema.ElicitRequestParams) (*schema.ElicitResult, error) {
	if m == nil || m.operations == nil {
		return nil, jsonrpc.NewInternalError("mcp client unavailable", nil)
	}
	request := &schema.ElicitRequest{Method: schema.MethodElicitationCreate}
	if params != nil {
		request.Params = *params
	}
	result, err := m.operations.Elicit(ctx, &jsonrpc.TypedRequest[*schema.ElicitRequest]{Request: request})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *mcpClient) GenerateContent(ctx context.Context, params *schema.CreateMessageRequestParams) (*schema.CreateMessageResult, error) {
	if m == nil || m.operations == nil {
		return nil, jsonrpc.NewInternalError("mcp client unavailable", nil)
	}
	request := &schema.CreateMessageRequest{Method: schema.MethodSamplingCreateMessage}
	if params != nil {
		request.Params = *params
	}
	result, err := m.operations.CreateMessage(ctx, &jsonrpc.TypedRequest[*schema.CreateMessageRequest]{Request: request})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (i *Handler) withMCPContext(ctx context.Context) context.Context {
	if i == nil || i.DefaultHandler == nil || i.DefaultHandler.Client == nil {
		return ctx
	}
	return state.WithMCPContext(ctx, &mcpContext{client: &mcpClient{operations: i.DefaultHandler.Client}})
}

// Implements checks if the method is implemented
func (i *Handler) Implements(method string) bool {
	switch method {
	case schema.MethodResourcesList,
		schema.MethodResourcesTemplatesList,
		schema.MethodResourcesRead,
		schema.MethodSubscribe,
		schema.MethodUnsubscribe,
		schema.MethodToolsList,
		schema.MethodToolsCall:
		return true
	}
	return false
}

func (i *Handler) ReadResource(ctx context.Context, request *jsonrpc.TypedRequest[*schema.ReadResourceRequest]) (*schema.ReadResourceResult, *jsonrpc.Error) {
	return i.DefaultHandler.ReadResource(i.withMCPContext(ctx), request)
}

func (i *Handler) CallTool(ctx context.Context, request *jsonrpc.TypedRequest[*schema.CallToolRequest]) (*schema.CallToolResult, *jsonrpc.Error) {
	return i.DefaultHandler.CallTool(i.withMCPContext(ctx), request)
}

// New creates a new implementer
func New(registry *server.Registry) server.NewHandler {
	return func(_ context.Context, notifier transport.Notifier, logger logger.Logger, client client.Operations) (server.Handler, error) {
		base := server.NewDefaultHandler(notifier, logger, client)
		base.Registry = registry
		return &Handler{
			DefaultHandler: base,
		}, nil
	}
}
