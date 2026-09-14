package server

import (
	"context"

	"github.com/viant/jsonrpc"
	protocolclient "github.com/viant/mcp-protocol/client"
	"github.com/viant/mcp-protocol/schema"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

type protocolClient struct {
	operations protocolclient.Operations
}

func (c *protocolClient) CanElicit() bool {
	return c != nil && c.operations != nil && c.operations.Implements(schema.MethodElicitationCreate)
}

func (c *protocolClient) CanGenerateContent() bool {
	return c != nil && c.operations != nil && c.operations.Implements(schema.MethodSamplingCreateMessage)
}

func (c *protocolClient) Elicit(ctx context.Context, params *schema.ElicitRequestParams) (*schema.ElicitResult, error) {
	if c == nil || c.operations == nil {
		return nil, jsonrpc.NewInternalError("MCP client is unavailable", nil)
	}
	request := &schema.ElicitRequest{Method: schema.MethodElicitationCreate}
	if params != nil {
		request.Params = *params
	}
	result, protocolErr := c.operations.Elicit(ctx, &jsonrpc.TypedRequest[*schema.ElicitRequest]{Request: request})
	if protocolErr != nil {
		return nil, protocolErr
	}
	return result, nil
}

func (c *protocolClient) GenerateContent(ctx context.Context, params *schema.CreateMessageRequestParams) (*schema.CreateMessageResult, error) {
	if c == nil || c.operations == nil {
		return nil, jsonrpc.NewInternalError("MCP client is unavailable", nil)
	}
	request := &schema.CreateMessageRequest{Method: schema.MethodSamplingCreateMessage}
	if params != nil {
		request.Params = *params
	}
	result, protocolErr := c.operations.CreateMessage(ctx, &jsonrpc.TypedRequest[*schema.CreateMessageRequest]{Request: request})
	if protocolErr != nil {
		return nil, protocolErr
	}
	return result, nil
}

var _ xmcp.Client = (*protocolClient)(nil)
