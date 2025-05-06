package extension

import (
	"context"
	"fmt"
	"github.com/viant/jsonrpc"
	"github.com/viant/jsonrpc/transport"
	"github.com/viant/mcp-protocol/client"
	"github.com/viant/mcp-protocol/logger"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/mcp-protocol/server"
)

type (
	Implementer struct {
		*server.DefaultImplementer
		integration *Integration
	}
)

func (i *Implementer) Initialize(ctx context.Context, init *schema.InitializeRequestParams, result *schema.InitializeResult) {
	i.ClientInitialize = init
	result.ProtocolVersion = init.ProtocolVersion
}

// ListResources lists all resources
func (i *Implementer) ListResources(ctx context.Context, request *schema.ListResourcesRequest) (*schema.ListResourcesResult, *jsonrpc.Error) {
	var resources []schema.Resource
	for _, resource := range i.integration.Resources {
		resources = append(resources, resource.Resource)
	}
	return &schema.ListResourcesResult{Resources: resources}, nil
}

// ListResourceTemplates lists all resource templates
func (i *Implementer) ListResourceTemplates(ctx context.Context, request *schema.ListResourceTemplatesRequest) (*schema.ListResourceTemplatesResult, *jsonrpc.Error) {
	var resources []schema.ResourceTemplate
	for _, resource := range i.integration.ResourceTemplates {
		resources = append(resources, resource.ResourceTemplate)
	}
	return &schema.ListResourceTemplatesResult{ResourceTemplates: resources}, nil
}

// ReadResource reads a resource
func (i *Implementer) ReadResource(ctx context.Context, request *schema.ReadResourceRequest) (*schema.ReadResourceResult, *jsonrpc.Error) {
	resourceTemplate, ok := i.integration.ResourceTemplatesByURI[request.Params.Uri]
	if ok {
		contents, err := resourceTemplate.Handler(ctx, &request.Params)
		if err != nil {
			return nil, jsonrpc.NewInternalError(fmt.Sprintf("unable to read resource %v: %v", request.Params.Uri, err), nil)
		}
		return &schema.ReadResourceResult{
			Contents: contents,
		}, nil
	}
	resource, ok := i.integration.ResourcesByURI[request.Params.Uri]
	if !ok {
		return nil, schema.NewResourceNotFound(request.Params.Uri)
	}
	contents, err := resource.Handler(ctx, &request.Params)
	if err != nil {
		return nil, jsonrpc.NewInternalError(fmt.Sprintf("unable to read resource %v: %v", request.Params.Uri, err), nil)
	}
	result := schema.ReadResourceResult{
		Contents: contents,
	}
	return &result, nil
}

// ListTools lists all tools
func (i *Implementer) ListTools(ctx context.Context, request *schema.ListToolsRequest) (*schema.ListToolsResult, *jsonrpc.Error) {
	var resources []schema.Tool
	for _, resource := range i.integration.Tools {
		resources = append(resources, resource.Tool)
	}
	return &schema.ListToolsResult{Tools: resources}, nil
}

// CallTool calls a tool
func (i *Implementer) CallTool(ctx context.Context, request *schema.CallToolRequest) (*schema.CallToolResult, *jsonrpc.Error) {
	tool, ok := i.integration.ToolsByName[request.Params.Name]
	if !ok {
		return nil, schema.NewResourceNotFound(request.Params.Name)
	}
	result, err := tool.Handler(ctx, &request.Params)
	if err != nil {
		return nil, jsonrpc.NewInternalError(fmt.Sprintf("unable to read tool %v: %v", request.Params.Name, err), nil)
	}
	return result, nil
}

// Implements checks if the method is implemented
func (i *Implementer) Implements(method string) bool {
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

// New creates a new implementer
func New(integration *Integration) server.NewImplementer {
	return func(_ context.Context, notifier transport.Notifier, logger logger.Logger, client client.Operations) server.Implementer {
		base := server.NewDefaultImplementer(notifier, logger, client)
		return &Implementer{
			integration:        integration,
			DefaultImplementer: base,
		}
	}
}
