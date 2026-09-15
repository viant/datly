// Package server adapts a compiled Datly MCP service to the upstream protocol
// handler. Transport startup and authorization remain separate concerns.
package server

import (
	"context"
	"fmt"
	"sort"

	"github.com/viant/jsonrpc"
	"github.com/viant/jsonrpc/transport"
	protocolclient "github.com/viant/mcp-protocol/client"
	"github.com/viant/mcp-protocol/logger"
	"github.com/viant/mcp-protocol/schema"
	protocolserver "github.com/viant/mcp-protocol/server"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

// Service is the immutable MCP catalog and resource execution surface required
// by a protocol handler.
type Service interface {
	Registry() *protocolserver.Registry
	ReadResource(context.Context, *schema.ReadResourceRequest) (*schema.ReadResourceResult, *jsonrpc.Error)
}

type Handler struct {
	*protocolserver.DefaultHandler
	service Service
	source  *sourceBinding
	context xmcp.Context
}

// NewHandler creates one upstream handler factory over a compiled Datly service.
func NewHandler(service Service) (protocolserver.NewHandler, error) {
	return newHandler(service, nil)
}

func newHandler(service Service, source *sourceBinding) (protocolserver.NewHandler, error) {
	if source == nil && (service == nil || service.Registry() == nil) {
		return nil, fmt.Errorf("MCP service and registry are required")
	}
	return func(_ context.Context, notifier transport.Notifier, log logger.Logger, operations protocolclient.Operations) (protocolserver.Handler, error) {
		base := protocolserver.NewDefaultHandler(notifier, log, operations)
		if source != nil {
			// Dynamic services support the catalog operations even while their
			// current generation has no entries. Session capabilities describe
			// that stable protocol surface, not an initial catalog's size.
			base.ServerCapabilities = &schema.ServerCapabilities{Tools: &schema.ServerCapabilitiesTools{}, Resources: &schema.ServerCapabilitiesResources{}}
		}
		if service != nil {
			base.Registry = service.Registry()
		}
		var client xmcp.Client
		if operations != nil {
			client = &protocolClient{operations: operations}
		}
		return &Handler{DefaultHandler: base, service: service, source: source, context: &requestContext{client: client}}, nil
	}, nil
}

func (h *Handler) CallTool(ctx context.Context, request *jsonrpc.TypedRequest[*schema.CallToolRequest]) (*schema.CallToolResult, *jsonrpc.Error) {
	ctx, h, err := h.forRequest(ctx)
	if err != nil {
		return nil, err
	}
	defer releasePinned(ctx)
	if preparer, ok := h.service.(interface {
		PrepareTool(context.Context, string) error
	}); ok && request != nil && request.Request != nil {
		if prepareErr := preparer.PrepareTool(ctx, request.Request.Params.Name); prepareErr != nil {
			return nil, jsonrpc.NewInternalError("MCP tool is unavailable", nil)
		}
		h.DefaultHandler.Registry = h.service.Registry()
	}
	return h.DefaultHandler.CallTool(h.withContext(ctx), request)
}

func (h *Handler) ListTools(ctx context.Context, request *jsonrpc.TypedRequest[*schema.ListToolsRequest]) (*schema.ListToolsResult, *jsonrpc.Error) {
	ctx, h, err := h.forRequest(ctx)
	if err != nil {
		return nil, err
	}
	defer releasePinned(ctx)
	if request == nil {
		request = &jsonrpc.TypedRequest[*schema.ListToolsRequest]{}
	}
	if request.Request == nil {
		copy := *request
		copy.Request = &schema.ListToolsRequest{}
		request = &copy
	}
	if preparer, ok := h.service.(interface{ PrepareTools(context.Context) error }); ok {
		if prepareErr := preparer.PrepareTools(ctx); prepareErr != nil {
			return nil, jsonrpc.NewInternalError("MCP tools are unavailable", nil)
		}
		h.DefaultHandler.Registry = h.service.Registry()
	}
	result, protocolErr := h.DefaultHandler.ListTools(ctx, request)
	if result != nil {
		sort.SliceStable(result.Tools, func(i, j int) bool { return result.Tools[i].Name < result.Tools[j].Name })
	}
	return result, protocolErr
}

func (h *Handler) ListResources(ctx context.Context, request *jsonrpc.TypedRequest[*schema.ListResourcesRequest]) (*schema.ListResourcesResult, *jsonrpc.Error) {
	ctx, h, err := h.forRequest(ctx)
	if err != nil {
		return nil, err
	}
	defer releasePinned(ctx)
	result, protocolErr := h.DefaultHandler.ListResources(ctx, request)
	if result != nil {
		sort.SliceStable(result.Resources, func(i, j int) bool { return result.Resources[i].Uri < result.Resources[j].Uri })
	}
	return result, protocolErr
}

func (h *Handler) ListResourceTemplates(ctx context.Context, request *jsonrpc.TypedRequest[*schema.ListResourceTemplatesRequest]) (*schema.ListResourceTemplatesResult, *jsonrpc.Error) {
	ctx, h, err := h.forRequest(ctx)
	if err != nil {
		return nil, err
	}
	defer releasePinned(ctx)
	result, protocolErr := h.DefaultHandler.ListResourceTemplates(ctx, request)
	if result != nil {
		sort.SliceStable(result.ResourceTemplates, func(i, j int) bool {
			return result.ResourceTemplates[i].UriTemplate < result.ResourceTemplates[j].UriTemplate
		})
	}
	return result, protocolErr
}

func (h *Handler) ReadResource(ctx context.Context, request *jsonrpc.TypedRequest[*schema.ReadResourceRequest]) (*schema.ReadResourceResult, *jsonrpc.Error) {
	ctx, h, err := h.forRequest(ctx)
	if err != nil {
		return nil, err
	}
	defer releasePinned(ctx)
	if request == nil {
		return h.service.ReadResource(h.withContext(ctx), nil)
	}
	if result, err, ok := h.Registry.ReadStaticSkillResource(ctx, request.Request); ok {
		return result, err
	}
	return h.service.ReadResource(h.withContext(ctx), request.Request)
}

func (h *Handler) withContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return xmcp.WithContext(ctx, h.context)
}

type requestContext struct {
	client xmcp.Client
}

func (c *requestContext) Client() xmcp.Client {
	if c == nil {
		return nil
	}
	return c.client
}

var _ protocolserver.Handler = (*Handler)(nil)
var _ xmcp.Context = (*requestContext)(nil)
