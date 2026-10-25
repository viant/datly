// Package server adapts a compiled Datly MCP service to the upstream protocol
// handler. Transport startup and authorization remain separate concerns.
package server

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/viant/datly/observability"
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
	service        Service
	toolsListTtlMs int
	source         *sourceBinding
	context        xmcp.Context
}

// NewHandler creates one upstream handler factory over a compiled Datly service.
func NewHandler(service Service) (protocolserver.NewHandler, error) {
	return newHandler(service, nil)
}

func newHandler(service Service, source *sourceBinding, configuredTtlMs ...int64) (protocolserver.NewHandler, error) {
	ttl := DefaultToolsListTtlMs
	if len(configuredTtlMs) > 0 {
		ttl = configuredTtlMs[0]
	}
	if ttl < 0 || ttl > int64(^uint(0)>>1) || ttl > int64((1<<63-1)/time.Millisecond) {
		return nil, fmt.Errorf("MCP tools list TTL must be a nonnegative integer in milliseconds")
	}
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
		return &Handler{DefaultHandler: base, service: service, toolsListTtlMs: int(ttl), source: source, context: &requestContext{client: client}}, nil
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
			// Keep internal causes in the process log, not MCP log notifications.
			log.Printf("MCP preparation failed operation=%q stage=prepare request_id=%d tool=%q error=%q", schema.MethodToolsCall, request.Id, request.Request.Params.Name, prepareErr.Error())
			return nil, jsonrpc.NewInternalError("MCP tool is unavailable", nil)
		}
		h.DefaultHandler.Registry = h.service.Registry()
	}
	result, callErr := h.DefaultHandler.CallTool(h.withContext(ctx), request)
	return attachToolTiming(ctx, result), callErr
}

func attachToolTiming(ctx context.Context, result *schema.CallToolResult) *schema.CallToolResult {
	if result == nil {
		return nil
	}
	phases := observability.ToolTimingSnapshot(ctx)
	if len(phases) == 0 {
		return result
	}
	meta := schema.ResultMetaObject{}
	if result.Meta != nil {
		meta = *result.Meta
	}
	existing, ok := meta.AdditionalProperties.(map[string]interface{})
	if meta.AdditionalProperties != nil && !ok {
		// Preserve an existing result with malformed extension metadata. Timing
		// is diagnostic and must never alter tool execution or its error.
		return result
	}
	extras := make(map[string]interface{}, len(existing)+1)
	for key, value := range existing {
		extras[key] = value
	}
	extras[observability.ToolTimingMetaKey] = phases
	meta.AdditionalProperties = extras
	copy := *result
	copy.Meta = &meta
	return &copy
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
			log.Printf("MCP preparation failed operation=%q stage=prepare request_id=%d error=%q", schema.MethodToolsList, request.Id, prepareErr.Error())
			return nil, jsonrpc.NewInternalError("MCP tools are unavailable", nil)
		}
		h.DefaultHandler.Registry = h.service.Registry()
	}
	result, protocolErr := h.DefaultHandler.ListTools(ctx, request)
	if result != nil {
		visible := result.Tools[:0]
		for _, entry := range result.Tools {
			if authorizeCatalogTool(ctx, h.service, entry.Name, "discover") == nil && authorizeCatalogTool(ctx, h.service, entry.Name, "describe") == nil {
				visible = append(visible, entry)
			}
		}
		result.Tools = visible
		// Catalog visibility is filtered above for this caller. The producer grants
		// only a private metadata lease; tool execution retains its authorization.
		result.CacheScope = schema.ListToolsResultCacheScopePrivate
		result.TtlMs = h.toolsListTtlMs
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
	result, protocolErr, handled := h.listResourceCatalog(h.withContext(ctx), request)
	if !handled {
		result, protocolErr = h.DefaultHandler.ListResources(ctx, request)
	}
	if result != nil {
		visible := result.Resources[:0]
		for _, entry := range result.Resources {
			if authorizeCatalogResource(ctx, h.service, entry.Uri, "discover") == nil && authorizeCatalogResource(ctx, h.service, entry.Uri, "describe") == nil {
				visible = append(visible, entry)
			}
		}
		result.Resources = visible
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
		visible := result.ResourceTemplates[:0]
		for _, entry := range result.ResourceTemplates {
			if authorizeCatalogResource(ctx, h.service, entry.UriTemplate, "discover") == nil && authorizeCatalogResource(ctx, h.service, entry.UriTemplate, "describe") == nil {
				visible = append(visible, entry)
			}
		}
		result.ResourceTemplates = visible
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
	if request.Request == nil {
		return nil, jsonrpc.NewInvalidParamsError("resource request required", nil)
	}
	if guard, ok := h.service.(interface {
		AuthorizeResourceRead(context.Context, string) error
	}); ok && guard.AuthorizeResourceRead(ctx, request.Request.Params.Uri) != nil {
		return nil, jsonrpc.NewInvalidRequest("MCP resource authorization denied", nil)
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
