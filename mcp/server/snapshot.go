package server

import (
	"context"

	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

// forRequest copies only the invocation adapter; connection/client state remains
// owned by the original upstream handler. Published registries are never changed.
func (h *Handler) forRequest(ctx context.Context) (context.Context, *Handler, *jsonrpc.Error) {
	if h.source == nil {
		return ctx, h, nil
	}
	ctx, err := h.source.pin(ctx)
	if err != nil {
		return ctx, nil, jsonrpc.NewInternalError("MCP service unavailable", nil)
	}
	service, _ := h.source.service(ctx)
	copy := *h
	base := *h.DefaultHandler
	base.Registry = service.Registry()
	copy.DefaultHandler = &base
	copy.service = service
	copy.source = nil
	return ctx, &copy, nil
}

// ImplementsContext is consumed at the native JSON-RPC ingress after pinning.
func (h *Handler) ImplementsContext(ctx context.Context, method string) bool {
	_, snapshot, err := h.forRequest(ctx)
	if h.source != nil && err == nil {
		switch method {
		case schema.MethodToolsList, schema.MethodToolsCall, schema.MethodResourcesList, schema.MethodResourcesTemplatesList, schema.MethodResourcesRead:
			return true
		}
	}
	return err == nil && snapshot.DefaultHandler.Implements(method)
}

func (h *Handler) Initialize(ctx context.Context, input *schema.InitializeRequestParams, result *schema.InitializeResult) {
	ctx, snapshot, err := h.forRequest(ctx)
	if err != nil {
		return
	}
	snapshot.DefaultHandler.Initialize(ctx, input, result)
	h.ClientInitialize = snapshot.ClientInitialize
}

func (h *Handler) Discover(ctx context.Context, result *schema.DiscoverResult) {
	ctx, snapshot, err := h.forRequest(ctx)
	if err != nil {
		return
	}
	snapshot.DefaultHandler.Discover(ctx, result)
}
