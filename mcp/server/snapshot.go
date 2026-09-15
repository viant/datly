package server

import (
	"context"

	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

type releaseKey struct{}

// WithRelease attaches application-generation ownership to one protocol call.
func WithRelease(ctx context.Context, release func()) context.Context {
	return context.WithValue(ctx, releaseKey{}, release)
}

func releasePinned(ctx context.Context) {
	if release, ok := ctx.Value(releaseKey{}).(func()); ok && release != nil {
		release()
	}
}

func hasRelease(ctx context.Context) bool {
	_, ok := ctx.Value(releaseKey{}).(func())
	return ok
}

// Release releases a generation pinned by Source.Pin when a caller uses the
// source directly instead of the protocol handler.
func Release(ctx context.Context) { releasePinned(ctx) }

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
	if !hasRelease(ctx) {
		if owned, ok := h.source.source.(interface {
			PinOwned(context.Context) (context.Context, ServerService, error)
		}); ok {
			var pinErr error
			var ownedService ServerService
			ctx, ownedService, pinErr = owned.PinOwned(ctx)
			if pinErr != nil {
				return ctx, nil, jsonrpc.NewInternalError("MCP service unavailable", nil)
			}
			ctx = context.WithValue(ctx, pinnedServiceKey{h.source}, ownedService)
		}
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
	if h.source == nil {
		return h.DefaultHandler.Implements(method)
	}
	ctx, err := h.source.pin(ctx)
	if err != nil {
		return false
	}
	if hasRelease(ctx) {
		defer releasePinned(ctx)
	}
	implemented := false
	switch method {
	case schema.MethodToolsList, schema.MethodToolsCall, schema.MethodResourcesList, schema.MethodResourcesTemplatesList, schema.MethodResourcesRead:
		implemented = true
	}
	service, ok := h.source.service(ctx)
	if !ok {
		return false
	}
	base := *h.DefaultHandler
	base.Registry = service.Registry()
	return implemented || base.Implements(method)
}

func (h *Handler) Initialize(ctx context.Context, input *schema.InitializeRequestParams, result *schema.InitializeResult) {
	ctx, snapshot, err := h.forRequest(ctx)
	if err != nil {
		return
	}
	defer releasePinned(ctx)
	snapshot.DefaultHandler.Initialize(ctx, input, result)
	h.ClientInitialize = snapshot.ClientInitialize
}

func (h *Handler) Discover(ctx context.Context, result *schema.DiscoverResult) {
	ctx, snapshot, err := h.forRequest(ctx)
	if err != nil {
		return
	}
	defer releasePinned(ctx)
	snapshot.DefaultHandler.Discover(ctx, result)
}
