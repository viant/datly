package server

import (
	"context"

	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

func (h *Handler) OnNotification(ctx context.Context, notification *jsonrpc.Notification) {
	ctx, snapshot, err := h.forRequest(ctx)
	if err != nil {
		return
	}
	defer releasePinned(ctx)
	snapshot.DefaultHandler.OnNotification(ctx, notification)
}

func (h *Handler) Subscribe(ctx context.Context, request *jsonrpc.TypedRequest[*schema.SubscribeRequest]) (*schema.SubscribeResult, *jsonrpc.Error) {
	ctx, snapshot, err := h.forRequest(ctx)
	if err != nil {
		return nil, err
	}
	defer releasePinned(ctx)
	return snapshot.DefaultHandler.Subscribe(ctx, request)
}

func (h *Handler) Unsubscribe(ctx context.Context, request *jsonrpc.TypedRequest[*schema.UnsubscribeRequest]) (*schema.UnsubscribeResult, *jsonrpc.Error) {
	ctx, snapshot, err := h.forRequest(ctx)
	if err != nil {
		return nil, err
	}
	defer releasePinned(ctx)
	return snapshot.DefaultHandler.Unsubscribe(ctx, request)
}

func (h *Handler) ListPrompts(ctx context.Context, request *jsonrpc.TypedRequest[*schema.ListPromptsRequest]) (*schema.ListPromptsResult, *jsonrpc.Error) {
	ctx, snapshot, err := h.forRequest(ctx)
	if err != nil {
		return nil, err
	}
	defer releasePinned(ctx)
	return snapshot.DefaultHandler.ListPrompts(ctx, request)
}

func (h *Handler) GetPrompt(ctx context.Context, request *jsonrpc.TypedRequest[*schema.GetPromptRequest]) (*schema.GetPromptResult, *jsonrpc.Error) {
	ctx, snapshot, err := h.forRequest(ctx)
	if err != nil {
		return nil, err
	}
	defer releasePinned(ctx)
	return snapshot.DefaultHandler.GetPrompt(ctx, request)
}

func (h *Handler) Complete(ctx context.Context, request *jsonrpc.TypedRequest[*schema.CompleteRequest]) (*schema.CompleteResult, *jsonrpc.Error) {
	ctx, snapshot, err := h.forRequest(ctx)
	if err != nil {
		return nil, err
	}
	defer releasePinned(ctx)
	return snapshot.DefaultHandler.Complete(ctx, request)
}
