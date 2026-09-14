package server

import (
	"context"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

func (h *Handler) ListSkills(ctx context.Context, request *jsonrpc.TypedRequest[*schema.ListSkillsRequest]) (*schema.ListSkillsResult, *jsonrpc.Error) {
	ctx, snapshot, err := h.forRequest(ctx)
	if err != nil {
		return nil, err
	}
	return snapshot.DefaultHandler.ListSkills(ctx, request)
}

func (h *Handler) GetSkill(ctx context.Context, request *jsonrpc.TypedRequest[*schema.GetSkillRequest]) (*schema.GetSkillResult, *jsonrpc.Error) {
	ctx, snapshot, err := h.forRequest(ctx)
	if err != nil {
		return nil, err
	}
	return snapshot.DefaultHandler.GetSkill(ctx, request)
}
