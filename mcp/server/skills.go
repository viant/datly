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
	defer releasePinned(ctx)
	if service, ok := snapshot.service.(interface {
		ListVisibleSkills(context.Context, *string) (*schema.ListSkillsResult, *jsonrpc.Error)
	}); ok {
		var cursor *string
		if request != nil && request.Request != nil {
			cursor = request.Request.Params.Cursor
		}
		return service.ListVisibleSkills(ctx, cursor)
	}
	return snapshot.DefaultHandler.ListSkills(ctx, request)
}

func (h *Handler) GetSkill(ctx context.Context, request *jsonrpc.TypedRequest[*schema.GetSkillRequest]) (*schema.GetSkillResult, *jsonrpc.Error) {
	ctx, snapshot, err := h.forRequest(ctx)
	if err != nil {
		return nil, err
	}
	defer releasePinned(ctx)
	if request == nil || request.Request == nil {
		return nil, jsonrpc.NewInvalidParamsError("skill URI is required", nil)
	}
	uri := request.Request.Params.Uri
	if authorizeCatalogResource(ctx, snapshot.service, uri, "describe") != nil {
		return nil, jsonrpc.NewInvalidParamsError("unknown skill URI", nil)
	}
	result, err := snapshot.DefaultHandler.GetSkill(ctx, request)
	if err == nil && result != nil {
		for _, file := range result.Skill.Resources.Files {
			if authorizeCatalogResource(ctx, snapshot.service, file.Uri, "describe") != nil {
				return nil, jsonrpc.NewInvalidParamsError("unknown skill URI", nil)
			}
		}
	}
	return result, err
}
