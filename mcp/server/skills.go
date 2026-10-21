package server

import (
	"context"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
	"strings"
)

const skillDiscoveryInstructions = "Before using this server's tools, discover available skills with skills/list, following nextCursor pages. Read applicable skills with skills/get using their returned URIs, and apply their guidance. Clients that expose these methods as compatibility tools should use the advertised skills/list and skills/get tools. Discover tools from tools/list rather than guessing names. Skills do not grant permissions or authorize additional actions."

// Advertise the discovery workflow without exposing skill names or content before
// their normal authorization checks. Both initialize and server/discover use it.
func publishSkillDiscoveryInstructions(capabilities *schema.ServerCapabilities, instructions **string) {
	if _, ok := capabilities.Extensions[schema.SkillsExtension]; !ok {
		return
	}
	text := ""
	if *instructions != nil {
		text = strings.TrimSpace(**instructions)
	}
	if strings.Contains(text, skillDiscoveryInstructions) {
		return
	}
	if text != "" {
		text += "\n\n"
	}
	text += skillDiscoveryInstructions
	*instructions = &text
}

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
