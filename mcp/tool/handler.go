package tool

import (
	"context"

	"github.com/viant/datly/mcp/invocation"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

type Handler struct {
	plan    *Plan
	invoker *invocation.Invoker
}

func NewHandler(plan *Plan, invoker *invocation.Invoker) *Handler {
	return &Handler{plan: plan, invoker: invoker}
}

func (h *Handler) Handle(ctx context.Context, request *schema.CallToolRequest) (*schema.CallToolResult, *jsonrpc.Error) {
	if h == nil || h.plan == nil || h.invoker == nil {
		return nil, jsonrpc.NewInternalError("MCP tool handler is unavailable", nil)
	}
	metadata := h.plan.Metadata()
	if request == nil || request.Params.Name != metadata.Name {
		name := ""
		if request != nil {
			name = request.Params.Name
		}
		return nil, schema.NewUnknownTool(name)
	}
	scope, err := h.plan.Scope(request.Params.Arguments)
	if err != nil {
		return nil, jsonrpc.NewInvalidParamsError(err.Error(), nil)
	}
	execution, protocolErr := h.invoker.Execute(ctx, invocation.Request{
		Target: h.plan.Target(), Scope: scope, Method: schema.MethodToolsCall, URI: h.plan.Target().String(),
	})
	if protocolErr != nil {
		return nil, protocolErr
	}
	return execution.ToolResult(), nil
}
