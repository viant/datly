package tool

import (
	"context"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/exec"

	"github.com/viant/datly/mcp/invocation"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

type Handler struct {
	plan           *Plan
	invoker        *invocation.Invoker
	binding        *exec.ComponentBinding
	requireBinding bool
}

func NewHandler(plan *Plan, invoker *invocation.Invoker) *Handler {
	return &Handler{plan: plan, invoker: invoker}
}

// Deprecated: BindComponent is retained to reject legacy pinned MCP intent.
// Use resource/datasource execution for server-local component bindings.
func (h *Handler) BindComponent(binding *exec.ComponentBinding, required bool) {
	h.requireBinding = required
	if binding != nil {
		copy := *binding
		h.binding = &copy
	}
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
	if err := rejectLegacyComponentBinding(request); err != nil {
		return nil, jsonrpc.NewInvalidParamsError(err.Error(), nil)
	}
	if h.requireBinding || h.binding != nil {
		return nil, jsonrpc.NewInvalidParamsError("MCP component wire binding is unsupported; declare component identity on the resource or datasource", nil)
	}
	selected, selectErr := h.plan.SelectPath(request.Params.Arguments)
	if selectErr != nil {
		return nil, jsonrpc.NewInvalidParamsError(selectErr.Error(), nil)
	}
	var scope *requestprovider.Scope
	defer func() {
		if scope != nil {
			scope.Close()
		}
	}()
	execution, protocolErr := h.invoker.Execute(ctx, invocation.Request{
		Target: selected.Target(), Method: schema.MethodToolsCall, URI: selected.Target().String(),
		PrepareScope: func(effective *exec.EffectiveContract) (*requestprovider.Scope, error) {
			plan, err := selected.EffectivePlan(effective)
			if err != nil {
				return nil, err
			}
			scope, err = plan.scope(ctx, request.Params.Arguments)
			return scope, err
		},
	})
	if protocolErr != nil {
		return nil, protocolErr
	}
	return execution.ToolResult(), nil
}
