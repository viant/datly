package reader

import (
	"context"
	"fmt"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/sqlx"
)

type dryRunHandler struct {
	reader dexec.ReadPlanner
	input  *registry.RouteInputContract
}

func NewDryRunHandler(reader dexec.Reader, input *registry.RouteInputContract) (rhandler.Handler, error) {
	planner, ok := reader.(dexec.ReadPlanner)
	if !ok || input == nil {
		return nil, fmt.Errorf("registered reader does not support dry-run")
	}
	return &dryRunHandler{reader: planner, input: input}, nil
}
func (h *dryRunHandler) Execute(ctx context.Context, invocation rhandler.Invocation) (any, error) {
	return h.reader.PlanRead(ctx, invocation.Input, invocation.Binder, sqlx.ParameterResolver(h.input.Resolver(invocation.Input)))
}
