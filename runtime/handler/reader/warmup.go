package reader

import (
	"context"
	"fmt"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/sqlx"
)

type warmupHandler struct {
	reader  dexec.ReaderWarmer
	input   *registry.RouteInputContract
	request dexec.ReaderWarmupRequest
}

func NewWarmupHandler(reader dexec.Reader, input *registry.RouteInputContract, request dexec.ReaderWarmupRequest) (rhandler.Handler, error) {
	warmer, ok := reader.(dexec.ReaderWarmer)
	if !ok || input == nil {
		return nil, fmt.Errorf("registered reader does not support cache warmup")
	}
	request.Settings = request.Settings.Clone()
	return &warmupHandler{reader: warmer, input: input, request: request}, nil
}

func (h *warmupHandler) Execute(ctx context.Context, invocation rhandler.Invocation) (any, error) {
	return h.reader.Warmup(ctx, dexec.ReaderWarmupInvocation{Request: h.request, Input: invocation.Input, Binder: invocation.Binder, Parameters: sqlx.ParameterResolver(h.input.Resolver(invocation.Input))})
}
