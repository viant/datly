package reader

import (
	"context"
	"fmt"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/sqlx"
)

type queryHandler struct {
	reader dexec.QueryPreparer
	input  *registry.RouteInputContract
}

func NewQueryHandler(reader dexec.Reader, input *registry.RouteInputContract) (rhandler.Handler, error) {
	preparer, ok := reader.(dexec.QueryPreparer)
	if !ok || input == nil {
		return nil, fmt.Errorf("registered reader does not support query preparation")
	}
	return &queryHandler{reader: preparer, input: input}, nil
}

func (h *queryHandler) Execute(ctx context.Context, invocation rhandler.Invocation) (any, error) {
	return h.reader.PrepareQuery(ctx, invocation.Input, invocation.Binder, sqlx.ParameterResolver(h.input.Resolver(invocation.Input)))
}
