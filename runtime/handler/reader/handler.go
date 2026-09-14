package reader

import (
	"context"
	"fmt"

	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/sqlx"
)

// Handler is the built-in reader flavor over the shared handler engine.
type Handler struct {
	options *dexec.ReaderOptions
	reader  dexec.Reader
	input   *registry.RouteInputContract
}

func NewHandler(reader dexec.Reader, input *registry.RouteInputContract) *Handler {
	return &Handler{reader: reader, input: input}
}

func (h *Handler) Execute(ctx context.Context, invocation rhandler.Invocation) (any, error) {
	if h == nil || h.reader == nil {
		return nil, fmt.Errorf("reader handler is not initialized")
	}
	if h.input == nil {
		return nil, fmt.Errorf("reader input contract is required")
	}
	if h.options != nil {
		ctx = h.options.Context(ctx)
	}
	resolver := sqlx.ParameterResolver(h.input.Resolver(invocation.Input))
	return h.reader.Read(ctx, invocation.Input, invocation.Binder, resolver)
}

// WithReadOptions configures this invocation's result read without changing
// canonical input initialization or the binding of dependency data points.
func (h *Handler) WithReadOptions(options dexec.ReaderOptions) *Handler {
	clone := *h
	clone.options = &options
	return &clone
}
