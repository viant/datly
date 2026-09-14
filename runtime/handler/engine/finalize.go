package engine

import (
	"context"
	"errors"

	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

// finalizeBeforeCompletion lets error-aware output participate in the root
// transaction outcome. A returned error prevents commit and rolls back work
// prepared in a locally owned transaction. Preparation errors are supplied by
// the engine before this once-only callback.
func finalizeBeforeCompletion(ctx context.Context, result any, handlerErr error) (any, error) {
	if finalizer, ok := result.(xhandler.ErrorFinalizer); ok {
		finalizeErr := finalizer.Finalize(ctx, handlerErr)
		switch {
		case handlerErr != nil && finalizeErr != nil:
			return result, errors.Join(handlerErr, finalizeErr)
		case handlerErr != nil:
			return result, handlerErr
		case finalizeErr != nil:
			return result, finalizeErr
		}
	}
	return result, handlerErr
}

// finalizeAfterCompletion runs success-only output hooks after the root data
// scope has flushed and completed its locally owned transactions.
func finalizeAfterCompletion(ctx context.Context, result any) (any, error) {
	_, injectorAware := result.(xhandler.InjectorFinalizer)
	if _, errorAware := result.(xhandler.ErrorFinalizer); !errorAware && !injectorAware {
		if finalizer, ok := result.(xhandler.Finalizer); ok {
			if err := finalizer.Finalize(ctx); err != nil {
				return result, err
			}
		}
	}
	if mcpContext, ok := xmcp.LookupContext(ctx); ok {
		if finalizer, ok := result.(xmcp.Finalizer); ok {
			if err := finalizer.FinalizeMCP(ctx, mcpContext); err != nil {
				return result, err
			}
		}
	}
	return result, nil
}

// Nested success/MCP hooks join the existing root outcome queue. A child seal
// supplies no commit evidence and must not publish success before its parent.
type outputCompletion struct{}

func (outputCompletion) FinalizeOutcome(ctx context.Context, _ rhandler.Invocation, result any, outcome xhandler.Outcome) error {
	if outcome.Error != nil {
		return nil
	}
	_, err := finalizeAfterCompletion(ctx, result)
	return err
}
func hasCompletionHooks(ctx context.Context, result any) bool {
	if _, ok := result.(xhandler.Finalizer); ok {
		return true
	}
	if _, ok := xmcp.LookupContext(ctx); ok {
		_, ok = result.(xmcp.Finalizer)
		return ok
	}
	return false
}
