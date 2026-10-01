package engine

import (
	"context"
	"errors"
	dexec "github.com/viant/datly/exec"

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
		finalizeErr := invokeErrorFinalizer(ctx, finalizer, handlerErr)
		switch {
		case handlerErr != nil && finalizeErr != nil:
			joined := errors.Join(handlerErr, finalizeErr)
			projection := (&outcomeErrors{}).publicError(finalizeErr, handlerErr)
			if projection != nil {
				return result, &publicOutcomeError{BodyError: projection, cause: joined}
			}
			return result, joined
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

// Recover at the callback boundary: completion must still run even when the
// engine is already finishing an earlier failure. Panic values remain private.
func invokeErrorFinalizer(ctx context.Context, finalizer xhandler.ErrorFinalizer, cause error) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = dexec.NewPanicError("output error finalizer", value)
		}
	}()
	return finalizer.Finalize(ctx, cause)
}
