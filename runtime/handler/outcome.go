package handler

import (
	"context"
	xhandler "github.com/viant/xdatly/handler"
)

// OutcomeFinalizer is the opt-in bridge used by generic mutation adapters.
// It replaces output Finalizer/ErrorFinalizer/FinalizeMCP for that adapter only,
// and runs once after the owning root completes every database unit. Invocation
// may have a nil Binder and result may be nil after an early binding/init error.
// No MCP sampling hook is invoked. Publication must check CommitConfirmed().
type OutcomeFinalizer interface {
	FinalizeOutcome(context.Context, Invocation, any, xhandler.Outcome) error
}

// EarlyErrorOutputFinalizer opts a mutation adapter into error-aware output
// construction for failures before its execution snapshot exists. Its outcome
// callback remains the sole finalizer and runs after normal root cleanup.
type EarlyErrorOutputFinalizer interface {
	EarlyErrorOutputEnabled() bool
}

// CapturedErrorOutput optionally preserves a canonical captured output after
// an ordinary initializer error. A nil result declines. The additional error
// is joined after the original failure; outcome finalization still owns cleanup.
type CapturedErrorOutput interface {
	CapturedErrorOutput(context.Context, Invocation, error) (any, error)
}
