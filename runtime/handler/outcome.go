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
