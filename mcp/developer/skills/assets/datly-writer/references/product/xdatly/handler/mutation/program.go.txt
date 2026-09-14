// Package mutation defines the generated, typed policy consumed by the optional
// mutation handler. These contracts are compiler-facing; applications customize
// focused business hooks rather than reimplementing record traversal.
package mutation

import (
	"context"

	"github.com/viant/xdatly/handler"
)

// Definition is immutable and shared. Capture runs before input Init/InitMCP and
// returns a fresh Program with detached original identity and presence. It must
// not mutate input. A non-nil Program returned alongside an error may finalize
// that failed capture. FinalizeFailure handles failures before a Program exists.
type Definition[I, O any] interface {
	Capture(context.Context, *I) (Program[O], error)
	FinalizeFailure(context.Context, *I, *O, handler.Outcome) error
}

// Finalizer is the unified component completion capability. A prepared root
// EntityHooks object may implement it, reusing invocation-scoped services.
// An explicit definition finalizer may also handle pre-capture failures and
// must then be safe for concurrent invocations. Input/output may be nil on
// early failure. Only Outcome.CommitConfirmed is evidence for commit-dependent
// publication; cancellation must not suppress the completion notification.
type Finalizer[I, O any] interface {
	Finalize(context.Context, *I, *O, handler.Outcome) error
}

// Program owns invocation-local typed state and generated record traversal.
// Prepare binds dependencies and creates business hooks; it must not initialize
// entities or write data. SyncPresence precedes invariant backfill and business
// Init. Diff uses captured original identity, never a newly allocated key.
// Reconcile repairs identity/parent links before Queue. Queue only buffers DML;
// the existing invocation owner completes every transaction before Finalize.
type Program[O any] interface {
	Prepare(context.Context, handler.Binder) error
	SyncPresence(context.Context) error
	Invariants(context.Context) error
	Init(context.Context) error
	Validate(context.Context) error
	RequiresTransaction() bool
	Sequence(context.Context) error
	Diff(context.Context) error
	Reconcile(context.Context) error
	Queue(context.Context) error
	Output() *O
	Finalize(context.Context, handler.Outcome) error
}

// AfterSequencer and AfterQueuer are optional generated delegation points for
// invocation-local business hooks. Neither is proof of a database commit.
type AfterSequencer interface{ AfterSequence(context.Context) error }
type AfterQueuer interface{ AfterQueue(context.Context) error }
