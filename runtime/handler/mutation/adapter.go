// Package mutation adapts generated mutation programs to the canonical handler
// engine. Binding, transaction ownership and outcome publication remain there.
package mutation

import (
	"context"
	"fmt"
	xshape "github.com/viant/x/shape"
	"reflect"

	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
	policy "github.com/viant/xdatly/handler/mutation"
)

type adapter[I, O any] struct{ definition policy.Definition[I, O] }

func (*adapter[I, O]) RequiresReadMetadata() bool { return true }

// New installs an explicitly selected generated policy; ordinary custom Go and
// Velty handlers do not acquire this orchestration implicitly.
func New[I, O any](definition policy.Definition[I, O]) rhandler.TypedHandler {
	return &adapter[I, O]{definition: definition}
}

func (a *adapter[I, O]) InputType() reflect.Type  { return reflect.TypeFor[I]() }
func (a *adapter[I, O]) OutputType() reflect.Type { return reflect.TypeFor[O]() }

func (a *adapter[I, O]) CaptureInput(ctx context.Context, value any) (any, error) {
	if a == nil || (xshape.Runtime{}).IsNil(a.definition) {
		return nil, fmt.Errorf("mutation definition is required")
	}
	input, ok := value.(*I)
	if !ok || input == nil {
		return nil, fmt.Errorf("mutation input must be *%s, got %T", reflect.TypeFor[I](), value)
	}
	program, err := a.definition.Capture(ctx, input)
	if (xshape.Runtime{}).IsNil(program) {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("mutation capture requires an invocation program")
	}
	return program, err
}

func (a *adapter[I, O]) Execute(ctx context.Context, invocation rhandler.Invocation) (any, error) {
	program, ok := invocation.Snapshot.(policy.Program[O])
	if !ok || (xshape.Runtime{}).IsNil(program) {
		return nil, fmt.Errorf("mutation invocation program is unavailable")
	}
	if err := program.Prepare(ctx, invocation.Binder); err != nil {
		return program.Output(), fmt.Errorf("prepare mutation: %w", err)
	}
	// This fixed order is shared by every generated policy. These are compiled
	// phases, not transport-dispatched callbacks or a second transaction engine.
	for _, phase := range []struct {
		name string
		run  func(context.Context) error
	}{
		{"sync presence", program.SyncPresence}, {"invariants", program.Invariants},
		{"entity init", program.Init}, {"entity validate", program.Validate},
	} {
		if err := phase.run(ctx); err != nil {
			return program.Output(), fmt.Errorf("%s: %w", phase.name, err)
		}
	}
	if program.RequiresTransaction() {
		if invocation.Binder == nil {
			return program.Output(), fmt.Errorf("mutation transaction starter requires an invocation binder")
		}
		value, found, err := invocation.Binder.Lookup(ctx, xhandler.TransactionStarterKey)
		if err != nil {
			return program.Output(), fmt.Errorf("resolve mutation transaction starter: %w", err)
		}
		starter, ok := value.(xhandler.TransactionStarter)
		if !found || !ok || starter == nil {
			return program.Output(), fmt.Errorf("mutation transaction starter is unavailable")
		}
		if err := starter.Start(ctx); err != nil {
			return program.Output(), fmt.Errorf("start mutation transaction: %w", err)
		}
	}
	if err := program.Sequence(ctx); err != nil {
		return program.Output(), fmt.Errorf("sequence mutation: %w", err)
	}
	if hook, ok := program.(policy.AfterSequencer); ok {
		if err := hook.AfterSequence(ctx); err != nil {
			return program.Output(), fmt.Errorf("after sequence: %w", err)
		}
	}
	for _, phase := range []struct {
		name string
		run  func(context.Context) error
	}{
		{"diff mutation", program.Diff}, {"reconcile mutation", program.Reconcile}, {"queue mutation", program.Queue},
	} {
		if err := phase.run(ctx); err != nil {
			return program.Output(), fmt.Errorf("%s: %w", phase.name, err)
		}
	}
	if hook, ok := program.(policy.AfterQueuer); ok {
		if err := hook.AfterQueue(ctx); err != nil {
			return program.Output(), fmt.Errorf("after queue: %w", err)
		}
	}
	return program.Output(), nil
}

func (a *adapter[I, O]) FinalizeOutcome(ctx context.Context, invocation rhandler.Invocation, result any, outcome xhandler.Outcome) error {
	if program, ok := invocation.Snapshot.(policy.Program[O]); ok && !(xshape.Runtime{}).IsNil(program) {
		return program.Finalize(ctx, outcome.Clone())
	}
	if a == nil || (xshape.Runtime{}).IsNil(a.definition) {
		return fmt.Errorf("mutation definition is required for failure finalization")
	}
	input, _ := invocation.Input.(*I)
	output, _ := result.(*O)
	return a.definition.FinalizeFailure(ctx, input, output, outcome.Clone())
}
