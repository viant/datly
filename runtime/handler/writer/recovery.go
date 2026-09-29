package writer

import (
	"context"
	"fmt"
	rhandler "github.com/viant/datly/runtime/handler"
	"reflect"
)

func (h *Handler) SupportsMutationRecovery() bool {
	if h != nil && h.metadata != nil && hasScopedSequences(h.metadata.Root) {
		return true
	}
	if h == nil || h.metadata == nil || h.metadata.Root == nil || h.metadata.Root.HookType == nil {
		return false
	}
	_, ok := reflect.PointerTo(h.metadata.Root.HookType).MethodByName("Recover")
	return ok
}

func (h *Handler) RecoverMutation(ctx context.Context, invocation rhandler.Invocation, _ any, outcome rhandler.MutationOutcome) (rhandler.Recovery, error) {
	program, _ := invocation.Snapshot.(*Program)
	if program == nil || !program.hook.IsValid() {
		return rhandler.RecoveryNone, nil
	}
	method := program.hook.MethodByName("Recover")
	if !method.IsValid() {
		return rhandler.RecoveryNone, nil
	}
	results := method.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(program.input), reflect.ValueOf(program.output), reflect.ValueOf(outcome)})
	if len(results) != 2 {
		return rhandler.RecoveryNone, fmt.Errorf("invalid Recover hook result")
	}
	decision, ok := results[0].Interface().(rhandler.Recovery)
	if !ok {
		return rhandler.RecoveryNone, fmt.Errorf("invalid Recover hook decision")
	}
	if !results[1].IsNil() {
		return decision, results[1].Interface().(error)
	}
	return decision, nil
}
