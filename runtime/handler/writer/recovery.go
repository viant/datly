package writer

import (
	"context"
	"fmt"
	rhandler "github.com/viant/datly/runtime/handler"
	"reflect"
)

func (h *Handler) SupportsMutationRecovery() bool {
	return h != nil && (h.transactionRetrySupported || h.recoverySupported || h.scopedSequences)
}

func (h *Handler) SupportsTransactionRetry() bool { return h != nil && h.transactionRetrySupported }

func (h *Handler) RetryTransaction(ctx context.Context, invocation rhandler.Invocation, _ any, outcome rhandler.MutationOutcome) (bool, error) {
	program, _ := invocation.Snapshot.(*Program)
	if !h.SupportsTransactionRetry() || program == nil || !program.hook.IsValid() || (program.afterQueueInputWasStarted() || program.afterValidateInputWasViolated()) || program.reconciliation != nil {
		return false, nil
	}
	method := program.hook.Method(h.transactionRetryMethod)
	results := method.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(program.input), reflect.ValueOf(program.output), reflect.ValueOf(outcome)})
	if !results[1].IsNil() {
		return false, results[1].Interface().(error)
	}
	return results[0].Bool(), nil
}

func (h *Handler) RecoverMutation(ctx context.Context, invocation rhandler.Invocation, _ any, outcome rhandler.MutationOutcome) (rhandler.Recovery, error) {
	program, _ := invocation.Snapshot.(*Program)
	if h == nil || !h.recoverySupported || program == nil || !program.hook.IsValid() || (program.afterQueueInputWasStarted() || program.afterValidateInputWasViolated()) || program.reconciliation != nil {
		return rhandler.RecoveryNone, nil
	}
	method := program.hook.Method(h.recoveryMethod)
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
