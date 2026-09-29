package engine

import (
	"context"
	"errors"
	"fmt"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx/io/errx"
	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
)

var errMutationRetry = errors.New("mutation attempt completed; native recovery requested a replay")

// Trusted bound callers have typed presence that JSON cannot preserve. Use
// native shape cloning for request fields/markers only, excluding Current,
// indexes, injected services and derived predicate controls.
func captureMutationInput(contract *registry.RouteInputContract, input any) (any, error) {
	options := xshape.CloneOptions{}
	var paths []string
	for _, field := range contract.Fields() {
		if (spec.BindSource{Kind: field.Binding().Location.Kind}).RequestValue() {
			paths = append(paths, field.Path())
		}
	}
	for i := 0; i < contract.Type().NumField(); i++ {
		field := contract.Type().Field(i)
		if field.Tag.Get("setMarker") == "true" {
			paths = append(paths, field.Name)
		}
	}
	if err := options.Select(contract.Type(), paths...); err != nil {
		return nil, err
	}
	return (xshape.Runtime{}).CloneValue(input, options)
}

// Recovery admission is deliberately narrower than ordinary transaction
// finalization: one finished root, one known unit, one executed row operation.
// Nothing can replay siblings, batches, caller work or unknown commit outcomes.
func recoverMutation(ctx context.Context, request Request, data *dataScope, invocation rhandler.Invocation, completionErr error) (decision rhandler.Recovery, recovered bool, failure error) {
	defer func() {
		if value := recover(); value != nil {
			decision, recovered, failure = rhandler.RecoveryNone, false, dexec.NewPanicError("mutation recovery hook", value)
		}
	}()
	recoverer, ok := request.Handler.(rhandler.MutationRecoverer)
	if !ok || !recoverer.SupportsMutationRecovery() || len(data.units) != 0 || len(data.finalizers) != 1 || !data.finalizers[0].finished {
		return rhandler.RecoveryNone, false, nil
	}
	if _, ok := request.Handler.(rhandler.OutcomeFinalizer); !ok || data.finalizers[0].invocation.Input != invocation.Input {
		return rhandler.RecoveryNone, false, nil
	}
	reporter, ok := data.data.(dexec.MutationReporter)
	if !ok {
		return rhandler.RecoveryNone, false, nil
	}
	report := reporter.MutationReport()
	if report.Nested || report.Queued != 1 || len(report.Results) != 1 || report.Results[0].Records != 1 {
		return rhandler.RecoveryNone, false, nil
	}
	mutation := report.Results[0]
	if mutation.Operation != "insert" && mutation.Operation != "update" && mutation.Operation != "delete" {
		return rhandler.RecoveryNone, false, nil
	}
	outcome := data.completionOutcome()
	if len(outcome.Transactions) != 1 {
		return rhandler.RecoveryNone, false, nil
	}
	switch outcome.Transactions[0].State {
	case xhandler.TransactionCommitted:
		if completionErr != nil || mutation.Error != nil || mutation.Affected != 0 {
			return rhandler.RecoveryNone, false, nil
		}
	case xhandler.TransactionRolledBack:
		if mutation.Error == nil || completionErr == nil || completionIntroducedError(completionErr, mutation.Error) {
			return rhandler.RecoveryNone, false, nil
		}
		var conflict *xhandler.Conflict
		if !errors.As(mutation.Error, &conflict) && !errx.IsDuplicateKey(mutation.Error) && !mutation.Contention {
			return rhandler.RecoveryNone, false, nil
		}
	default:
		return rhandler.RecoveryNone, false, nil
	}
	decision, err := recoverer.RecoverMutation(ctx, invocation, data.finalizers[0].result, rhandler.MutationOutcome{Outcome: outcome, Mutation: mutation, Attempt: request.mutationAttempt, RetryLimit: 1})
	if err != nil || decision == rhandler.RecoveryNone {
		return decision, false, err
	}
	if decision != rhandler.RecoveryAccept && decision != rhandler.RecoveryRetry {
		return decision, false, fmt.Errorf("invalid mutation recovery decision %d", decision)
	}
	if decision == rhandler.RecoveryRetry && request.mutationAttempt >= 1 {
		return decision, false, fmt.Errorf("mutation recovery retry limit reached")
	}
	return decision, true, nil
}
