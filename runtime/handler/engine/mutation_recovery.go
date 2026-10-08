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
	"time"
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
	if data.mutationGuard().reconciliationVetoedReplay() {
		return rhandler.RecoveryNone, false, nil
	}
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
	if classifier, ok := data.data.(dexec.TransactionContentionClassifier); ok {
		report.Contention = classifier.TransactionContention(completionErr)
	}

	if native, ok := request.Handler.(rhandler.ScopedMutationRecoverer); ok {
		outcome := data.completionOutcome()
		if completionErr != nil && len(outcome.Transactions) == 1 && outcome.Transactions[0].State == xhandler.TransactionRolledBack {
			allowed, e := native.RecoverScopedMutation(ctx, invocation, report, outcome)
			if e != nil {
				return rhandler.RecoveryNone, false, e
			}
			if allowed {
				if request.mutationAttempt >= native.ScopedMutationRetryLimit() {
					return rhandler.RecoveryNone, false, fmt.Errorf("scoped sequence retry limit reached")
				}
				if report.Contention {
					if err := waitContentionRetry(ctx, request.mutationAttempt); err != nil {
						return rhandler.RecoveryNone, false, err
					}
				}
				return rhandler.RecoveryRetry, true, nil
			}
		}
	}
	if retryer, ok := request.Handler.(rhandler.TransactionRetryer); ok && retryer.SupportsTransactionRetry() {
		outcome := data.completionOutcome()
		if ctx.Err() == nil && !report.Nested && len(outcome.Transactions) == 1 && outcome.Transactions[0].State == xhandler.TransactionRolledBack {
			if mutation, eligible := rollbackRetryEvidence(report, data.finalizers[0].err, completionErr); eligible {
				allowed, err := retryer.RetryTransaction(ctx, invocation, data.finalizers[0].result, rhandler.MutationOutcome{Outcome: outcome, Mutation: mutation, Attempt: request.mutationAttempt, RetryLimit: 1})
				if err != nil {
					return rhandler.RecoveryNone, false, err
				}
				if allowed {
					if request.mutationAttempt >= 1 {
						return rhandler.RecoveryNone, false, fmt.Errorf("transaction retry limit reached")
					}
					if mutation.Contention {
						if err := waitContentionRetry(ctx, request.mutationAttempt); err != nil {
							return rhandler.RecoveryNone, false, err
						}
					}
					return rhandler.RecoveryRetry, true, nil
				}
			}
		}
	}
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

// Back off only after a confirmed rollback and an opted-in coded-contention
// retry. No transaction is held while waiting, and cancellation stops replay.
func waitContentionRetry(ctx context.Context, attempt int) error {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 4 {
		attempt = 4
	}
	timer := time.NewTimer(time.Duration(1<<attempt) * 10 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func rollbackRetryEvidence(report dexec.MutationReport, operationErr, completionErr error) (dexec.MutationResult, bool) {
	if completionErr == nil || report.Queued < 0 || len(report.Results) > report.Queued {
		return dexec.MutationResult{}, false
	}
	var failure dexec.MutationResult
	found := false
	for _, result := range report.Results {
		if found || result.Records < 1 || (result.Operation != "insert" && result.Operation != "update" && result.Operation != "delete") {
			return failure, false
		}
		if result.Error != nil {
			failure = result
			found = true
		}
	}
	if found {
		if completionIntroducedError(completionErr, failure.Error) {
			return failure, false
		}
		var conflict *xhandler.Conflict
		return failure, failure.Contention || errx.IsDuplicateKey(failure.Error) || errors.As(failure.Error, &conflict)
	}
	// Binding/allocation contention may precede queued DML. The database owner
	// classifies its error code; additional cleanup failures remain ineligible.
	if report.Contention && operationErr != nil && !completionIntroducedError(completionErr, operationErr) {
		return dexec.MutationResult{Error: operationErr, Contention: true}, true
	}
	return failure, false
}
