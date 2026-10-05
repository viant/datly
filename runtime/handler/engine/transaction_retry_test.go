package engine

import (
	"context"
	"errors"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
	"testing"
)

type transactionRetryProbe struct {
	outcomeAwareHandler
	calls int
	allow bool
}

func (*transactionRetryProbe) SupportsMutationRecovery() bool { return true }
func (*transactionRetryProbe) RecoverMutation(context.Context, rhandler.Invocation, any, rhandler.MutationOutcome) (rhandler.Recovery, error) {
	return rhandler.RecoveryNone, nil
}
func (*transactionRetryProbe) SupportsTransactionRetry() bool { return true }
func (p *transactionRetryProbe) RetryTransaction(_ context.Context, _ rhandler.Invocation, _ any, outcome rhandler.MutationOutcome) (bool, error) {
	p.calls++
	return p.allow, nil
}

func TestTransactionRetryAdmissionRequiresConfirmedRootRollback(t *testing.T) {
	busy := errors.New("driver-coded contention")
	for _, tc := range []struct {
		name                             string
		state                            xhandler.TransactionState
		queued, attempt, frames          int
		nested, extra, ordinary, decline bool
		calls                            int
		retry, failed                    bool
	}{
		{name: "rolled back graph", state: xhandler.TransactionRolledBack, queued: 3, calls: 1, retry: true},
		{name: "application declines", state: xhandler.TransactionRolledBack, queued: 3, decline: true, calls: 1},
		{name: "second retry bounded", state: xhandler.TransactionRolledBack, queued: 3, attempt: 1, calls: 1, failed: true},
		{name: "commit cannot replay", state: xhandler.TransactionCommitted, queued: 3},
		{name: "caller pending cannot replay", state: xhandler.TransactionCallerPending, queued: 3},
		{name: "unknown cannot replay", state: xhandler.TransactionRollbackUnknown, queued: 3},
		{name: "nested cannot replay", state: xhandler.TransactionRolledBack, queued: 3, nested: true},
		{name: "child finalizer cannot replay", state: xhandler.TransactionRolledBack, queued: 3, frames: 2},
		{name: "additional error cannot replay", state: xhandler.TransactionRolledBack, queued: 3, extra: true},
		{name: "ordinary error cannot replay", state: xhandler.TransactionRolledBack, queued: 3, ordinary: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &transactionRetryProbe{allow: !tc.decline}
			completionErr := busy
			if tc.extra {
				completionErr = errors.Join(busy, errors.New("cleanup failed"))
			}
			data := &dataScope{data: recoveryData{report: dexec.MutationReport{Queued: tc.queued, Nested: tc.nested, Results: []dexec.MutationResult{{Operation: "insert", Table: "records", Records: 1, Error: busy, Contention: !tc.ordinary}}}}, completion: xhandler.Outcome{Error: completionErr, Transactions: []xhandler.TransactionOutcome{{State: tc.state}}}}
			frames := tc.frames
			if frames == 0 {
				frames = 1
			}
			for i := 0; i < frames; i++ {
				data.finalizers = append(data.finalizers, &outcomeFrame{finished: true})
			}
			decision, recovered, err := recoverMutation(context.Background(), Request{Handler: probe, mutationAttempt: tc.attempt}, data, rhandler.Invocation{}, completionErr)
			if probe.calls != tc.calls || recovered != tc.retry || (err != nil) != tc.failed || recovered && decision != rhandler.RecoveryRetry {
				t.Fatalf("calls=%d recovered=%v decision=%v err=%v", probe.calls, recovered, decision, err)
			}
		})
	}
}

func TestRollbackRetryEvidenceRejectsUntruthfulReports(t *testing.T) {
	cause := errors.New("coded contention")
	failed := dexec.MutationResult{Operation: "insert", Table: "records", Records: 1, Error: cause, Contention: true}
	for _, report := range []dexec.MutationReport{
		{Queued: 0, Results: []dexec.MutationResult{failed}},
		{Queued: 2, Results: []dexec.MutationResult{failed, {Operation: "insert", Records: 1}}},
		{Queued: 1, Results: []dexec.MutationResult{{Operation: "execute", Records: 1, Error: cause, Contention: true}}},
	} {
		if _, ok := rollbackRetryEvidence(report, nil, cause); ok {
			t.Fatalf("accepted %+v", report)
		}
	}
	if _, ok := rollbackRetryEvidence(dexec.MutationReport{Contention: true}, cause, cause); !ok {
		t.Fatal("lost pre-DML contention evidence")
	}
}

func TestContentionRetryWaitHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(waitContentionRetry(ctx, 4), context.Canceled) {
		t.Fatal("cancelled retry waited or replayed")
	}
}
