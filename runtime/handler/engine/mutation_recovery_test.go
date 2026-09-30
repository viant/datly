package engine

import (
	"context"
	"errors"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bindstate "github.com/viant/bindly/state"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/sqlx/io/errx"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

type recoveryProbe struct {
	outcomeAwareHandler
	recover func(context.Context, rhandler.Invocation, any, rhandler.MutationOutcome) (rhandler.Recovery, error)
}

func (*recoveryProbe) SupportsMutationRecovery() bool { return true }
func (h *recoveryProbe) RecoverMutation(c context.Context, i rhandler.Invocation, o any, m rhandler.MutationOutcome) (rhandler.Recovery, error) {
	return h.recover(c, i, o, m)
}

type recoveryData struct {
	xhandler.Data
	report dexec.MutationReport
}

func (d recoveryData) MutationReport() dexec.MutationReport { return d.report }

func TestMutationRecoveryAdmission(t *testing.T) {
	conflict := &xhandler.Conflict{Entity: "items", Reason: "stale"}
	duplicate := errx.DuplicateKey("insert", "items", errors.New("UNIQUE constraint failed: items.id"))
	type useCase struct {
		desc                             string
		input                            xhandler.TransactionState
		expect                           bool
		mutationErr, errorExtra          error
		queued, records, frames, attempt int
		decision                         rhandler.Recovery
		decisionError                    bool
		nested                           bool
	}
	for _, tc := range []useCase{
		{desc: "committed zero", input: xhandler.TransactionCommitted, expect: true},
		{desc: "rolled back CAS", input: xhandler.TransactionRolledBack, mutationErr: conflict, expect: true},
		{desc: "rolled back duplicate", input: xhandler.TransactionRolledBack, mutationErr: duplicate, expect: true},
		{desc: "unrelated DB failure", input: xhandler.TransactionRolledBack, mutationErr: errors.New("connection failure")},
		{desc: "rollback diagnostic", input: xhandler.TransactionRolledBack, mutationErr: conflict, errorExtra: errors.New("cleanup failed")},
		{desc: "caller pending", input: xhandler.TransactionCallerPending},
		{desc: "commit unknown", input: xhandler.TransactionCommitUnknown},
		{desc: "rollback unknown", input: xhandler.TransactionRollbackUnknown, mutationErr: conflict},
		{desc: "partial", input: xhandler.TransactionPartial},
		{desc: "queued sibling", input: xhandler.TransactionCommitted, queued: 2},
		{desc: "batched insert", input: xhandler.TransactionCommitted, records: 2},
		{desc: "child finalizer", input: xhandler.TransactionCommitted, frames: 2},
		{desc: "child mutation", input: xhandler.TransactionCommitted, nested: true},
		{desc: "bounded replay exhausted", input: xhandler.TransactionCommitted, expect: true, attempt: 1, decision: rhandler.RecoveryRetry, decisionError: true},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			queued, records, frames := tc.queued, tc.records, tc.frames
			if queued == 0 {
				queued = 1
			}
			if records == 0 {
				records = 1
			}
			if frames == 0 {
				frames = 1
			}
			calls := 0
			probe := &recoveryProbe{recover: func(_ context.Context, _ rhandler.Invocation, _ any, o rhandler.MutationOutcome) (rhandler.Recovery, error) {
				calls++
				if o.Attempt != tc.attempt || o.RetryLimit != 1 {
					t.Fatal(o)
				}
				return tc.decision, nil
			}}
			completionErr := errors.Join(tc.mutationErr, tc.errorExtra)
			scope := &dataScope{data: recoveryData{report: dexec.MutationReport{Queued: queued, Nested: tc.nested, Results: []dexec.MutationResult{{Operation: "insert", Table: "items", Records: records, Error: tc.mutationErr}}}}, completion: xhandler.Outcome{Error: completionErr, Transactions: []xhandler.TransactionOutcome{{State: tc.input}}}}
			for i := 0; i < frames; i++ {
				scope.finalizers = append(scope.finalizers, &outcomeFrame{finished: true})
			}
			_, _, err := recoverMutation(context.Background(), Request{Handler: probe, mutationAttempt: tc.attempt}, scope, rhandler.Invocation{}, completionErr)
			if (calls == 1) != tc.expect || (err != nil) != tc.decisionError {
				t.Fatalf("calls=%d error=%v", calls, err)
			}
		})
	}
}

type recoveryReplayInput struct {
	ID      int
	Current int
	Has     *struct{ ID, Current bool } `setMarker:"true"`
}

func (i *recoveryReplayInput) Init(context.Context) error { i.ID += 10; return nil }

func TestMutationRecoveryRebindsOriginalFactsAndFreshDependencies(t *testing.T) {
	for _, bound := range []bool{false, true} {
		t.Run(map[bool]string{false: "transport", true: "trusted bound"}[bound], func(t *testing.T) {
			h := testharness.NewSQLiteHarness(t)
			ctx := context.Background()
			if err := h.ExecStatements(ctx, "CREATE TABLE items(id INTEGER PRIMARY KEY)", "CREATE TRIGGER ignored BEFORE INSERT ON items BEGIN SELECT RAISE(IGNORE); END"); err != nil {
				t.Fatal(err)
			}
			contract := testRouteInput(t, reflect.TypeFor[recoveryReplayInput](), bindly.BindingSpec{Path: "ID", Name: "ID", Location: bindstate.Location{Kind: "query", In: "id"}}, bindly.BindingSpec{Path: "Current", Name: "Current", Location: bindstate.Location{Kind: "view", In: "current"}})
			reads, executes, recoveries, finalizes, observations := 0, 0, 0, 0, 0
			probe := &recoveryProbe{}
			probe.execute = func(ctx context.Context, i rhandler.Invocation) (any, error) {
				executes++
				in := i.Input.(*recoveryReplayInput)
				if in.ID != 11 || in.Current != executes || in.Has == nil || !in.Has.ID {
					t.Fatalf("attempt=%d input=%+v", executes, in)
				}
				if executes == 2 {
					return in, nil
				}
				value, _, err := i.Binder.Lookup(ctx, xhandler.DMLKey)
				if err != nil {
					return nil, err
				}
				return in, value.(xhandler.DML).Insert("items", &struct {
					ID int `sqlx:"id,primaryKey"`
				}{ID: in.ID})
			}
			probe.recover = func(_ context.Context, _ rhandler.Invocation, _ any, o rhandler.MutationOutcome) (rhandler.Recovery, error) {
				recoveries++
				if o.Mutation.Affected != 0 || o.Attempt != 0 || !o.CommitConfirmed() {
					t.Fatal(o)
				}
				return rhandler.RecoveryRetry, nil
			}
			probe.finalize = func(_ context.Context, _ rhandler.Invocation, _ any, o xhandler.Outcome) error {
				finalizes++
				if finalizes == 1 && !errors.Is(o.Error, errMutationRetry) {
					t.Fatalf("retry publication not guarded: %+v", o)
				}
				return nil
			}
			request := Request{Input: contract, Handler: probe, DataSource: dml.Source{DB: h.DB}, Providers: []locator.Provider{provider.Named("view", func(context.Context, reflect.Type, string) (any, bool, error) { reads++; return reads, true, nil })}, Completion: func(xhandler.Outcome) { observations++ }}
			if bound {
				request.BoundInput = &recoveryReplayInput{ID: 1, Current: 999, Has: &struct{ ID, Current bool }{ID: true, Current: true}}
			} else {
				request.Scope = engineProviderScope{provider.Named("query", func(context.Context, reflect.Type, string) (any, bool, error) { return "1", true, nil })}
			}
			result, err := New().Execute(ctx, request)
			if err != nil || result == nil || executes != 2 || recoveries != 1 || finalizes != 2 || reads != 2 || observations != 1 {
				t.Fatalf("result=%v err=%v executes=%d recovery=%d finalizes=%d reads=%d observer=%d", result, err, executes, recoveries, finalizes, reads, observations)
			}
		})
	}
}
