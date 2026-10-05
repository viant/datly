package engine

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/response"
)

type panicCompletionData struct {
	xhandler.Data
	preparePanic, finalizationPanic, completePanic, postPanic any
	begins, prepares, finalizationPrepares, completes         int
	commits, rollbacks                                        int
	outcome                                                   xhandler.TransactionOutcome
}

func (d *panicCompletionData) BeginInvocation() error { d.begins++; return nil }
func (d *panicCompletionData) PrepareCompletion(context.Context) error {
	d.prepares++
	if d.preparePanic != nil {
		panic(d.preparePanic)
	}
	return nil
}
func (d *panicCompletionData) PrepareFinalization(context.Context) error {
	d.finalizationPrepares++
	if d.finalizationPanic != nil {
		panic(d.finalizationPanic)
	}
	return nil
}
func (d *panicCompletionData) Complete(_ context.Context, cause error) error {
	d.completes++
	if cause != nil {
		d.rollbacks++
		d.outcome = xhandler.TransactionOutcome{State: xhandler.TransactionRollbackUnknown, Error: cause}
	} else {
		d.commits++
		d.outcome = xhandler.TransactionOutcome{State: xhandler.TransactionCommitUnknown}
	}
	if d.completePanic != nil {
		panic(d.completePanic)
	}
	if cause != nil {
		d.outcome.State = xhandler.TransactionRolledBack
	} else {
		d.outcome.State = xhandler.TransactionCommitted
	}
	if d.postPanic != nil {
		panic(d.postPanic)
	}
	return cause
}
func (d *panicCompletionData) TransactionOutcome() xhandler.TransactionOutcome { return d.outcome }

func assertEngineCompletionPanic(t *testing.T, err error, cause any) {
	t.Helper()
	var recovered *dexec.PanicError
	if !errors.As(err, &recovered) || recovered.Cause() != cause || len(recovered.Stack()) == 0 {
		t.Fatalf("panic cause/stack lost: %v", err)
	}
	if _, public := response.ErrorBody(err); public || response.ErrorStatusCode(err, 500) != 500 {
		t.Fatalf("panic acquired public response: %v", err)
	}
	encoded, marshalErr := json.Marshal(err)
	if marshalErr != nil || strings.Contains(err.Error(), "PRIVATE") || strings.Contains(string(encoded), "PRIVATE") {
		t.Fatalf("panic exposed: %v %s", err, encoded)
	}
}

func TestDataScopePanicContinuesRemainingUnitCleanup(t *testing.T) {
	for _, stage := range []string{"preparation", "finalization preparation", "rollback", "commit"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			private := &response.Error{Code: 400, Payload: "PRIVATE completion panic"}
			data := []*panicCompletionData{{}, {}, {}}
			root := neutralDataScope()
			root.data = data[0]
			for _, d := range data[1:] {
				root.units = append(root.units, &dataScope{root: root, data: d})
			}
			var operationErr error
			switch stage {
			case "preparation":
				data[2].preparePanic = private
			case "finalization preparation":
				data[1].finalizationPanic = private
				operationErr = root.prepare(ctx)
				assertEngineCompletionPanic(t, operationErr, private)
			case "rollback":
				operationErr = errors.New("handler failed")
				data[2].completePanic = private
			case "commit":
				data[1].completePanic = private
			}
			err := root.complete(ctx, operationErr)
			assertEngineCompletionPanic(t, err, private)
			if operationErr != nil && !errors.Is(err, operationErr) {
				t.Fatalf("lost initial failure: %v", err)
			}
			report := root.completionOutcome()
			assertEngineCompletionPanic(t, report.Error, private)
			if len(report.Transactions) != 3 {
				t.Fatalf("remaining unit snapshot lost: %+v", report)
			}
			for i, d := range data {
				commits, rollbacks, prepares := 0, 1, 0
				state := xhandler.TransactionRolledBack
				if stage == "preparation" || stage == "commit" {
					prepares = 1
				}
				if stage == "rollback" && i == 2 {
					state = xhandler.TransactionRollbackUnknown
				}
				if stage == "commit" && i < 2 {
					commits, rollbacks, state = 1, 0, xhandler.TransactionCommitted
					if i == 1 {
						state = xhandler.TransactionCommitUnknown
					}
				}
				if d.completes != 1 || d.commits != commits || d.rollbacks != rollbacks || d.prepares != prepares || report.Transactions[i].State != state {
					t.Fatalf("unit %d completes=%d commits=%d rollbacks=%d prepares=%d outcome=%+v", i, d.completes, d.commits, d.rollbacks, d.prepares, report.Transactions[i])
				}
				if stage == "finalization preparation" {
					want := 1
					if i == 2 {
						want = 0
					}
					if d.finalizationPrepares != want {
						t.Fatalf("unit %d preparation count=%d", i, d.finalizationPrepares)
					}
				}
			}
			if stage == "rollback" {
				assertEngineCompletionPanic(t, root.units[1].completionErr, private)
			}
			if stage == "commit" {
				assertEngineCompletionPanic(t, root.units[0].completionErr, private)
			}
		})
	}
}

type panicFlushOnlyData struct {
	xhandler.Data
	cause   any
	flushes int
}

func (d *panicFlushOnlyData) Flush(context.Context, string) error { d.flushes++; panic(d.cause) }

func TestDataScopeFlushOnlyPanicRetainsUnknownEvidence(t *testing.T) {
	private := errors.New("PRIVATE custom flush")
	data := &panicFlushOnlyData{cause: private}
	root := neutralDataScope()
	root.data = data
	err := root.complete(context.Background(), nil)
	assertEngineCompletionPanic(t, err, private)
	outcome := root.completionOutcome()
	if data.flushes != 1 || outcome.State() != xhandler.TransactionUnknown || outcome.CommitConfirmed() {
		t.Fatalf("flush=%d outcome=%+v", data.flushes, outcome)
	}
	assertEngineCompletionPanic(t, outcome.Transactions[0].Error, private)
}

func TestEngineCompletionPanicFinalizesAndNotifiesOnce(t *testing.T) {
	for _, stage := range []string{"preparation", "commit", "rollback", "postcommit", "outcome callback", "completion notification"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			private := &response.Error{Code: 400, Payload: "PRIVATE callback payload"}
			data := &panicCompletionData{}
			source := &staticDataSource{data: data}
			output := &completionObservedOutput{}
			var operationErr error
			state := xhandler.TransactionCommitted
			switch stage {
			case "preparation":
				data.preparePanic, state = private, xhandler.TransactionRolledBack
			case "commit":
				data.completePanic, state = private, xhandler.TransactionCommitUnknown
			case "rollback":
				data.completePanic, state = private, xhandler.TransactionRollbackUnknown
				operationErr = errors.New("handler failed")
			case "postcommit":
				data.postPanic = private
			}
			invocations, finalizers, notifications := 0, 0, 0
			var finalOutcome, notified xhandler.Outcome
			h := &outcomeAwareHandler{
				execute: func(ctx context.Context, inv rhandler.Invocation) (any, error) {
					invocations++
					if _, _, err := inv.Binder.Lookup(ctx, xhandler.DataKey); err != nil {
						return nil, err
					}
					return output, operationErr
				},
				finalize: func(_ context.Context, _ rhandler.Invocation, result any, outcome xhandler.Outcome) error {
					finalizers++
					finalOutcome = outcome
					if result != output {
						t.Fatal("original callback result lost")
					}
					if stage == "outcome callback" {
						panic(private)
					}
					return nil
				},
			}
			_, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), BoundInput: &struct{}{}, Handler: h, DataSource: source, Completion: func(outcome xhandler.Outcome) {
				notifications++
				notified = outcome
				if stage == "completion notification" {
					panic(private)
				}
			}})
			assertEngineCompletionPanic(t, err, private)
			if operationErr != nil && !errors.Is(err, operationErr) {
				t.Fatalf("lost handler failure: %v", err)
			}
			if invocations != 1 || finalizers != 1 || notifications != 1 || output.calls != 0 || source.opens != 1 || data.begins != 1 || data.completes != 1 {
				t.Fatalf("handler=%d finalizers=%d notification=%d old hooks=%d open=%d begin=%d complete=%d", invocations, finalizers, notifications, output.calls, source.opens, data.begins, data.completes)
			}
			commits, rollbacks, prepares := 1, 0, 1
			if stage == "preparation" || stage == "rollback" {
				commits, rollbacks = 0, 1
			}
			if stage == "rollback" {
				prepares = 0
			}
			if data.commits != commits || data.rollbacks != rollbacks || data.prepares != prepares {
				t.Fatalf("commit=%d rollback=%d prepare=%d", data.commits, data.rollbacks, data.prepares)
			}
			if finalOutcome.State() != state || notified.State() != state {
				t.Fatalf("final=%+v notified=%+v want=%s", finalOutcome, notified, state)
			}
			if stage != "outcome callback" && stage != "completion notification" {
				assertEngineCompletionPanic(t, finalOutcome.Error, private)
				assertEngineCompletionPanic(t, notified.Error, private)
			}
			if stage == "outcome callback" {
				var finalization *FinalizationError
				if !errors.As(err, &finalization) || !finalization.Outcome.CommitConfirmed() {
					t.Fatalf("postcommit finalization evidence lost: %v", err)
				}
			}
		})
	}
}

func TestChildOutcomePanicContinuesParentAndPublicProjection(t *testing.T) {
	ctx := context.Background()
	private := &response.Error{Code: 400, Payload: "PRIVATE child panic"}
	data := &panicCompletionData{}
	source := &staticDataSource{data: data}
	var order []string
	invocations, notifications := 0, 0
	body := map[string]any{"status": "error", "message": "Internal Server Error", "data": []any{}, "violations": []any{}}
	public := &response.Error{Code: 500, Payload: body}
	child := &outcomeAwareHandler{
		execute: func(context.Context, rhandler.Invocation) (any, error) { invocations++; return nil, nil },
		finalize: func(_ context.Context, _ rhandler.Invocation, _ any, outcome xhandler.Outcome) error {
			order = append(order, "child")
			if !outcome.CommitConfirmed() {
				t.Fatal("child lost commit evidence")
			}
			panic(private)
		},
	}
	parent := &outcomeAwareHandler{
		execute: func(ctx context.Context, inv rhandler.Invocation) (any, error) {
			invocations++
			if _, _, err := inv.Binder.Lookup(ctx, xhandler.DataKey); err != nil {
				return nil, err
			}
			return New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), Handler: child, DataSource: source})
		},
		finalize: func(_ context.Context, _ rhandler.Invocation, _ any, outcome xhandler.Outcome) error {
			order = append(order, "parent")
			if !outcome.CommitConfirmed() {
				t.Fatal("parent lost commit evidence")
			}
			return public
		},
	}
	_, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), Handler: parent, DataSource: source, Completion: func(outcome xhandler.Outcome) {
		notifications++
		if !outcome.CommitConfirmed() {
			t.Fatal("notification lost commit")
		}
	}})
	var recovered *dexec.PanicError
	var finalization *FinalizationError
	if !errors.As(err, &recovered) || recovered.Cause() != private || !errors.As(err, &finalization) || !finalization.Outcome.CommitConfirmed() || !errors.Is(err, public) {
		t.Fatalf("callback causes/evidence lost: %v", err)
	}
	projected, explicit := response.ErrorBody(err)
	if !explicit || !reflect.DeepEqual(projected, body) || response.ErrorStatusCode(err, 0) != 500 {
		t.Fatalf("public projection=%#v error=%v", projected, err)
	}
	encoded, _ := json.Marshal(projected)
	if strings.Contains(string(encoded), "PRIVATE") || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("panic escaped redaction")
	}
	if !reflect.DeepEqual(order, []string{"child", "parent"}) || invocations != 2 || notifications != 1 || data.begins != 1 || data.prepares != 1 || data.completes != 1 || data.commits != 1 || data.rollbacks != 0 {
		t.Fatalf("order=%v invocation=%d notification=%d data=%+v", order, invocations, notifications, data)
	}
}

func TestPanickingOutcomeCallbacksAreNotReplayed(t *testing.T) {
	root := neutralDataScope()
	calls := 0
	private := errors.New("PRIVATE finalizer")
	for _, route := range []string{"parent", "child"} {
		frame, err := root.registerOutcome(context.Background(), route, &outcomeAwareHandler{finalize: func(context.Context, rhandler.Invocation, any, xhandler.Outcome) error { calls++; panic(private) }})
		if err != nil {
			t.Fatal(err)
		}
		root.finishOutcome(frame, rhandler.Invocation{}, nil, nil)
	}
	operationErr := errors.New("completion failure")
	err := root.finalizeOutcomes(operationErr)
	assertEngineCompletionPanic(t, err, private)
	if !errors.Is(err, operationErr) || calls != 2 {
		t.Fatalf("calls=%d error=%v", calls, err)
	}
	if again := root.finalizeOutcomes(err); again != err || calls != 2 {
		t.Fatal("outcome callbacks replayed")
	}
}

type panicPreparedSQLData struct {
	*sqldml.Data
	private                                           any
	begins, prepares, finalizationPrepares, completes int
}

func (d *panicPreparedSQLData) BeginInvocation() error { d.begins++; return d.Data.BeginInvocation() }
func (d *panicPreparedSQLData) PrepareCompletion(ctx context.Context) error {
	d.prepares++
	if err := d.Data.PrepareCompletion(ctx); err != nil {
		return err
	}
	panic(d.private)
}
func (d *panicPreparedSQLData) PrepareFinalization(ctx context.Context) error {
	d.finalizationPrepares++
	if err := d.Data.PrepareFinalization(ctx); err != nil {
		return err
	}
	panic(d.private)
}
func (d *panicPreparedSQLData) Complete(ctx context.Context, cause error) error {
	d.completes++
	return d.Data.Complete(ctx, cause)
}

type panicPreparationOutput struct {
	calls int
	cause error
}

func (o *panicPreparationOutput) Finalize(_ context.Context, cause error) error {
	o.calls++
	o.cause = cause
	return nil
}

func TestEnginePreparationPanicRollsBackFlushedRowSQLite(t *testing.T) {
	for _, stage := range []string{"completion", "finalization"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER)"); err != nil {
				t.Fatal(err)
			}
			private := errors.New("PRIVATE preparation after SQL execution")
			data := &panicPreparedSQLData{Data: sqldml.NewData(h.DB), private: private}
			source := &staticDataSource{data: data}
			output := &panicPreparationOutput{}
			invocations, finalizers, notifications := 0, 0, 0
			var notified xhandler.Outcome
			execute := rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
				invocations++
				value, _, err := inv.Binder.Lookup(ctx, xhandler.DataKey)
				if err != nil {
					return nil, err
				}
				return output, value.(xhandler.Data).Execute("INSERT INTO audit VALUES(1)")
			})
			var handler rhandler.Handler = execute
			if stage == "completion" {
				handler = &outcomeAwareHandler{execute: execute, finalize: func(_ context.Context, _ rhandler.Invocation, result any, outcome xhandler.Outcome) error {
					finalizers++
					if result != output || outcome.State() != xhandler.TransactionRolledBack {
						t.Fatalf("result=%v outcome=%+v", result, outcome)
					}
					assertEngineCompletionPanic(t, outcome.Error, private)
					return nil
				}}
			}
			result, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), Handler: handler, DataSource: source, Completion: func(outcome xhandler.Outcome) { notifications++; notified = outcome }})
			assertEngineCompletionPanic(t, err, private)
			assertEngineCompletionPanic(t, notified.Error, private)
			if result != nil || notified.State() != xhandler.TransactionRolledBack || invocations != 1 || notifications != 1 || source.opens != 1 || data.begins != 1 || data.completes != 1 {
				t.Fatalf("result=%v outcome=%+v invocations=%d notifications=%d opens=%d begin=%d complete=%d", result, notified, invocations, notifications, source.opens, data.begins, data.completes)
			}
			if stage == "completion" {
				if data.prepares != 1 || data.finalizationPrepares != 0 || finalizers != 1 || output.calls != 0 {
					t.Fatal("completion hooks replayed")
				}
			} else {
				if data.prepares != 0 || data.finalizationPrepares != 1 || finalizers != 0 || output.calls != 1 {
					t.Fatal("finalization hooks replayed")
				}
				assertEngineCompletionPanic(t, output.cause, private)
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{0}})
		})
	}
}
