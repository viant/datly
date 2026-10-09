package engine

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

type customOutcomeInput struct{}
type customOutcomeOutput struct{ automaticCalls int }

func (o *customOutcomeOutput) Finalize(context.Context, error) error { o.automaticCalls++; return nil }

type customOutcomeContract struct {
	execute  func(context.Context, xhandler.Session) error
	finalize func(context.Context, rhandler.Invocation, any, xhandler.Outcome) error
}

func (h *customOutcomeContract) Exec(ctx context.Context, sess xhandler.Session, _ *customOutcomeInput, _ *customOutcomeOutput) error {
	return h.execute(ctx, sess)
}
func (h *customOutcomeContract) FinalizeOutcome(ctx context.Context, invocation rhandler.Invocation, output any, outcome xhandler.Outcome) error {
	return h.finalize(ctx, invocation, output, outcome)
}

type customRootVetoOutput struct {
	veto  error
	calls int
}

func (o *customRootVetoOutput) Finalize(_ context.Context, cause error) error {
	o.calls++
	if cause != nil {
		return cause
	}
	return o.veto
}

func TestCustomOutcomeContractWaitsForBufferedRootSQLite(t *testing.T) {
	for _, mode := range []string{"commit", "parent late SQL failure", "root finalizer veto", "callback failure"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE audit(id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			source := sqldml.Source{DB: h.DB}
			calls := 0
			callbackFailure, veto := errors.New("callback failed"), errors.New("root veto")
			var observed xhandler.Outcome
			var childResult *customOutcomeOutput
			child := custom.New[customOutcomeInput, customOutcomeOutput](&customOutcomeContract{
				execute: func(ctx context.Context, sess xhandler.Session) error {
					value, found, err := sess.Binder().Lookup(ctx, xhandler.DMLKey)
					if err != nil || !found {
						t.Fatalf("child DML lookup found=%v err=%v", found, err)
					}
					return value.(xhandler.DML).Execute("INSERT INTO audit VALUES(1)")
				},
				finalize: func(_ context.Context, invocation rhandler.Invocation, result any, outcome xhandler.Outcome) error {
					calls++
					observed = outcome
					if _, ok := invocation.Input.(*customOutcomeInput); !ok || result != childResult {
						t.Fatal("child invocation/result lost")
					}
					var count int
					if err := h.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&count); err != nil {
						t.Fatal(err)
					}
					want := 1
					if mode == "parent late SQL failure" || mode == "root finalizer veto" {
						want = 0
					}
					if count != want {
						t.Fatalf("callback preceded physical completion: count=%d want=%d", count, want)
					}
					if mode == "callback failure" {
						return callbackFailure
					}
					return nil
				},
			})
			rootOutput := &customRootVetoOutput{}
			if mode == "root finalizer veto" {
				rootOutput.veto = veto
			}
			_, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[customOutcomeInput]()), DataSource: source, BufferedComponentCalls: true,
				Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
					result, err := New().Execute(PrepareComponent(ctx, ComponentImperative, ""), Request{Input: testRouteInput(t, reflect.TypeFor[customOutcomeInput]()), DataSource: source, Handler: child, BufferedComponentCalls: true})
					if err != nil {
						return rootOutput, err
					}
					childResult = result.(*customOutcomeOutput)
					if calls != 0 || childResult.automaticCalls != 0 {
						t.Fatal("child finalized on return")
					}
					var count int
					if err := h.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count != 0 {
						t.Fatal("child flushed/committed buffered write")
					}
					if mode == "parent late SQL failure" {
						value, _, lookupErr := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
						if lookupErr != nil {
							return rootOutput, lookupErr
						}
						return rootOutput, value.(xhandler.DML).Execute("INSERT INTO missing_parent_table VALUES(2)")
					}
					return rootOutput, nil
				}),
			})
			if calls != 1 || childResult == nil || childResult.automaticCalls != 0 || rootOutput.calls != 1 {
				t.Fatalf("completion dispatch calls=%d output=%+v root=%d", calls, childResult, rootOutput.calls)
			}
			switch mode {
			case "commit":
				if err != nil || observed.Error != nil || !observed.CommitConfirmed() {
					t.Fatalf("commit err=%v outcome=%+v", err, observed)
				}
			case "parent late SQL failure":
				if err == nil || !strings.Contains(err.Error(), "missing_parent_table") || observed.Error == nil || observed.CommitConfirmed() {
					t.Fatalf("late failure err=%v outcome=%+v", err, observed)
				}
			case "root finalizer veto":
				if !errors.Is(err, veto) || !errors.Is(observed.Error, veto) || observed.CommitConfirmed() {
					t.Fatalf("veto err=%v outcome=%+v", err, observed)
				}
			case "callback failure":
				var finalized *FinalizationError
				if !errors.Is(err, callbackFailure) || !errors.As(err, &finalized) || !finalized.Outcome.CommitConfirmed() || !observed.CommitConfirmed() || observed.Error != nil {
					t.Fatalf("callback failure rewrote commit: err=%v outcome=%+v", err, observed)
				}
			}
		})
	}
}

type customEarlyOutcomeInput struct {
	Failure error `bind:"kind=transient"`
}

func (i *customEarlyOutcomeInput) Init(context.Context) error { return i.Failure }

type customEarlyOutcomeContract struct {
	calls, executed int
	observed        xhandler.Outcome
	result          any
}

func (h *customEarlyOutcomeContract) Exec(context.Context, xhandler.Session, *customEarlyOutcomeInput, *customOutcomeOutput) error {
	h.executed++
	return nil
}
func (h *customEarlyOutcomeContract) FinalizeOutcome(_ context.Context, _ rhandler.Invocation, result any, outcome xhandler.Outcome) error {
	h.calls++
	h.result, h.observed = result, outcome
	return nil
}

func TestCustomOutcomeContractEarlyFailurePreservesCause(t *testing.T) {
	for _, mode := range []string{"binding", "initialization"} {
		t.Run(mode, func(t *testing.T) {
			contract := &customEarlyOutcomeContract{}
			cause := errors.New("early contract failure")
			input := &customEarlyOutcomeInput{}
			var bindings []bindly.BindingSpec
			if mode == "binding" {
				required := true
				bindings = append(bindings, bindly.BindingSpec{Path: "Failure", Location: bindstate.Location{Kind: "missing_custom_outcome_provider"}, Required: &required})
			} else {
				input.Failure = cause
			}
			result, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[customEarlyOutcomeInput](), bindings...), BoundInput: input, Handler: custom.New[customEarlyOutcomeInput, customOutcomeOutput](contract)})
			if err == nil || result != nil || contract.result != nil || contract.calls != 1 || contract.executed != 0 {
				t.Fatalf("early callback err=%v result=%v contract=%+v", err, result, contract)
			}
			if mode == "initialization" && (!errors.Is(err, cause) || !errors.Is(contract.observed.Error, cause)) {
				t.Fatalf("initialization cause lost: %v %+v", err, contract.observed)
			}
			if mode == "binding" {
				var bindingErr *bindly.BindingError
				if !errors.As(err, &bindingErr) || !errors.Is(contract.observed.Error, bindingErr) {
					t.Fatalf("binding cause lost: %v %+v", err, contract.observed)
				}
			}
		})
	}
}
