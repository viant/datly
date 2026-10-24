package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	h "github.com/viant/xdatly/handler"
)

type customPhaseObserver struct {
	events []h.PhaseEvent
	panic  bool
}

func (o *customPhaseObserver) ObservePhase(_ context.Context, event h.PhaseEvent) {
	if o.panic {
		panic("private observer details")
	}
	o.events = append(o.events, event)
}

type customPhaseContract struct {
	observers                []*customPhaseObserver
	calls, factories         int
	factoryPanic, eventPanic bool
	failure                  error
}

func (c *customPhaseContract) NewPhaseObserver() h.PhaseObserver {
	c.factories++
	if c.factoryPanic {
		panic("private factory details")
	}
	observer := &customPhaseObserver{panic: c.eventPanic}
	c.observers = append(c.observers, observer)
	return observer
}

func (c *customPhaseContract) Exec(_ context.Context, _ h.Session, in *earlyOutputInput, out *earlyOutputInput) error {
	c.calls++
	out.Name = in.Name
	return c.failure
}

type customOutcomePhaseContract struct {
	*customPhaseContract
	outcomes int
}

func (c *customOutcomePhaseContract) FinalizeOutcome(context.Context, rhandler.Invocation, any, h.Outcome) error {
	c.outcomes++
	return nil
}

func TestEngineCustomContractForwardsInvocationLocalPhaseObserver(t *testing.T) {
	for _, outcomeAware := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "outcome-aware"}[outcomeAware], func(t *testing.T) {
			contract := &customPhaseContract{}
			outcome := &customOutcomePhaseContract{customPhaseContract: contract}
			var typed h.Contract[earlyOutputInput, earlyOutputInput] = contract
			if outcomeAware {
				typed = outcome
			}
			adapter := custom.New(typed)
			var previousID uint64
			for invocation := 0; invocation < 2; invocation++ {
				result, err := New().Execute(context.Background(), Request{
					Input: testRouteInput(t, reflect.TypeFor[earlyOutputInput]()), Handler: adapter,
					BoundInput: &earlyOutputInput{Name: "bound"},
				})
				if err != nil || result.(*earlyOutputInput).Name != "bound" {
					t.Fatalf("custom result changed: %v %v", result, err)
				}
				if len(contract.observers) != invocation+1 {
					t.Fatal("typed adapter lost the observer factory")
				}
				events := contract.observers[invocation].events
				if len(events) < 4 || events[0].Phase != h.PhaseInvocation || events[0].Boundary != h.PhaseBegin {
					t.Fatalf("missing invocation boundaries: %+v", events)
				}
				var bindingEnd, executionBegin = -1, -1
				for index, event := range events {
					if event.Phase == h.PhaseBinding && event.Boundary == h.PhaseEnd {
						bindingEnd = index
					}
					if event.Phase == h.PhaseExecution && event.Boundary == h.PhaseBegin {
						executionBegin = index
					}
				}
				last := events[len(events)-1]
				if bindingEnd < 0 || executionBegin <= bindingEnd || last.Phase != h.PhaseInvocation || last.Boundary != h.PhaseEnd || last.Result != h.PhaseSucceeded || last.InvocationID == 0 || last.InvocationID == previousID {
					t.Fatalf("input binding or observer ownership changed: %+v", events)
				}
				previousID = last.InvocationID
			}
			if contract.calls != 2 || contract.factories != 2 || outcomeAware && outcome.outcomes != 2 {
				t.Fatalf("custom capabilities changed: %+v %+v", contract, outcome)
			}
		})
	}
}

func TestEngineCustomContractObserverFailuresDoNotChangeExecution(t *testing.T) {
	for _, failure := range []string{"factory panic", "observer panic", "handler error", "binding error"} {
		t.Run(failure, func(t *testing.T) {
			contract := &customPhaseContract{factoryPanic: failure == "factory panic", eventPanic: failure == "observer panic"}
			if failure == "handler error" {
				contract.failure = errors.New("operation failed")
			}
			request := Request{Input: testRouteInput(t, reflect.TypeFor[earlyOutputInput]()), Handler: custom.New[earlyOutputInput, earlyOutputInput](contract), BoundInput: &earlyOutputInput{Name: "bound"}}
			if failure == "binding error" {
				request.BoundInput = nil
			}
			result, err := New().Execute(context.Background(), request)
			switch failure {
			case "factory panic", "observer panic":
				if err != nil || result.(*earlyOutputInput).Name != "bound" || contract.calls != 1 {
					t.Fatalf("diagnostics changed result: %v %v", result, err)
				}
			case "handler error":
				if !errors.Is(err, contract.failure) || contract.calls != 1 {
					t.Fatalf("handler error changed: %v", err)
				}
			case "binding error":
				if err == nil || contract.calls != 0 {
					t.Fatalf("binding error changed: %v", err)
				}
			}
		})
	}
}

func TestEngineCustomContractWithoutObserverRetainsOrdinaryExecution(t *testing.T) {
	contract := h.ContractFunc[earlyOutputInput, earlyOutputInput](func(_ context.Context, _ h.Session, in, out *earlyOutputInput) error {
		out.Name = in.Name
		return nil
	})
	adapter := custom.New(contract)
	if adapter.(rhandler.PhaseObserverFactory).NewPhaseObserver() != nil {
		t.Fatal("non-observing contract acquired an observer")
	}
	result, err := New().Execute(context.Background(), Request{Input: testRouteInput(t, reflect.TypeFor[earlyOutputInput]()), Handler: adapter, BoundInput: &earlyOutputInput{Name: "unchanged"}})
	if err != nil || result.(*earlyOutputInput).Name != "unchanged" {
		t.Fatalf("ordinary custom handler changed: %v %v", result, err)
	}
}
