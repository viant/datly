package custom

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	xhandler "github.com/viant/xdatly/handler"
)

type outcomeContract struct {
	Dependency string `bind:"kind=static,required"`
	calls      int
	invocation rhandler.Invocation
	result     any
	outcome    xhandler.Outcome
	failure    error
}

func (*outcomeContract) Exec(context.Context, xhandler.Session, *staticInput, *staticOutput) error {
	return nil
}
func (*outcomeContract) RequiresReadMetadata() bool { return true }
func (*outcomeContract) CaptureInput(_ context.Context, input *staticInput) (any, error) {
	return input, nil
}
func (c *outcomeContract) FinalizeOutcome(_ context.Context, invocation rhandler.Invocation, result any, outcome xhandler.Outcome) error {
	c.calls++
	c.invocation, c.result, c.outcome = invocation, result, outcome
	return c.failure
}

func TestOutcomeContractNewAndFactoryPreserveAdapterSurface(t *testing.T) {
	for _, factory := range []bool{false, true} {
		t.Run(map[bool]string{false: "New", true: "Factory"}[factory], func(t *testing.T) {
			contract := &outcomeContract{failure: errors.New("callback failure")}
			adapt := func(c xhandler.Contract[staticInput, staticOutput]) rhandler.TypedHandler {
				if !factory {
					return New(c)
				}
				h, err := Factory(func() xhandler.Contract[staticInput, staticOutput] { return c })()
				if err != nil {
					t.Fatal(err)
				}
				return h
			}
			if _, ok := adapt(&staticContract{}).(rhandler.OutcomeFinalizer); ok {
				t.Fatal("default contract acquired outcome finalization")
			}
			h := adapt(contract)
			finalizer, ok := h.(rhandler.OutcomeFinalizer)
			if !ok {
				t.Fatal("explicit outcome contract lost finalizer")
			}
			if h.InputType() != reflect.TypeFor[staticInput]() || h.OutputType() != reflect.TypeFor[staticOutput]() {
				t.Fatal("typed surface changed")
			}
			binder := h.(interface {
				BindStatic(context.Context, *bindly.Injector) error
			})
			for _, value := range []string{"first", "second"} {
				injector, err := bindly.NewInjector(bindly.WithProviders(handlerprovider.Static(xhandler.ValueKey("static"), value)))
				if err != nil {
					t.Fatal(err)
				}
				if err := binder.BindStatic(t.Context(), injector); err != nil {
					t.Fatal(err)
				}
			}
			if contract.Dependency != "first" {
				t.Fatal("static binding did not remain once-only")
			}
			input := &staticInput{}
			captured, err := h.(interface {
				CaptureInput(context.Context, any) (any, error)
			}).CaptureInput(t.Context(), input)
			if err != nil || captured != input {
				t.Fatalf("input capture changed: %v %v", captured, err)
			}
			if !h.(interface{ RequiresReadMetadata() bool }).RequiresReadMetadata() {
				t.Fatal("read metadata opt-in lost")
			}
			if !h.(interface{ SupportsIndependentChildTransactions() bool }).SupportsIndependentChildTransactions() {
				t.Fatal("ordinary adapter marker lost")
			}
			invocation := rhandler.Invocation{Input: input, Binder: &testBinder{input: input}, Response: &testResponse{}}
			result, err := h.Execute(t.Context(), invocation)
			if err != nil || result == nil {
				t.Fatalf("execution failed: %v", err)
			}
			cause := errors.New("root cause")
			if err := finalizer.FinalizeOutcome(t.Context(), invocation, result, xhandler.Outcome{Error: cause}); err != contract.failure {
				t.Fatalf("callback error changed: %v", err)
			}
			if contract.calls != 1 || contract.invocation.Input != input || contract.result != result || contract.outcome.Error != cause {
				t.Fatal("callback arguments changed")
			}
		})
	}
}
