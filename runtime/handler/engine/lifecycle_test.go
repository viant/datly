package engine

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	xhandler "github.com/viant/xdatly/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

type lifecycleInput struct {
	Value    string
	Injected xhandler.ComponentInvoker
	init     func(context.Context, xhandler.ComponentInvoker) error
	order    *[]string
}

func (i *lifecycleInput) Init(ctx context.Context, invoker xhandler.ComponentInvoker) error {
	*i.order = append(*i.order, "init")
	return i.init(ctx, invoker)
}
func (i *lifecycleInput) InitMCP(context.Context, xmcp.Context) error {
	*i.order = append(*i.order, "init-mcp")
	return nil
}

type lifecycleOutput struct {
	finalize func(context.Context, xhandler.ComponentInvoker) error
	order    *[]string
}

func (o *lifecycleOutput) Finalize(ctx context.Context, invoker xhandler.ComponentInvoker) error {
	*o.order = append(*o.order, "finalize")
	return o.finalize(ctx, invoker)
}
func (o *lifecycleOutput) FinalizeMCP(context.Context, xmcp.Context) error {
	*o.order = append(*o.order, "finalize-mcp")
	return nil
}

type lifecycleInvoker func(context.Context, xhandler.ComponentRequest) (any, error)

func (f lifecycleInvoker) Invoke(ctx context.Context, request xhandler.ComponentRequest) (any, error) {
	return f(ctx, request)
}

func TestInvokerLifecycleBindingOrderAndLease(t *testing.T) {
	var order []string
	var retained []xhandler.ComponentInvoker
	calls := 0
	ctx := xmcp.WithContext(t.Context(), engineMCPContext{})
	invoker := lifecycleInvoker(func(ctx context.Context, request xhandler.ComponentRequest) (any, error) {
		calls++
		if _, ok := xmcp.LookupContext(ctx); !ok {
			t.Fatal("replacement call context lost invocation values")
		}
		return request.Input, nil
	})
	input := &lifecycleInput{order: &order}
	input.init = func(ctx context.Context, capability xhandler.ComponentInvoker) error {
		if input.Injected == nil {
			t.Fatal("input capability injection was not preserved")
		}
		if _, err := input.Injected.Invoke(ctx, xhandler.ComponentRequest{}); err != nil {
			return err
		}
		retained = append(retained, capability)
		value, err := capability.Invoke(context.Background(), xhandler.ComponentRequest{Input: "initialized"})
		if err == nil {
			input.Value = value.(string)
		}
		return err
	}
	output := &lifecycleOutput{order: &order, finalize: func(ctx context.Context, capability xhandler.ComponentInvoker) error {
		if _, err := retained[0].Invoke(ctx, xhandler.ComponentRequest{}); err == nil {
			t.Fatal("Init invoker escaped into Finalize")
		}
		retained = append(retained, capability)
		_, err := capability.Invoke(ctx, xhandler.ComponentRequest{})
		return err
	}}
	request := Request{
		Input:      testRouteInput(t, reflect.TypeFor[lifecycleInput](), bindly.BindingSpec{Path: "Injected", Location: bindstate.Location{Kind: "component_invoker"}}),
		BoundInput: input, OutputType: reflect.TypeFor[lifecycleOutput](),
		ComponentInvoker: handlerprovider.Static(xhandler.ComponentInvokerKey, invoker),
		Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) {
			order = append(order, "execute")
			if input.Value != "initialized" {
				t.Fatal("handler ran before initialization")
			}
			return output, nil
		}),
		Completion: func(outcome xhandler.Outcome) {
			order = append(order, "complete")
			if outcome.Error != nil {
				t.Error(outcome.Error)
			}
		},
	}
	actual, err := New().Execute(ctx, request)
	if err != nil || actual != output {
		t.Fatalf("result=%v err=%v", actual, err)
	}
	if got := strings.Join(order, ","); got != "init,init-mcp,execute,finalize,complete,finalize-mcp" {
		t.Fatalf("order=%s", got)
	}
	for _, capability := range retained {
		if _, err := capability.Invoke(ctx, xhandler.ComponentRequest{}); err == nil || !strings.Contains(err.Error(), "scope is closed") {
			t.Fatalf("escaped capability: %v", err)
		}
	}
	if calls != 3 {
		t.Fatalf("child calls=%d", calls)
	}
}

func TestInvokerLifecycleFailureBoundaries(t *testing.T) {
	failure := errors.New("child failed")
	operation := errors.New("handler failed")
	for _, phase := range []string{"init", "finalize"} {
		for _, mode := range []string{"returned", "swallowed", "panic", "canceled", "missing provider", "unused"} {
			t.Run(phase+"/"+mode, func(t *testing.T) {
				var order []string
				hook := func(ctx context.Context, invoker xhandler.ComponentInvoker) error {
					if mode == "unused" {
						return nil
					}
					if mode == "panic" {
						panic("private lifecycle panic")
					}
					if mode == "canceled" {
						var cancel context.CancelFunc
						ctx, cancel = context.WithCancel(ctx)
						cancel()
					}
					_, err := invoker.Invoke(ctx, xhandler.ComponentRequest{})
					if mode == "swallowed" {
						return nil
					}
					return err
				}
				noop := func(context.Context, xhandler.ComponentInvoker) error { return nil }
				input := &lifecycleInput{order: &order, init: noop}
				output := &lifecycleOutput{order: &order, finalize: noop}
				if phase == "init" {
					input.init = hook
				} else {
					output.finalize = hook
				}
				executes, completions := 0, 0
				request := Request{Input: testRouteInput(t, reflect.TypeFor[lifecycleInput]()), BoundInput: input, OutputType: reflect.TypeFor[lifecycleOutput](),
					Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) {
						executes++
						if mode == "returned" && phase == "finalize" {
							return output, operation
						}
						return output, nil
					}),
					Completion: func(xhandler.Outcome) { completions++ },
				}
				if mode != "missing provider" && mode != "unused" {
					request.ComponentInvoker = handlerprovider.Static(xhandler.ComponentInvokerKey, lifecycleInvoker(func(context.Context, xhandler.ComponentRequest) (any, error) { return nil, failure }))
				}
				_, err := New().Execute(t.Context(), request)
				if mode == "unused" {
					if err != nil {
						t.Fatal(err)
					}
				} else if err == nil {
					t.Fatal("lifecycle failure was swallowed")
				}
				if (mode == "returned" || mode == "swallowed") && !errors.Is(err, failure) {
					t.Fatalf("lost child failure: %v", err)
				}
				if phase == "finalize" && mode == "returned" && !errors.Is(err, operation) {
					t.Fatalf("lost handler failure: %v", err)
				}
				if mode == "canceled" && !errors.Is(err, context.Canceled) {
					t.Fatalf("lost cancellation: %v", err)
				}
				if phase == "init" && mode != "unused" && executes != 0 {
					t.Fatal("execution ran after failed Init")
				}
				if completions != 1 {
					t.Fatalf("completions=%d", completions)
				}
			})
		}
	}
}

func TestInvokerFinalizerAndOutcomeAreSeparatePhases(t *testing.T) {
	var order []string
	output := &lifecycleOutput{order: &order, finalize: func(context.Context, xhandler.ComponentInvoker) error { return nil }}
	handler := &outcomeAwareHandler{
		execute: func(context.Context, rhandler.Invocation) (any, error) {
			order = append(order, "execute")
			return output, nil
		},
		finalize: func(_ context.Context, _ rhandler.Invocation, _ any, outcome xhandler.Outcome) error {
			order = append(order, "outcome")
			return outcome.Error
		},
	}
	_, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), OutputType: reflect.TypeFor[lifecycleOutput](), Handler: handler})
	if err != nil || strings.Join(order, ",") != "execute,finalize,outcome" {
		t.Fatalf("order=%v err=%v", order, err)
	}
}
