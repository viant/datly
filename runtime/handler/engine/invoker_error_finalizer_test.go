package engine

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	xhandler "github.com/viant/xdatly/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
	"github.com/viant/xdatly/response"
)

type invokerErrorOutput struct {
	Value           string
	Logger          xhandler.Logger
	calls, mcpCalls int
	seen            error
	hook            func(context.Context, xhandler.ComponentInvoker, error) error
}

func (o *invokerErrorOutput) Finalize(ctx context.Context, invoker xhandler.ComponentInvoker, cause error) error {
	o.calls++
	o.seen = cause
	if o.hook != nil {
		return o.hook(ctx, invoker, cause)
	}
	return nil
}
func (o *invokerErrorOutput) FinalizeMCP(context.Context, xmcp.Context) error {
	o.mcpCalls++
	return nil
}

var _ xhandler.InvokerErrorFinalizer = (*invokerErrorOutput)(nil)

type invokerErrorData struct {
	preBindingData
	prepareErr          error
	prepares, completes int
	cause               error
}

func (*invokerErrorData) BeginInvocation() error { return nil }
func (d *invokerErrorData) PrepareFinalization(context.Context) error {
	d.prepares++
	return d.prepareErr
}
func (d *invokerErrorData) Complete(_ context.Context, cause error) error {
	d.completes++
	d.cause = cause
	return cause
}

func TestInvokerErrorFinalizerAccumulatedErrorGate(t *testing.T) {
	operation := errors.New("handler failure")
	bindingCause := errors.New("output capability failure")
	prepareCause := errors.New("preparation failure")
	for _, mode := range []string{"handler", "binding", "handler and binding", "prepare"} {
		for _, returnCause := range []bool{false, true} {
			t.Run(mode+map[bool]string{false: "/nil", true: "/cause"}[returnCause], func(t *testing.T) {
				data := &invokerErrorData{}
				input := testRouteInput(t, reflect.TypeFor[struct{}]())
				out := &invokerErrorOutput{Value: "untouched"}
				providerCalls, childCalls, completions := 0, 0, 0
				out.hook = func(_ context.Context, _ xhandler.ComponentInvoker, cause error) error {
					if cause == nil {
						t.Fatal("gate received success")
					}
					if returnCause {
						return cause
					}
					return nil
				}
				request := Request{Input: input, OutputType: reflect.TypeFor[invokerErrorOutput](), DataSource: &staticDataSource{data: data},
					ComponentInvoker: handlerprovider.Named("component_invoker", func(context.Context, reflect.Type, string) (any, bool, error) {
						providerCalls++
						return lifecycleInvoker(func(context.Context, xhandler.ComponentRequest) (any, error) { childCalls++; return nil, nil }), true, nil
					}),
					Handler: rhandler.HandlerFunc(func(ctx context.Context, in rhandler.Invocation) (any, error) {
						if _, _, err := in.Binder.Lookup(ctx, xhandler.DataKey); err != nil {
							return nil, err
						}
						if strings.Contains(mode, "handler") {
							return out, operation
						}
						return out, nil
					}),
					Completion: func(outcome xhandler.Outcome) {
						completions++
						if outcome.Error == nil || outcome.CommitConfirmed() {
							t.Fatalf("failure became success: %+v", outcome)
						}
					},
				}
				if strings.Contains(mode, "binding") {
					injector, err := bindly.NewInjector()
					if err != nil {
						t.Fatal(err)
					}
					required := true
					plan, err := injector.CompilePlan(reflect.TypeFor[invokerErrorOutput](), bindly.BindingSpec{Path: "Logger", Location: bindstate.Location{Kind: "finalizer_probe"}, Required: &required, ErrorCode: 418})
					if err != nil {
						t.Fatal(err)
					}
					request.OutputCapabilities = plan
					request.Providers = append(request.Providers, handlerprovider.Named("finalizer_probe", func(context.Context, reflect.Type, string) (any, bool, error) { return nil, false, bindingCause }))
				}
				if mode == "prepare" {
					data.prepareErr = &response.Error{Code: 409, Payload: "prepare", Cause: prepareCause}
				}
				actual, err := New().Execute(t.Context(), request)
				if err == nil || out.calls != 1 || out.Value != "untouched" || providerCalls != 0 || childCalls != 0 || completions != 1 || data.completes != 1 || data.cause == nil {
					t.Fatalf("result=%T err=%v hook=%d providers=%d children=%d completion=%d data=%+v", actual, err, out.calls, providerCalls, childCalls, completions, data)
				}
				if mode == "handler" && out.seen != operation {
					t.Fatalf("exact cause=%v", out.seen)
				}
				for cause, present := range map[error]bool{operation: strings.Contains(mode, "handler"), bindingCause: strings.Contains(mode, "binding"), prepareCause: mode == "prepare"} {
					if present && (!errors.Is(out.seen, cause) || !errors.Is(err, cause)) {
						t.Fatalf("lost cause %v: seen=%v err=%v", cause, out.seen, err)
					}
				}
				if strings.Contains(mode, "binding") && response.ErrorStatusCode(err, 500) != 418 {
					t.Fatalf("binding status lost: %v", err)
				}
				if mode == "prepare" {
					body, ok := response.ErrorBody(err)
					if actual != nil || !ok || body != "prepare" || response.ErrorStatusCode(err, 500) != 409 {
						t.Fatalf("prepare projection result=%T body=%v err=%v", actual, body, err)
					}
				} else if actual != out {
					t.Fatalf("failure output changed: %T", actual)
				}
			})
		}
	}
}

func TestInvokerErrorFinalizerLeaseFailureBoundaries(t *testing.T) {
	child := errors.New("child failure")
	operation := &response.Error{Code: 403, Payload: "operation", Cause: errors.New("original failure")}
	for _, mode := range []string{"returned", "ignored", "panic", "canceled", "missing provider", "unused"} {
		t.Run(mode, func(t *testing.T) {
			calls, completions := 0, 0
			var retained xhandler.ComponentInvoker
			out := &invokerErrorOutput{hook: func(ctx context.Context, invoker xhandler.ComponentInvoker, cause error) error {
				if cause != operation {
					t.Fatalf("cause=%v", cause)
				}
				retained = invoker
				if mode == "unused" {
					return nil
				}
				if mode == "panic" {
					panic("private callback panic")
				}
				if mode == "canceled" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				_, err := invoker.Invoke(ctx, xhandler.ComponentRequest{})
				if mode == "ignored" {
					return nil
				}
				return err
			}}
			request := Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), OutputType: reflect.TypeFor[invokerErrorOutput](), Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) { return out, operation }), Completion: func(xhandler.Outcome) { completions++ }}
			if mode != "missing provider" && mode != "unused" {
				request.ComponentInvoker = handlerprovider.Static(xhandler.ComponentInvokerKey, lifecycleInvoker(func(context.Context, xhandler.ComponentRequest) (any, error) { calls++; return nil, child }))
			}
			_, err := New().Execute(t.Context(), request)
			if !errors.Is(err, operation) || out.calls != 1 || completions != 1 {
				t.Fatalf("original/completion lost: err=%v hook=%d completion=%d", err, out.calls, completions)
			}
			if (mode == "returned" || mode == "ignored") && (!errors.Is(err, child) || calls != 1) {
				t.Fatalf("child err=%v calls=%d", err, calls)
			}
			if mode == "panic" {
				var panicErr *dexec.PanicError
				if !errors.As(err, &panicErr) {
					t.Fatalf("panic not recovered: %v", err)
				}
			}
			if mode == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost: %v", err)
			}
			if mode == "missing provider" && err == operation {
				t.Fatal("missing provider error lost")
			}
			if mode == "unused" && err != operation {
				t.Fatalf("unused invoker needed provider: %v", err)
			}
			body, ok := response.ErrorBody(err)
			if !ok || body != "operation" || response.ErrorStatusCode(err, 500) != 403 {
				t.Fatalf("original projection changed: body=%v err=%v", body, err)
			}
			if _, err := retained.Invoke(t.Context(), xhandler.ComponentRequest{}); err == nil || !strings.Contains(err.Error(), "scope is closed") {
				t.Fatalf("expired invoker=%v", err)
			}
		})
	}
}

func TestInvokerErrorFinalizerDoesNotSynthesizeEarlyOutput(t *testing.T) {
	for _, mode := range []string{"bind", "init", "no output"} {
		t.Run(mode, func(t *testing.T) {
			failure := errors.New("early failure")
			out := &invokerErrorOutput{}
			calls := 0
			request := Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), OutputType: reflect.TypeFor[invokerErrorOutput](), Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) { calls++; return nil, failure })}
			if mode == "bind" {
				request.Input = testRouteInput(t, reflect.TypeFor[earlyOutputInput]())
				request.Scope = testharness.Request{}.WithQuery(url.Values{})
			}
			if mode == "init" {
				var order []string
				request.Input = testRouteInput(t, reflect.TypeFor[lifecycleInput]())
				request.BoundInput = &lifecycleInput{order: &order, init: func(context.Context, xhandler.ComponentInvoker) error { return failure }}
			}
			actual, err := New().Execute(t.Context(), request)
			if actual != nil || err == nil || out.calls != 0 || (mode != "no output" && calls != 0) {
				t.Fatalf("early result=%T err=%v executes=%d", actual, err, calls)
			}
			if mode != "bind" && !errors.Is(err, failure) {
				t.Fatalf("early cause lost: %v", err)
			}
			if errorAwareOutput(reflect.TypeFor[invokerErrorOutput]()) != nil {
				t.Fatal("composition output synthesized")
			}
		})
	}
}

func TestInvokerErrorFinalizerOwnershipDetection(t *testing.T) {
	contract := custom.NewFunc[struct{}, invokerErrorOutput](func(context.Context, *struct{}) (*invokerErrorOutput, error) { return nil, nil })
	for _, request := range []Request{{OutputType: reflect.TypeFor[invokerErrorOutput]()}, {OutputType: reflect.TypeFor[*invokerErrorOutput]()}, {Handler: contract}} {
		if !request.hasCompositionFinalizer() {
			t.Fatal("composition contract not detected")
		}
	}
	err := validateIndependentChildren(t.Context(), Request{Handler: contract})
	var rejected *dexec.IndependentChildTransactionError
	if !errors.As(err, &rejected) || rejected.Reason != "root owns composition finalization" {
		t.Fatalf("ownership guard=%v", err)
	}
}

func TestInvokerLifecycleReturnedPublicProjection(t *testing.T) {
	private := errors.New("private child")
	child := &response.Error{Code: 418, Payload: "child diagnostics", Cause: private}
	parent := &response.Error{Code: 401, Payload: map[string]any{"message": "parent policy"}, Cause: child}
	for _, phase := range []string{"init", "finalize", "error finalize"} {
		t.Run(phase, func(t *testing.T) {
			var order []string
			input := &lifecycleInput{order: &order, init: func(context.Context, xhandler.ComponentInvoker) error { return nil }}
			hook := func(ctx context.Context, invoker xhandler.ComponentInvoker) error {
				_, err := invoker.Invoke(ctx, xhandler.ComponentRequest{})
				if !errors.Is(err, child) {
					t.Fatalf("child=%v", err)
				}
				return parent
			}
			output := any(&lifecycleOutput{order: &order, finalize: hook})
			if phase == "init" {
				input.init = hook
			}
			if phase == "error finalize" {
				output = &invokerErrorOutput{hook: func(ctx context.Context, invoker xhandler.ComponentInvoker, _ error) error { return hook(ctx, invoker) }}
			}
			_, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[lifecycleInput]()), BoundInput: input, ComponentInvoker: handlerprovider.Static(xhandler.ComponentInvokerKey, lifecycleInvoker(func(context.Context, xhandler.ComponentRequest) (any, error) { return nil, child })), Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) { return output, nil })})
			body, ok := response.ErrorBody(err)
			if !errors.Is(err, child) || !errors.Is(err, private) || !errors.Is(err, parent) || !ok || !reflect.DeepEqual(body, parent.Payload) || response.ErrorStatusCode(err, 500) != 401 {
				t.Fatalf("projection body=%v error=%v", body, err)
			}
		})
	}
}
