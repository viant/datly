package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/internal/drainowner"
	rh "github.com/viant/datly/runtime/handler"
	hp "github.com/viant/datly/runtime/handler/provider"
	xh "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/response"
)

func TestReaderReceiptDoesNotAuthorizeOtherCalls(t *testing.T) {
	original := errors.New("reader403")
	owner := neutralDataScope(t.Context())
	receipt := &readerInvocation{owner: owner}
	ctx := context.WithValue(withDataScope(t.Context(), owner), readerInvocationKey{}, receipt)
	if claimReaderCall(ctx) != receipt || claimReaderCall(ctx) != nil {
		t.Fatal("nested invocation reclaimed receipt")
	}
	receipt.publish(original)
	if !receipt.handled(original) || receipt.handled(fmt.Errorf("other call: %w", original)) || receipt.handled(errors.Join(original, errors.New("fatal"))) {
		t.Fatal("receipt certified unrelated aggregate")
	}
	foreignCtx := context.WithValue(withDataScope(t.Context(), neutralDataScope(t.Context())), readerInvocationKey{}, &readerInvocation{owner: owner})
	if claimReaderCall(foreignCtx) != nil {
		t.Fatal("foreign owner claimed receipt")
	}
	another := &readerInvocation{}
	if another.handled(original) {
		t.Fatal("another invocation inherited receipt")
	}
	// Generic binder/provider run keeps failure sticky even with a valid receipt.
	lease := &lifecycleScope{ctx: t.Context(), data: neutralDataScope(t.Context()), phase: "test"}
	err := lease.finish(func(context.Context) error {
		_ = lease.run(ctx, func(context.Context) error { return original })
		return nil
	}, nil)
	if !errors.Is(err, original) {
		t.Fatalf("generic run ignored error: %v", err)
	}
}
func TestNeutralReaderRetirementGuards(t *testing.T) {
	for _, mode := range []string{"neutral", "foreign token", "stale token", "canceled", "completed", "unpublished writer", "published writer", "protected", "captured"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			root := neutralDataScope(ctx)
			if err := root.enrollBufferedScope(ctx); err != nil {
				t.Fatal(err)
			}
			token, err := root.admitActivity(ctx)
			if err != nil {
				t.Fatal(err)
			}
			cause := errors.New("reader403")
			switch mode {
			case "foreign token":
				foreign := neutralDataScope(ctx)
				token, err = foreign.admitActivity(ctx)
				if err != nil {
					t.Fatal(err)
				}
			case "stale token":
				if err = root.finishActivity(token, nil); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "completed":
				root.completionStarted = true
			case "unpublished writer":
				writer, _ := invocationDataScope(withDataScope(ctx, root), &staticDataSource{})
				if writer.source == nil || len(root.units) != 0 {
					t.Fatal("writer source published before resolution")
				}
			case "published writer":
				if _, err = root.databaseUnit(ctx, &staticDataSource{}, "writer", ""); err != nil {
					t.Fatal(err)
				}
			case "protected":
				drainowner.FailProtected(root.nativeInvocation, errors.New("guard"))
			case "captured":
				root.writeEligibility.guardedFailure = errors.New("captured guard")
			}
			retired, err := root.finishNeutralReaderActivity(ctx, token, cause)
			if retired != (mode == "neutral" || mode == "unpublished writer") {
				t.Fatalf("receipt %v error %v", retired, err)
			}
			if mode == "foreign token" || mode == "stale token" {
				if !errors.Is(err, drainowner.ErrActivity) {
					t.Fatalf("invalid activity lost: %v", err)
				}
			}
			if mode == "neutral" || mode == "unpublished writer" {
				if drainowner.ProtectedFailure(root.nativeInvocation) != nil {
					t.Fatal("neutral reader poisoned owner")
				}
				if _, err = root.finishNeutralReaderActivity(ctx, token, cause); !errors.Is(err, drainowner.ErrActivity) {
					t.Fatalf("duplicate retirement accepted: %v", err)
				}
			}
		})
	}
}
func TestCanonicalReaderEarlyFailuresStaySticky(t *testing.T) {
	for _, mode := range []string{"provider collision", "logger lookup", "output binding"} {
		t.Run(mode, func(t *testing.T) {
			cause := &response.Error{Code: 403, Cause: errors.New("early protected failure")}
			root := neutralDataScope(t.Context())
			if err := root.enrollBufferedScope(t.Context()); err != nil {
				t.Fatal(err)
			}
			receipt := &readerInvocation{owner: root}
			ctx := context.WithValue(withDataScope(t.Context(), root), readerInvocationKey{}, receipt)
			ctx = PrepareComponent(ctx, ComponentBufferedImperative, "")
			request := Request{CanonicalReader: true, Input: testRouteInput(t, reflect.TypeFor[struct{}]()), Handler: rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) { return &struct{ Logger xh.Logger }{}, nil })}
			switch mode {
			case "provider collision":
				request.Providers = []locator.Provider{hp.Static(xh.ComponentInvokerKey, lifecycleInvoker(func(context.Context, xh.ComponentRequest) (any, error) { return nil, cause }))}
			case "logger lookup":
			case "output binding":
				injector, err := bindly.NewInjector()
				if err != nil {
					t.Fatal(err)
				}
				request.Injector = injector
				request.OutputCapabilities, err = injector.CompilePlan(reflect.TypeFor[struct{ Logger xh.Logger }](), bindly.BindingSpec{Path: "Logger", Location: bindstate.Location{Kind: "logger"}})
				if err != nil {
					t.Fatal(err)
				}
			}
			if mode == "logger lookup" || mode == "output binding" {
				calls := 0
				request.Providers = []locator.Provider{hp.Named("logger", func(context.Context, reflect.Type, string) (any, bool, error) {
					calls++
					if mode == "output binding" && calls == 1 {
						return &contextLogger{}, true, nil
					}
					return nil, false, cause
				})}
			}
			_, err := New().Execute(ctx, request)
			if err == nil || receipt.handled(err) || drainowner.ProtectedFailure(root.nativeInvocation) == nil {
				t.Fatalf("early failure became handled: %v", err)
			}
		})
	}
}

func TestUnknownInvokerReaderStatusRemainsSticky(t *testing.T) {
	for _, phase := range []string{"init", "finalize", "error finalize"} {
		t.Run(phase, func(t *testing.T) {
			cause := &response.Error{Code: 403, Payload: map[string]any{"unknown": true}, Cause: errors.New("custom invoker")}
			hook := func(ctx context.Context, invoker xh.ComponentInvoker) error {
				_, err := invoker.Invoke(ctx, xh.ComponentRequest{})
				if !errors.Is(err, cause) {
					return fmt.Errorf("custom cause lost: %v", err)
				}
				return nil
			}
			var order []string
			input := &lifecycleInput{order: &order, init: func(context.Context, xh.ComponentInvoker) error { return nil }}
			var output any = &lifecycleOutput{order: &order, finalize: func(context.Context, xh.ComponentInvoker) error { return nil }}
			switch phase {
			case "init":
				input.init = hook
			case "finalize":
				output = &lifecycleOutput{order: &order, finalize: hook}
			case "error finalize":
				output = &invokerErrorOutput{hook: func(ctx context.Context, invoker xh.ComponentInvoker, _ error) error { return hook(ctx, invoker) }}
			}
			_, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[lifecycleInput]()), BoundInput: input, Handler: rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) { return output, nil }), ComponentInvoker: hp.Static(xh.ComponentInvokerKey, lifecycleInvoker(func(context.Context, xh.ComponentRequest) (any, error) { return nil, cause }))})
			if !errors.Is(err, cause) || response.ErrorStatusCode(err, 500) != 403 {
				t.Fatalf("custom403 ignored: %v", err)
			}
		})
	}
}
func TestReaderReceiptCannotHideLateContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	lease := &lifecycleScope{ctx: ctx, data: neutralDataScope(ctx), phase: "test"}
	receipt := &readerInvocation{owner: lease.data}
	lease.ctx = withDataScope(ctx, lease.data)
	cause := errors.New("reader403")
	err := lease.finish(func(context.Context) error {
		_ = lease.runInvocation(ctx, func(call context.Context) error { claimReaderCall(call).publish(cause); cancel(); return cause }, receipt)
		return nil
	}, nil)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
		t.Fatalf("late cancellation lost: %v", err)
	}
}

type incomparableReaderCause []string

func (e incomparableReaderCause) Error() string { return "unstable" }
func TestOrdinaryReaderCauseRequiresEveryBranchIdentity(t *testing.T) {
	ordinary := errors.New("same message")
	different := errors.New("same message")
	protected := errors.New("protected")
	projection := &response.Error{Code: 403, Cause: ordinary}
	for _, tc := range []struct {
		name     string
		cause    error
		eligible bool
	}{
		{"single", ordinary, true}, {"duplicate", errors.Join(ordinary, ordinary), true},
		{"nested projection", errors.Join(fmt.Errorf("read: %w", ordinary), &response.Error{Code: 403, Cause: errors.Join(projection, ordinary)}), true},
		{"same value distinct identity", errors.Join(ordinary, different), false},
		{"additional protected", errors.Join(ordinary, protected), false},
		{"additional canceled", errors.Join(ordinary, context.Canceled), false},
		{"additional deadline", errors.Join(ordinary, context.DeadlineExceeded), false},
		{"uncomparable", incomparableReaderCause{"unstable"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if ordinaryReaderCause(tc.cause) != tc.eligible {
				t.Fatalf("classification %v", tc.cause)
			}
		})
	}
}

type compositeReaderCause struct{ Fact any }

func (e compositeReaderCause) Error() string { return "composite business failure" }

type nestedCompositeReaderCause struct{ Facts [1]struct{ Fact any } }

func (e nestedCompositeReaderCause) Error() string { return "nested composite business failure" }
func TestReaderResultRejectsDynamicallyUncomparableValues(t *testing.T) {
	nested := nestedCompositeReaderCause{}
	nested.Facts[0].Fact = map[string]int{"fact": 1}
	for _, cause := range []error{compositeReaderCause{Fact: []string{"fact"}}, compositeReaderCause{Fact: map[string]int{"fact": 1}}, nested} {
		t.Run(cause.Error()+"/"+reflect.TypeOf(cause).String(), func(t *testing.T) {
			if !reflect.TypeOf(cause).Comparable() || reflect.ValueOf(cause).Comparable() {
				t.Fatal("fixture does not expose dynamic interface comparability")
			}
			projection := &response.Error{Code: 403, Cause: cause}
			if ordinaryReaderCause(cause) || ordinaryReaderCause(errors.Join(cause, cause)) || ordinaryReaderCause(errors.Join(projection, cause)) {
				t.Fatal("uncomparable value received ordinary reader disposition")
			}
			if sameReaderError(cause, cause) || sameReaderError(cause, projection) || sameReaderError(projection, cause) {
				t.Fatal("uncomparable value received exact-result identity")
			}
			receipt := &readerInvocation{}
			receipt.publish(cause)
			if receipt.handled(cause) {
				t.Fatal("uncomparable receipt became handled")
			}
			pointer := &compositeReaderCause{Fact: []string{"fact"}}
			if !ordinaryReaderCause(pointer) || !sameReaderError(pointer, pointer) {
				t.Fatal("safe pointer identity changed")
			}
		})
	}
}
