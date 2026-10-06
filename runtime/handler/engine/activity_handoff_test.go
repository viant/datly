package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/drainowner"
	rh "github.com/viant/datly/runtime/handler"
	xh "github.com/viant/xdatly/handler"
)

type activityProviderScope struct{ callback func() }

func (s activityProviderScope) Providers() []locator.Provider { s.callback(); return nil }

// The outer engine creates the canonical neutral root. The child is a real
// engine invocation, including its provider-collection phase, not a fabricated
// ledger activity. This is bookkeeping evidence, not native drain authority.
func TestActivityHandoffRetainsParentAndRetiresEarlyProviderPanic(t *testing.T) {
	for _, panicProvider := range []bool{false, true} {
		name := "success"
		if panicProvider {
			name = "provider panic"
		}
		t.Run(name, func(t *testing.T) {
			cause := errors.New("provider collection failure")
			var root *dataScope
			providerCalls, businessCalls := 0, 0
			childReturned := false
			contract := testRouteInput(t, reflect.TypeFor[struct{}]())
			_, err := New().Execute(t.Context(), Request{
				Input: contract, Completion: func(_ xh.Outcome) {},
				Handler: rh.HandlerFunc(func(ctx context.Context, _ rh.Invocation) (any, error) {
					root = mainScope(ctx)
					_, childErr := New().Execute(ctx, Request{Input: contract,
						Scope: activityProviderScope{callback: func() {
							providerCalls++
							if root.nativeInvocation == nil {
								t.Fatal("provider ran before activity admission")
							}
							if err := drainowner.EnrollActivities(root.nativeInvocation); err != nil {
								t.Fatal(err)
							}
							if err := drainowner.CheckActivities(root.nativeInvocation); !errors.Is(err, drainowner.ErrActivityUnfinished) {
								t.Fatalf("provider sees no active invocation: %v", err)
							}
							if panicProvider {
								panic(cause)
							}
						}},
						Handler: rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) { businessCalls++; return "child", nil }),
					})
					childReturned = true
					if !panicProvider && childErr != nil {
						return nil, childErr
					}
					// Child completion cannot retire the still-composing parent.
					if check := drainowner.CheckActivities(root.nativeInvocation); !errors.Is(check, drainowner.ErrActivityUnfinished) {
						t.Fatalf("parent activity retired before composition ended: %v", check)
					}
					return nil, childErr
				}),
			})
			if !childReturned {
				t.Fatal("provider panic escaped child invocation")
			}
			if providerCalls != 1 {
				t.Fatalf("provider calls=%d", providerCalls)
			}
			check := drainowner.CheckActivities(root.nativeInvocation)
			if errors.Is(check, drainowner.ErrActivityUnfinished) {
				t.Fatalf("early return leaked activity: %v", check)
			}
			if panicProvider {
				var panicError, ledgerPanic *dexec.PanicError
				if !errors.As(err, &panicError) || panicError.Cause() != cause || !errors.As(check, &ledgerPanic) || ledgerPanic.Cause() != cause || businessCalls != 0 {
					t.Fatalf("err=%v ledger=%v business=%d", err, check, businessCalls)
				}
			} else if err != nil || check != nil || businessCalls != 1 {
				t.Fatalf("err=%v ledger=%v business=%d", err, check, businessCalls)
			}
		})
	}
}
