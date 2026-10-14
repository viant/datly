package engine

import (
	"context"
	"errors"
	"testing"
	"time"
)

type lifecycleMarkerKey struct{}
type lifecycleOtherKey struct{}

func TestLifecycleCallContextLexicalValuesAndAuthority(t *testing.T) {
	root := neutralDataScope(t.Context())
	if err := root.enrollBufferedScope(t.Context()); err != nil {
		t.Fatal(err)
	}
	trustedReceipt := &readerInvocation{owner: root}
	foreign := neutralDataScope(t.Context())
	fakeReceipt := &readerInvocation{owner: foreign}
	base := withDataScope(context.WithValue(t.Context(), lifecycleMarkerKey{}, false), root)
	base = context.WithValue(base, readerInvocationKey{}, trustedReceipt)
	base = PrepareComponent(base, ComponentBufferedImperative, "original")
	call := context.WithValue(context.Background(), lifecycleMarkerKey{}, true)
	call = context.WithValue(call, lifecycleOtherKey{}, "call only")
	call = withDataScope(call, foreign)
	call = context.WithValue(call, readerInvocationKey{}, fakeReceipt)
	call = PrepareComponent(call, ComponentImperative, "forged")
	merged := RetainMutationAuthority(base)(lifecycleCallContext{Context: base, call: call})
	if merged.Value(lifecycleMarkerKey{}) != true || merged.Value(lifecycleOtherKey{}) != "call only" {
		t.Fatal("ordinary lexical call values lost")
	}
	if merged.Value(dataScopeContextKey{}) != root || merged.Value(readerInvocationKey{}) != trustedReceipt || componentFromContext(merged).order != "original" {
		t.Fatal("call replaced trusted lease authority")
	}
	without := lifecycleCallContext{Context: base, call: context.Background()}
	if without.Value(lifecycleMarkerKey{}) != false || without.Value(lifecycleOtherKey{}) != nil {
		t.Fatal("per-call marker leaked")
	}
	noReceipt := lifecycleCallContext{Context: withDataScope(t.Context(), root), call: call}
	if noReceipt.Value(readerInvocationKey{}) != nil {
		t.Fatal("caller spoofed missing receipt")
	}
}
func TestLifecycleCallCancellationAndDeadline(t *testing.T) {
	for _, mode := range []string{"caller canceled", "lease canceled", "caller deadline", "lease deadline"} {
		t.Run(mode, func(t *testing.T) {
			base := context.Background()
			call := context.Background()
			var cancel context.CancelFunc
			if mode == "lease canceled" || mode == "lease deadline" {
				if mode == "lease canceled" {
					base, cancel = context.WithCancel(base)
				} else {
					base, cancel = context.WithDeadline(base, time.Now().Add(-time.Second))
				}
			} else {
				if mode == "caller canceled" {
					call, cancel = context.WithCancel(call)
				} else {
					call, cancel = context.WithDeadline(call, time.Now().Add(-time.Second))
				}
			}
			defer cancel()
			if mode == "caller canceled" || mode == "lease canceled" {
				cancel()
			}
			lease := &lifecycleScope{ctx: base, data: neutralDataScope(base), phase: "test"}
			called := false
			err := lease.run(call, func(context.Context) error { called = true; return nil })
			expected := error(context.Canceled)
			if mode == "caller deadline" || mode == "lease deadline" {
				expected = context.DeadlineExceeded
			}
			if called || !errors.Is(err, expected) {
				t.Fatalf("phase %s called %v error %v", mode, called, err)
			}
		})
	}
	for _, callerEarlier := range []bool{false, true} {
		t.Run("earliest deadline "+map[bool]string{false: "lease", true: "caller"}[callerEarlier], func(t *testing.T) {
			first, second := time.Now().Add(time.Minute), time.Now().Add(2*time.Minute)
			leaseDeadline, callDeadline := first, second
			if callerEarlier {
				leaseDeadline, callDeadline = second, first
			}
			base, cancelBase := context.WithDeadline(t.Context(), leaseDeadline)
			defer cancelBase()
			call, cancelCall := context.WithDeadline(context.Background(), callDeadline)
			defer cancelCall()
			lease := &lifecycleScope{ctx: base, data: neutralDataScope(base), phase: "test"}
			if err := lease.run(call, func(ctx context.Context) error {
				deadline, ok := ctx.Deadline()
				if !ok || !deadline.Equal(first) {
					t.Errorf("deadline %v expected %v", deadline, first)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestLifecycleCallLiveCancellation(t *testing.T) {
	for _, byCaller := range []bool{false, true} {
		t.Run(map[bool]string{false: "lease", true: "caller"}[byCaller], func(t *testing.T) {
			base, cancelBase := context.WithCancel(t.Context())
			defer cancelBase()
			call, cancelCall := context.WithCancel(context.Background())
			defer cancelCall()
			lease := &lifecycleScope{ctx: base, data: neutralDataScope(base), phase: "test"}
			err := lease.run(call, func(ctx context.Context) error {
				if byCaller {
					cancelCall()
				} else {
					cancelBase()
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
					t.Fatal("cancellation did not reach invocation")
					return nil
				}
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost: %v", err)
			}
		})
	}
}
