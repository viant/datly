package handler

import (
	"context"
	"errors"
	"testing"
)

func TestServerGuardCaptureDoesNotAliasOrRenew(t *testing.T) {
	calls := 0
	ctx := WithServerExecutionGuard(context.Background(), func(context.Context) error { calls++; return nil })
	captured := ServerExecutionGuards(ctx)
	captured[0] = func(context.Context) error { return errors.New("mutated copy") }
	if err := CheckServerExecutionGuards(ctx); err != nil || calls != 1 {
		t.Fatalf("capture alias err=%v calls=%d", err, calls)
	}
	expired, cancel := context.WithCancel(ctx)
	cancel()
	if !errors.Is(CheckServerExecutionGuards(expired), context.Canceled) {
		t.Fatal("canceled original context renewed")
	}
	if calls != 1 {
		t.Fatal("guard called after operation expiry")
	}
}
