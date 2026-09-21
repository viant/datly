package exec

import (
	"context"
	"testing"
)

func TestCacheWarmupInvocationContext(t *testing.T) {
	ordinary := InvocationFromContext(context.Background())
	if ordinary.IsCacheWarmup() || ordinary.MayBypassRowAuthorization() || ordinary.WarmupPhase() != WarmupPhaseNone {
		t.Fatalf("ordinary invocation = %+v", ordinary)
	}
	for _, phase := range []WarmupPhase{WarmupPhasePrepare, WarmupPhaseFill} {
		ctx := WithCacheWarmup(context.Background(), phase)
		actual := InvocationFromContext(ctx)
		if !IsCacheWarmup(ctx) || !actual.IsCacheWarmup() || !actual.MayBypassRowAuthorization() || actual.WarmupPhase() != phase {
			t.Fatalf("warmup invocation for phase %v = %+v", phase, actual)
		}
	}
}
