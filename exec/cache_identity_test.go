package exec

import (
	"context"
	xinvocation "github.com/viant/xdatly/handler/exec"
	"testing"
)

func TestCacheIdentityIsDetachedAndNeverRowAuthorizationBypass(t *testing.T) {
	values := []any{int64(7)}
	ctx := WithCacheIdentity(context.Background(), "EffectiveIDs", values)
	values[0] = int64(99)
	name, actual, ok := CacheIdentityFromContext(ctx)
	if !ok || name != "EffectiveIDs" || actual[0] != int64(7) {
		t.Fatal("mutable retained identity")
	}
	actual[0] = int64(99)
	_, again, _ := CacheIdentityFromContext(ctx)
	if again[0] != int64(7) {
		t.Fatal("caller mutated context proof")
	}
	if xinvocation.InvocationFromContext(ctx).MayBypassRowAuthorization() || xinvocation.IsCacheWarmup(ctx) {
		t.Fatal("identity rendering acquired warmup/row bypass")
	}
	if _, _, ok = CacheIdentityFromContext(context.Background()); ok {
		t.Fatal("ordinary request inferred cache identity")
	}
	if _, _, ok = CacheIdentityFromContext(WithCacheIdentity(context.Background(), "EffectiveIDs", nil)); ok {
		t.Fatal("empty index acquired identity capability")
	}
}
