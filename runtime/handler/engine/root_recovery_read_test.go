package engine

import (
	"context"
	"errors"
	"testing"
)

func TestRootRecoveryReadRetainsOriginAndHookLifetime(t *testing.T) {
	origin, foreign := &dataScope{}, &dataScope{}
	restore := RetainMutationAuthority(withDataScope(context.Background(), origin))
	ctx, closeReads := beginRootRecoveryReads(context.Background(), origin)
	fresh := restore(ctx)
	if !IsRootRecoveryRead(fresh) || fresh.Value(dataScopeContextKey{}) != (*dataScope)(nil) {
		t.Fatal("fresh reader retained completed mutation scope")
	}
	if err := CheckComponentMutation(fresh, true); err != nil {
		t.Fatal(err)
	}
	held := RetainMutationAuthority(fresh)
	if err := closeReads(); err != nil {
		t.Fatal(err)
	}
	if err := CheckComponentMutation(held(context.Background()), true); !errors.Is(err, ErrRootRecoveryReadOnly) {
		t.Fatalf("held reader escaped hook: %v", err)
	}
	ctx, closeReads = beginRootRecoveryReads(context.Background(), origin)
	defer closeReads()
	foreignRestore := RetainMutationAuthority(withDataScope(context.Background(), foreign))
	if err := CheckComponentMutation(foreignRestore(ctx), true); !errors.Is(err, ErrRootRecoveryReadOnly) {
		t.Fatalf("foreign invoker borrowed root recovery: %v", err)
	}
	if err := CheckComponentMutation(restore(ctx), true); !errors.Is(err, ErrRootRecoveryReadOnly) {
		t.Fatalf("ignored foreign violation did not fail closed: %v", err)
	}
}
