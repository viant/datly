package drainowner

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func guardTestOwner() *attachmentTestOwner {
	o := &attachmentTestOwner{}
	o.State = NewState(o, nil)
	return o
}

func TestGuardBindingExactRegistration(t *testing.T) {
	o, foreign := guardTestOwner(), guardTestOwner()
	b := NewGuardBinding()
	copyBinding := *b
	if err := BindExecutionGuard(o, &copyBinding); !errors.Is(err, ErrGuardBinding) {
		t.Fatalf("copy: %v", err)
	}
	copyOwner := *o
	if err := BindExecutionGuard(&copyOwner, b); !errors.Is(err, ErrOwner) {
		t.Fatalf("owner copy: %v", err)
	}
	wrapper := &struct{ *attachmentTestOwner }{o}
	if err := BindExecutionGuard(wrapper, b); !errors.Is(err, ErrOwner) {
		t.Fatalf("forwarder: %v", err)
	}
	if err := BindExecutionGuard(o, b); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []*attachmentTestOwner{o, foreign} {
		if err := BindExecutionGuard(owner, b); !errors.Is(err, ErrGuardBinding) {
			t.Fatalf("reuse: %v", err)
		}
	}
	if _, _, err := WithGuardPurpose(context.Background(), foreign, b, false); !errors.Is(err, ErrGuardBinding) {
		t.Fatalf("foreign scope: %v", err)
	}
	if _, _, err := WithGuardPurpose(context.Background(), o, &copyBinding, false); !errors.Is(err, ErrGuardBinding) {
		t.Fatalf("copied scope: %v", err)
	}
}

func TestGuardPurposeScope(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		o := guardTestOwner()
		b, other := NewGuardBinding(), NewGuardBinding()
		if err := BindExecutionGuard(o, b); err != nil {
			t.Fatal(err)
		}
		if err := BindExecutionGuard(o, other); err != nil {
			t.Fatal(err)
		}
		ctx, close, err := WithGuardPurpose(context.Background(), o, b, terminal)
		if err != nil {
			t.Fatal(err)
		}
		child, cancel := context.WithCancel(ctx)
		defer cancel()
		for _, validContext := range []context.Context{ctx, child} {
			got, err := GuardPurpose(validContext, b)
			if err != nil || got != terminal {
				t.Fatalf("purpose %v: %v %v", terminal, got, err)
			}
		}
		if _, err := GuardPurpose(ctx, other); !errors.Is(err, ErrGuardScope) {
			t.Fatalf("other callback: %v", err)
		}
		if _, err := GuardPurpose(context.Background(), b); !errors.Is(err, ErrGuardScope) {
			t.Fatalf("replaced context: %v", err)
		}
		close()
		close()
		for _, expired := range []context.Context{ctx, child} {
			if _, err := GuardPurpose(expired, b); !errors.Is(err, ErrGuardScope) {
				t.Fatalf("expired: %v", err)
			}
		}
	}
}

func TestGuardPurposeCloseRace(t *testing.T) {
	o, b := guardTestOwner(), NewGuardBinding()
	if err := BindExecutionGuard(o, b); err != nil {
		t.Fatal(err)
	}
	ctx, close, err := WithGuardPurpose(context.Background(), o, b, true)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = GuardPurpose(ctx, b)
			}
		}()
	}
	close()
	wg.Wait()
	if _, err := GuardPurpose(ctx, b); !errors.Is(err, ErrGuardScope) {
		t.Fatal(err)
	}
}
