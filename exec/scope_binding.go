package exec

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/viant/bindly/locator"
)

// ErrScopeBindingUnavailable reports that no protocol adapter captured a scope
// binding for the current invocation, so trusted providers cannot be attached.
var ErrScopeBindingUnavailable = errors.New("trusted scope binding is unavailable for this invocation")

// ScopeBinding collects trusted, server-owned providers that an application's
// authorization hook attaches to one authorized component invocation. Protocol
// adapters capture a binding before calling their Authorize hook, and the
// runtime composes the captured providers as per-kind authority over
// transport-bound input, so a client cannot supply or override a bound value.
//
// A binding is never populated from a request payload; only code that runs
// with the authorization context may call BindScope. Nested component
// invocations inherit the same binding through their context.
type ScopeBinding struct {
	mu        sync.Mutex
	providers []locator.Provider
}

type scopeBindingKey struct{}

// CaptureScopeBinding attaches a fresh binding to ctx for one authorization
// pass. The adapter must use the returned context for the invocation so that
// the runtime can observe what the hook bound.
func CaptureScopeBinding(ctx context.Context) (context.Context, *ScopeBinding) {
	if ctx == nil {
		ctx = context.Background()
	}
	binding := &ScopeBinding{}
	return context.WithValue(ctx, scopeBindingKey{}, binding), binding
}

// BindScope attaches trusted providers to the invocation captured in ctx. It
// fails when no adapter captured a binding, so an application cannot silently
// lose enforcement on a transport that does not support server-owned input.
func BindScope(ctx context.Context, providers ...locator.Provider) error {
	if ctx == nil {
		return ErrScopeBindingUnavailable
	}
	binding, _ := ctx.Value(scopeBindingKey{}).(*ScopeBinding)
	if binding == nil {
		return ErrScopeBindingUnavailable
	}
	for index, provider := range providers {
		if provider == nil {
			return fmt.Errorf("scope provider at index %d is required", index)
		}
		if provider.Kind() == "" {
			return fmt.Errorf("scope provider at index %d has no kind", index)
		}
	}
	binding.mu.Lock()
	defer binding.mu.Unlock()
	binding.providers = append(binding.providers, providers...)
	return nil
}

// ScopeProviders returns the providers bound for the invocation in ctx, or nil
// when nothing was captured or bound.
func ScopeProviders(ctx context.Context) []locator.Provider {
	if ctx == nil {
		return nil
	}
	binding, _ := ctx.Value(scopeBindingKey{}).(*ScopeBinding)
	return binding.Providers()
}

// Providers returns a copy of the bound providers; nil for an empty binding.
func (b *ScopeBinding) Providers() []locator.Provider {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.providers) == 0 {
		return nil
	}
	return append([]locator.Provider(nil), b.providers...)
}
