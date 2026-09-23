package exec

import (
	"context"
	"errors"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/structology"
)

type scopeTestProvider struct{ kind string }

func (p scopeTestProvider) Kind() string                              { return p.kind }
func (p scopeTestProvider) Priority() int                             { return 0 }
func (p scopeTestProvider) DefaultCacheable() bool                    { return true }
func (p scopeTestProvider) Locate(*structology.State) locator.Locator { return nil }

func TestBindScopeRequiresCapturedBinding(t *testing.T) {
	if err := BindScope(context.Background(), scopeTestProvider{kind: "scope"}); !errors.Is(err, ErrScopeBindingUnavailable) {
		t.Fatalf("expected ErrScopeBindingUnavailable, got %v", err)
	}
	if err := BindScope(nil, scopeTestProvider{kind: "scope"}); !errors.Is(err, ErrScopeBindingUnavailable) { //nolint:staticcheck
		t.Fatalf("nil context: expected ErrScopeBindingUnavailable, got %v", err)
	}
	if providers := ScopeProviders(context.Background()); providers != nil {
		t.Fatalf("uncaptured context returned providers %+v", providers)
	}
}

func TestCaptureScopeBindingExposesBoundProvidersThroughContext(t *testing.T) {
	ctx, binding := CaptureScopeBinding(context.Background())
	if binding == nil || binding.Providers() != nil || ScopeProviders(ctx) != nil {
		t.Fatalf("fresh binding is not empty: %+v", binding.Providers())
	}
	first := scopeTestProvider{kind: "scope"}
	second := scopeTestProvider{kind: "tenant"}
	if err := BindScope(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := BindScope(context.WithValue(ctx, struct{}{}, "derived"), second); err != nil {
		t.Fatalf("derived context lost the binding: %v", err)
	}
	providers := ScopeProviders(ctx)
	if len(providers) != 2 || providers[0].Kind() != "scope" || providers[1].Kind() != "tenant" {
		t.Fatalf("providers=%+v", providers)
	}
	// Returned slices are copies; mutating them must not alter the binding.
	providers[0] = scopeTestProvider{kind: "tampered"}
	if again := ScopeProviders(ctx); again[0].Kind() != "scope" {
		t.Fatalf("binding aliased caller slice: %+v", again)
	}
	// A separate capture is isolated from the first.
	other, _ := CaptureScopeBinding(context.Background())
	if ScopeProviders(other) != nil {
		t.Fatal("captures shared state")
	}
}

func TestBindScopeRejectsNilOrKindlessProviders(t *testing.T) {
	ctx, _ := CaptureScopeBinding(context.Background())
	if err := BindScope(ctx, nil); err == nil {
		t.Fatal("nil provider accepted")
	}
	if err := BindScope(ctx, scopeTestProvider{kind: ""}); err == nil {
		t.Fatal("kindless provider accepted")
	}
	if ScopeProviders(ctx) != nil {
		t.Fatal("rejected providers were partially bound")
	}
}
