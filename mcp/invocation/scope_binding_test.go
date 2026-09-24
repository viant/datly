package invocation

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/viant/datly/exec"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
)

// TestInvokerCarriesAuthorizeBoundScopeIntoComponentInvocation proves that the
// MCP path captures a scope binding before Authorize and that providers the hook
// binds are visible to the component invocation through the same context, while
// a denying hook never reaches the component.
func TestInvokerCarriesAuthorizeBoundScopeIntoComponentInvocation(t *testing.T) {
	target := exec.ComponentTarget{
		Component: spec.Key{Kind: spec.KindComponent, Scope: "example.com/app", Name: "Tasks"},
		Route:     spec.RouteRef{Method: "GET", Path: "/tasks"},
	}
	var seen []string
	captured := &captureInvoker{result: map[string]interface{}{"ok": true}}
	captured.check = func(ctx context.Context) {
		for _, provider := range exec.ScopeProviders(ctx) {
			seen = append(seen, provider.Kind())
		}
	}
	authorize := func(ctx context.Context, actual exec.ComponentTarget) error {
		if actual != target {
			return errors.New("unexpected target")
		}
		return exec.BindScope(ctx, handlerprovider.Named("scope", func(context.Context, reflect.Type, string) (any, bool, error) {
			return []int{101}, true, nil
		}))
	}
	if _, protocolErr := invokeTool(context.Background(), New(Config{Invoker: captured, Authorize: authorize}), Request{Target: target}); protocolErr != nil {
		t.Fatal(protocolErr)
	}
	if len(seen) != 1 || seen[0] != "scope" {
		t.Fatalf("bound scope providers did not reach the component: %+v", seen)
	}
	if len(captured.request.Providers) != 0 {
		t.Fatalf("scope binding must not be smuggled through client-facing request providers: %+v", captured.request.Providers)
	}

	denied := &captureInvoker{result: map[string]interface{}{"ok": true}}
	denied.check = func(context.Context) { t.Fatal("denied hook reached the component") }
	if _, protocolErr := invokeTool(context.Background(), New(Config{Invoker: denied, Authorize: func(context.Context, exec.ComponentTarget) error { return errors.New("denied") }}), Request{Target: target}); protocolErr == nil {
		t.Fatal("denial was not reported")
	}

	// Without an Authorize hook nothing captures a binding, so a stray BindScope
	// from elsewhere cannot attach authority to the invocation.
	plain := &captureInvoker{result: map[string]interface{}{"ok": true}}
	plain.check = func(ctx context.Context) {
		if exec.ScopeProviders(ctx) != nil {
			t.Fatal("unauthorized invocation carried scope providers")
		}
		if err := exec.BindScope(ctx, handlerprovider.Named("scope", nil)); !errors.Is(err, exec.ErrScopeBindingUnavailable) {
			t.Fatalf("expected unavailable binding, got %v", err)
		}
	}
	if _, protocolErr := invokeTool(context.Background(), New(Config{Invoker: plain}), Request{Target: target}); protocolErr != nil {
		t.Fatal(protocolErr)
	}
}
