package provider

import (
	"context"
	"errors"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/internal/invocationbinder"
	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
	"testing"
)

func TestInvocationBinderPreservesCurrentEvaluationAndCancellation(t *testing.T) {
	provider := InvocationBinder()
	if provider.(locator.CachePolicy).DefaultCacheable() {
		t.Fatal("active binder must not be cached across nested evaluation contexts")
	}
	injector, err := bindly.NewInjector(bindly.WithProviders(provider))
	if err != nil {
		t.Fatal(err)
	}
	type target struct {
		Binder xhandler.Binder `bind:"kind=binder,required"`
	}
	outer := rhandler.NewBinder(injector, &struct{ Name string }{"outer"})
	inner := rhandler.NewBinder(injector, &struct{ Name string }{"inner"})
	ctx := invocationbinder.With(context.Background(), outer)
	for _, test := range []struct {
		ctx  context.Context
		want xhandler.Binder
	}{{ctx, outer}, {invocationbinder.With(ctx, inner), inner}, {ctx, outer}} {
		value := &target{}
		if err := injector.Bind(test.ctx, value); err != nil {
			t.Fatal(err)
		}
		if value.Binder != test.want {
			t.Fatal("binder leaked across evaluation scopes")
		}
	}
	if err := injector.Bind(context.Background(), &target{}); err == nil {
		t.Fatal("missing active binder accepted")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := injector.Bind(canceled, &target{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}
