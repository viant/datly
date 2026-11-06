package provider

import (
	"context"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/state"
)

type bindingScopeProbe struct{ calls int }

func (p *bindingScopeProbe) Bind(context.Context, any) error { p.calls++; return nil }
func (p *bindingScopeProbe) BindTarget(ctx context.Context, target any) error {
	return p.Bind(ctx, target)
}
func (*bindingScopeProbe) Value(context.Context, *state.Location) (any, bool, error) {
	return nil, false, nil
}

func TestViewScopeBinderDoesNotSilentlyDiscardOptions(t *testing.T) {
	scope := &bindingScopeProbe{}
	binder := scopeBinder{scope: scope}
	var target struct{}
	if err := binder.Bind(context.Background(), &target); err != nil || scope.calls != 1 {
		t.Fatal(err, scope.calls)
	}
	if err := binder.Bind(context.Background(), &target, bindly.WithSource(&struct{}{})); err == nil || scope.calls != 1 {
		t.Fatal("unsupported options were ignored", err, scope.calls)
	}
}
