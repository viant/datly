package runtime

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
)

type componentInjectors struct {
	runtime *Runtime
	scope   dexec.ProviderScope
}

func (i *componentInjectors) Lookup(ctx context.Context, output any, route xhandler.Route) (xhandler.Binder, error) {
	r, scope := i.runtime, i.scope
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	uri, err := url.ParseRequestURI(route.URL)
	if err != nil || uri.IsAbs() || uri.Host != "" || !strings.HasPrefix(uri.Path, "/") {
		return nil, fmt.Errorf("injector route requires a local absolute path: %q", route.URL)
	}
	component, params, ok := r.publicComponentByRoute(route.Method, uri.Path)
	if !ok {
		return nil, fmt.Errorf("injector component route not found: %s %s", route.Method, uri.Path)
	}
	if (route.Name != "" || route.Scope != "") && (route.Name != component.Key.Name || route.Scope != component.Key.Scope) {
		return nil, fmt.Errorf("injector route does not select named component %s/%s", route.Scope, route.Name)
	}
	endpoint, ok := r.bundle.RouteByMethodPath(route.Method, uri.Path)
	if !ok {
		return nil, fmt.Errorf("injector route metadata not found")
	}
	// Preserve the original protocol providers (including current authorization
	// and HTTP request identity); overlay only the selected route's URL values.
	values := requestprovider.NewValues(requestprovider.WithPathParams(params), requestprovider.WithQuery(uri.Query()))
	providers := []locator.Provider(nil)
	if scope != nil {
		for _, provider := range scope.Providers() {
			if provider.Kind() != requestprovider.PathKind && provider.Kind() != requestprovider.QueryKind {
				providers = append(providers, provider)
			}
		}
	}
	providers = append(providers, values.Providers()...)
	selectedScope, err := handlerengine.ComposeScope(nil, providers...)
	if err != nil {
		return nil, err
	}
	return &componentBinder{
		invoker:  &scopedComponentInvoker{runtime: r, scope: selectedScope},
		request:  dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: endpoint.Method, Path: endpoint.Path}}, BindingOutput: output},
		injector: r.injector,
	}, nil
}

// componentBinder is invoked only through the engine's expiring, serialized
// finalizer capability. It reuses the canonical invoker and Bindly result binding.
type componentBinder struct {
	invoker  dexec.ComponentInvoker
	request  dexec.ComponentRequest
	injector *bindly.Injector
	invoked  bool
	result   any
	err      error
	binder   xhandler.Binder
}

func (b *componentBinder) invoke(ctx context.Context) error {
	if b.invoked {
		return b.err
	}
	b.invoked = true
	b.result, b.err = b.invoker.InvokeComponent(ctx, b.request)
	if b.err != nil {
		return b.err
	}
	scope, err := b.injector.ForScope(handlerprovider.Parameter())
	if err != nil {
		b.err = err
		return err
	}
	b.binder = rhandler.NewBinder(scope, b.result)
	return nil
}
func (b *componentBinder) Bind(ctx context.Context, target any) error {
	if err := b.invoke(ctx); err != nil {
		return err
	}
	if b.result == nil {
		return fmt.Errorf("component returned no result to bind")
	}
	return b.binder.Bind(ctx, target)
}
func (b *componentBinder) Lookup(ctx context.Context, key xhandler.ValueKey) (any, bool, error) {
	if key != xhandler.ResultKey {
		return nil, false, nil
	}
	if err := b.invoke(ctx); err != nil {
		return nil, false, err
	}
	return b.result, true, nil
}
