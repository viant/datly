package runtime

import (
	"context"
	"fmt"
	"github.com/viant/datly/observability/otel"
	xexec "github.com/viant/xdatly/exec"
	"reflect"
	"strings"
	"time"

	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	rreader "github.com/viant/datly/runtime/handler/reader"
	"github.com/viant/datly/spec"
)

type componentStackKey struct{}

func (r *Runtime) componentProvider(scope dexec.ProviderScope, parent *spec.Component) locator.Provider {
	return handlerprovider.Named(string(spec.KindComponent), func(ctx context.Context, _ reflect.Type, name string) (any, bool, error) {
		ctx = handlerengine.PrepareComponent(ctx, handlerengine.ComponentBinding, componentBindingOrder(parent, name))
		ref, err := spec.ParseRouteRef(name)
		if err != nil {
			return nil, false, err
		}
		component, _, ok := r.bundle.ComponentByRouteWithParams(ref.Method, ref.Path)
		if !ok || component == nil {
			return nil, false, fmt.Errorf("component route not found: %s", ref.String())
		}
		value, err := r.invokeComponent(ctx, dexec.ComponentRequest{
			Target: dexec.ComponentTarget{Component: component.Key, Route: ref},
		}, scope)
		return value, err == nil, err
	})
}

type scopedComponentInvoker struct {
	runtime *Runtime
	scope   dexec.ProviderScope
}

func (i *scopedComponentInvoker) InvokeComponent(ctx context.Context, request dexec.ComponentRequest) (any, error) {
	if i == nil || i.runtime == nil {
		return nil, fmt.Errorf("component invoker is not configured")
	}
	ctx = handlerengine.PrepareComponent(ctx, handlerengine.ComponentImperative, "")
	return i.runtime.invokeComponent(ctx, request, i.scope)
}

func (r *Runtime) componentInvokerProvider(scope dexec.ProviderScope) locator.Provider {
	return handlerprovider.Static(dexec.ComponentInvokerKey, dexec.ComponentInvoker(&scopedComponentInvoker{
		runtime: r,
		scope:   scope,
	}))
}

// InvokeComponent executes one exact component target without route lookup.
func (r *Runtime) InvokeComponent(ctx context.Context, request dexec.ComponentRequest) (any, error) {
	return r.invokeComponent(ctx, request, nil)
}

func (r *Runtime) invokeComponent(ctx context.Context, request dexec.ComponentRequest, scope dexec.ProviderScope) (_ any, err error) {
	if r == nil || r.invoker == nil || r.injector == nil {
		return nil, fmt.Errorf("runtime component invocation is not configured")
	}
	if xexec.GetContext(ctx) == nil {
		ctx = xexec.WithContext(ctx, xexec.New(xexec.WithMethod(request.Target.Route.Method)))
	}
	preparingInput := request.Replay != nil && request.Replay.Only
	if !preparingInput && r.observability != nil && r.observability.adapter != nil {
		var tracked bool
		ctx, tracked = r.observability.begin(ctx)
		if tracked {
			defer r.observability.active.Done()
		}
	}
	stack, _ := ctx.Value(componentStackKey{}).([]dexec.ComponentTarget)
	if len(stack) == 0 && !preparingInput {
		defer func() {
			end := time.Now()
			failure := err
			panicked := recover()
			if panicked != nil {
				failure = dexec.NewPanicError("invocation", panicked)
			}
			ec := xexec.GetContext(ctx)
			ec.Complete(end, failure)
			if r.observability != nil && r.observability.adapter != nil {
				r.observability.adapter.TrySubmit(otel.Completion{Context: ec, End: end, Failed: failure != nil, Links: otel.Links(ctx)})
			}
			if panicked != nil {
				panic(panicked)
			}
		}()
	}

	if request.Target.Component.Kind != spec.KindComponent || strings.TrimSpace(request.Target.Component.Name) == "" {
		return nil, fmt.Errorf("exact component target requires a component key")
	}
	route, err := spec.ParseRouteRef(request.Target.Route.String())
	if err != nil {
		return nil, err
	}
	identity := request.Target.Component.String()
	registered, err := r.registeredComponent(ctx, request.Target.Component)
	if err != nil {
		return nil, err
	}
	if registered == nil || registered.Component == nil {
		return nil, fmt.Errorf("registered component not found: %s", identity)
	}
	if !componentOwnsRoute(registered.Component, route) {
		return nil, fmt.Errorf("component %s does not own route %s", identity, route.String())
	}
	if registered.Input == nil {
		return nil, fmt.Errorf("registered input contract not found: %s", identity)
	}
	inputRoute, ok := registered.Input.ForRoute(route)
	if !ok {
		return nil, fmt.Errorf("registered route input contract not found: %s", route.String())
	}
	target := dexec.ComponentTarget{Component: request.Target.Component, Route: route}
	ctx, err = enterComponent(ctx, target)
	if err != nil {
		return nil, err
	}
	effectiveScope, err := handlerengine.ComposeScope(scope, request.Providers...)
	if err != nil {
		return nil, err
	}
	if registered.Component.Settings != nil && registered.Component.Settings.IgnoreEmptyQueryParameters != nil {
		ctx = requestprovider.WithQueryPolicy(ctx, requestprovider.QueryPolicy{IgnoreEmptyParameters: *registered.Component.Settings.IgnoreEmptyQueryParameters})
	}
	if request.Replay != nil && request.Replay.Only && (request.Warmup != nil || request.PrepareQuery || request.DryRun) {
		return nil, fmt.Errorf("input-only replay cannot also request SQL execution controls")
	}
	if request.Replay != nil && request.Input != nil {
		return nil, fmt.Errorf("replay cannot bypass canonical binding with component Input")
	}
	if request.ReaderOptions != nil && (registered.Handler != nil || registered.Reader == nil || request.Warmup != nil || request.DryRun || request.PrepareQuery || request.Replay != nil && request.Replay.Only) {
		return nil, fmt.Errorf("result read options require a canonical reader invocation")
	}
	handler := registered.Handler
	dataSource := registered.DataSource
	if request.Replay != nil && request.Replay.Only {
		handler = rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) {
			return nil, fmt.Errorf("input-only preparation reached handler execution")
		})
		dataSource = nil
	} else if request.Warmup != nil {
		if request.PrepareQuery || request.DryRun || handler != nil || registered.Reader == nil {
			return nil, fmt.Errorf("cache warmup requires a registered reader component: %s", identity)
		}
		handler, err = rreader.NewWarmupHandler(registered.Reader, inputRoute, *request.Warmup)
		if err != nil {
			return nil, err
		}
		dataSource = nil
	} else if request.DryRun {
		if request.PrepareQuery || handler != nil || registered.Reader == nil {
			return nil, fmt.Errorf("dry-run requires a registered reader component: %s", identity)
		}
		handler, err = rreader.NewDryRunHandler(registered.Reader, inputRoute)
		if err != nil {
			return nil, err
		}
		dataSource = nil
	} else if request.PrepareQuery {
		if handler != nil || registered.Reader == nil {
			return nil, fmt.Errorf("query preparation requires a registered reader component: %s", identity)
		}
		handler, err = rreader.NewQueryHandler(registered.Reader, inputRoute)
		if err != nil {
			return nil, err
		}
		dataSource = nil
	} else if handler == nil {
		if registered.Reader == nil {
			return nil, fmt.Errorf("registered reader not found: %s", identity)
		}
		readerHandler := rreader.NewHandler(registered.Reader, inputRoute)
		if request.ReaderOptions != nil {
			readerHandler = readerHandler.WithReadOptions(*request.ReaderOptions)
		}
		handler = readerHandler
		dataSource = nil
	}
	if typed, ok := handler.(rhandler.TypedHandler); ok {
		if typed.InputType() != registered.Input.Type() || typed.OutputType() != registered.OutputType {
			return nil, fmt.Errorf(
				"registered handler contract mismatch: handler %s -> %s, component %s -> %s",
				typed.InputType(), typed.OutputType(), registered.Input.Type(), registered.OutputType,
			)
		}
	}
	sequenceStrategy := ""
	if registered.Component.Settings != nil {
		sequenceStrategy = registered.Component.Settings.SequenceStrategy
	}
	return r.invoker.Execute(ctx, handlerengine.Request{
		SequenceStrategy: sequenceStrategy,
		Injector:         r.injector,
		Input:            inputRoute,
		OutputType:       registered.OutputType,
		BoundInput:       request.Input,
		Replay:           request.Replay,
		BindingOutput:    request.BindingOutput,
		Injectors:        (&componentInjectors{runtime: r, scope: effectiveScope}).Lookup,
		Scope:            effectiveScope,
		Capabilities:     registered.Capabilities,
		Providers:        registered.Providers,
		Constants:        r.canonicalConstants[identity],
		Components:       r.componentProvider(effectiveScope, registered.Component),
		ComponentInvoker: r.componentInvokerProvider(effectiveScope),
		DataSource:       dataSource,
		Handler:          handler,
		Completion:       request.Completion,
	})
}

func componentBindingOrder(parent *spec.Component, source string) string {
	if parent == nil {
		return ""
	}
	for index, parameter := range parent.Parameters {
		if parameter != nil && parameter.Source.Kind == string(spec.KindComponent) && parameter.Source.Name == source {
			return fmt.Sprintf("%08d", index)
		}
	}
	return ""
}

func componentOwnsRoute(component *spec.Component, route spec.RouteRef) bool {
	if component == nil {
		return false
	}
	want := route.String()
	for _, candidate := range component.Routes {
		if candidate != nil && (spec.RouteRef{Method: candidate.Method, Path: candidate.Path}).String() == want {
			return true
		}
	}
	return false
}

func enterComponent(ctx context.Context, target dexec.ComponentTarget) (context.Context, error) {
	stack, _ := ctx.Value(componentStackKey{}).([]dexec.ComponentTarget)
	for _, active := range stack {
		if active == target {
			path := append(append([]dexec.ComponentTarget(nil), stack...), target)
			identities := make([]string, len(path))
			for index := range path {
				identities[index] = path[index].String()
			}
			return nil, fmt.Errorf("component dependency cycle: %s", strings.Join(identities, " -> "))
		}
	}
	path := append(append([]dexec.ComponentTarget(nil), stack...), target)
	return context.WithValue(ctx, componentStackKey{}, path), nil
}
