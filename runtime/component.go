package runtime

import (
	"context"
	"errors"
	"fmt"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/responsebinding"
	"github.com/viant/datly/observability/otel"
	xexec "github.com/viant/xdatly/exec"
	xhandler "github.com/viant/xdatly/handler"
	legacyresponse "github.com/viant/xdatly/handler/response"
	"reflect"
	"strings"
	"time"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	rreader "github.com/viant/datly/runtime/handler/reader"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	handlerexec "github.com/viant/xdatly/handler/exec"
)

type componentStackKey struct{}

func (r *Runtime) componentProvider(scope dexec.ProviderScope, parent *spec.Component, owners ...*RegisteredComponent) locator.Provider {
	scope = componentDependencyScope(scope)
	return drainowner.SealBindingGroupProvider(handlerprovider.Named(string(spec.KindComponent), func(ctx context.Context, typ reflect.Type, name string) (value any, found bool, failure error) {
		ctx = handlerengine.PrepareComponent(ctx, handlerengine.ComponentBinding, componentBindingOrder(parent, name))
		ref, err := spec.ParseRouteRef(name)
		if err != nil {
			return nil, false, err
		}
		var dispatch drainowner.BindingGroupDispatch
		reserved := false
		if drainowner.ScopedBindingGroupMember(ctx) {
			if len(owners) != 1 || owners[0] == nil {
				return nil, false, drainowner.ErrBindingGroup
			}
			ctx, dispatch, err = drainowner.PrepareBindingGroupDispatch(ctx, groupRegistration(owners[0]), ref.String(), typ)
			if err != nil {
				return nil, false, err
			}
			reserved = true
			defer func() {
				if panicked := recover(); panicked != nil {
					_ = dispatch.Complete(dexec.NewPanicError("component binding dispatch", panicked))
					panic(panicked)
				}
				if closeErr := dispatch.Complete(failure); closeErr != nil && failure == nil {
					failure = closeErr
					value, found = nil, false
				}
			}()
		}
		component, _, ok := r.bundle.ComponentByRouteWithParams(ref.Method, ref.Path)
		if !ok || component == nil {
			return nil, false, fmt.Errorf("component route not found: %s", ref.String())
		}
		// A reserved descendant carries its own reader ticket here. Actual
		// registration and managed-provider restrictions precede binding.
		if reserved {
			child, loadErr := r.registeredComponent(ctx, component.Key)
			if loadErr != nil {
				return nil, false, loadErr
			}
			if _, err = groupReaderLeaf(child, ref); err != nil {
				return nil, false, err
			}
		}
		result, err := r.invokeComponent(ctx, dexec.ComponentRequest{
			Target: dexec.ComponentTarget{Component: component.Key, Route: ref},
		}, scope)
		return result, err == nil, err
	}))
}

type scopedComponentInvoker struct {
	runtime   *Runtime
	scope     dexec.ProviderScope
	authority func(context.Context) context.Context
	buffered  bool
}

// resolvedComponentCallPolicyKey carries dispatcher policy through binding
// descendants, whose relationship marker alone cannot retain imperative policy.
type resolvedComponentCallPolicyKey struct{}

func (i *scopedComponentInvoker) InvokeComponent(ctx context.Context, request dexec.ComponentRequest) (_ any, failure error) {
	if i == nil || i.runtime == nil {
		return nil, fmt.Errorf("component invoker is not configured")
	}
	if i.authority != nil {
		ctx = i.authority(ctx)
	}
	if handlerengine.ReadOnlyDependency(ctx) && request.IndependentChildTransactions {
		return nil, handlerengine.RejectReadOnlyDependency(ctx, "independent child transactions")
	}
	observeChildFailure, _ := ctx.Value(responsebinding.ChildKey{}).(func())
	defer func() {
		if failure != nil && observeChildFailure != nil {
			observeChildFailure()
		}
	}()
	recoveryRead := handlerengine.IsRootRecoveryRead(ctx)
	policy := "imperative"
	if i.buffered && !recoveryRead {
		policy = "buffered"
	}
	ctx = context.WithValue(ctx, resolvedComponentCallPolicyKey{}, policy)
	relation := handlerengine.ComponentImperative
	if i.buffered && !recoveryRead {
		if request.IndependentChildTransactions {
			return nil, fmt.Errorf("buffered component calls cannot use independent child transactions")
		}
		relation = handlerengine.ComponentBufferedImperative
	}
	if recoveryRead {
		relation = handlerengine.ComponentBinding
	}
	ctx = handlerengine.PrepareComponent(ctx, relation, "")
	value, err := i.runtime.invokeComponent(ctx, request, i.scope)
	if err != nil && request.Input == nil && request.Replay == nil && !request.PrepareQuery && !request.DryRun && request.Warmup == nil && len(request.WarmupOmitInput) == 0 && request.ReaderOptions == nil && request.ReportOrdering == nil {
		// Scoped proxy calls preserve the checked child's source identity. The
		// declarative component provider retains its own binding ownership.
		registered, lookupErr := i.runtime.registeredComponent(ctx, request.Target.Component)
		if lookupErr == nil && registered != nil && registered.Reader != nil && registered.Handler == nil {
			err = registered.Output.PreserveReaderBindingSource(err)
		}
	}
	return value, err
}

// Invoke adapts the public application contract to the existing scoped call.
// Both interfaces share this invoker's authority, providers and buffered owner.
func (i *scopedComponentInvoker) Invoke(ctx context.Context, request xhandler.ComponentRequest) (any, error) {
	var seed *dexec.InputSeed
	if request.ExtraInput != nil {
		seed = dexec.WithInput(request.ExtraInput.Value())
	}
	return i.InvokeComponent(ctx, dexec.ComponentRequest{
		Target: dexec.ComponentTarget{
			Component: spec.Key{Kind: spec.KindComponent, Scope: request.Target.Scope, Name: request.Target.Name},
			Route:     spec.RouteRef{Method: request.Target.Method, Path: request.Target.Path},
		},
		Input: request.Input, ExtraInput: seed,
		Providers: request.Providers, Completion: request.Completion,
	})
}

var _ xhandler.ComponentInvoker = (*scopedComponentInvoker)(nil)
var _ dexec.ComponentInvoker = (*scopedComponentInvoker)(nil)

func (r *Runtime) componentInvokerProvider(scope dexec.ProviderScope, buffered bool) locator.Provider {
	scope = componentDependencyScope(scope)
	return handlerprovider.New(dexec.ComponentInvokerKey, func(ctx context.Context) (any, bool, error) {
		return dexec.ComponentInvoker(&scopedComponentInvoker{
			runtime:   r,
			scope:     scope,
			authority: handlerengine.RetainMutationAuthority(ctx),
			buffered:  buffered,
		}), true, nil
	})
}

type componentScope struct{ providers []locator.Provider }

func (s componentScope) Providers() []locator.Provider {
	return append([]locator.Provider(nil), s.providers...)
}

// Runtime selector capabilities target one exact reader invocation. Headers,
// credential providers and ordinary query inputs remain inherited; deeper
// components bind their own declared selectors. Explicit child Providers can
// install selectors for their own target without leaking to its dependencies.
func componentDependencyScope(scope dexec.ProviderScope) dexec.ProviderScope {
	if scope == nil {
		return nil
	}
	var providers []locator.Provider
	for _, provider := range scope.Providers() {
		if provider != nil && provider.Kind() != string(xhandler.SelectorsKey) {
			providers = append(providers, provider)
		}
	}
	return componentScope{providers: providers}
}

// InvokeComponent executes one exact component target without route lookup.
func (r *Runtime) InvokeComponent(ctx context.Context, request dexec.ComponentRequest) (any, error) {
	return r.invokeComponent(ctx, request, nil)
}

func (r *Runtime) invokeComponent(ctx context.Context, request dexec.ComponentRequest, scope dexec.ProviderScope) (_ any, err error) {
	readOnlyContext := ctx
	defer func() {
		if err != nil {
			handlerengine.RecordReadOnlyFailure(readOnlyContext, err)
		}
	}()
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
	// ComponentTarget already stores a typed route. Do not stringify and parse
	// it again on every invocation; normalize the two fields directly.
	route := request.Target.Route
	route.Method = strings.ToUpper(strings.TrimSpace(route.Method))
	route.Path = strings.TrimSpace(route.Path)
	if route.Method == "" || route.Path == "" || !strings.HasPrefix(route.Path, "/") {
		// Preserve the public validation contract formerly supplied by
		// ParseRouteRef, while keeping valid invocation off the stringify/parse
		// path.
		return nil, fmt.Errorf("component route reference %q must use METHOD:/path", request.Target.Route.String())
	}
	identity := request.Target.Component.String()
	registered, err := r.registeredComponent(ctx, request.Target.Component)
	if err != nil {
		return nil, err
	}
	if registered == nil || registered.Component == nil {
		return nil, fmt.Errorf("registered component not found: %s", identity)
	}
	if handlerengine.ReadOnlyDependency(ctx) {
		if !readOnlyGroupReader(registered, route) || request.IndependentChildTransactions || len(request.Providers) != 0 || request.Completion != nil || request.BindingOutput != nil || request.ExtraInput != nil || request.Replay != nil || request.Warmup != nil || request.PrepareQuery || request.DryRun || len(request.WarmupOmitInput) != 0 {
			return nil, handlerengine.RejectReadOnlyDependency(ctx, "child requires a native reader without provider overrides, independent transactions, or post-commit callbacks")
		}
		ctx, err = handlerengine.AdmitReadOnlyDependency(ctx, route.String(), true)
		if err != nil {
			return nil, err
		}
	}
	if err := registered.Component.Settings.ValidateComponentCallPolicy(); err != nil {
		return nil, err
	}
	if err := registered.Component.Settings.ValidateProtectedFlushTables(); err != nil {
		return nil, err
	}
	inheritedBuffered := handlerengine.IsBufferedComponent(ctx)
	inheritedPolicy, _ := ctx.Value(resolvedComponentCallPolicyKey{}).(string)
	if inheritedBuffered {
		inheritedPolicy = "buffered"
	}
	policy := ""
	if registered.Component.Settings != nil {
		policy = registered.Component.Settings.ComponentCallPolicy
	}
	if inheritedPolicy == "buffered" && policy == "imperative" {
		return nil, fmt.Errorf("imperative component call policy conflicts with inherited buffered calls")
	}
	independent := request.IndependentChildTransactions || registered.Component.Settings != nil && registered.Component.Settings.IndependentChildTransactions
	if policy == "" {
		policy = inheritedPolicy
		if policy == "" {
			policy = "buffered"
			if independent {
				policy = "imperative"
			}
		}
	}
	buffered := policy == "buffered"
	if buffered && independent {
		return nil, fmt.Errorf("buffered component calls cannot use independent child transactions")
	}
	ctx = context.WithValue(ctx, resolvedComponentCallPolicyKey{}, policy)
	if !componentOwnsRoute(registered.Component, route) {
		return nil, fmt.Errorf("component %s does not own route %s", identity, route.String())
	}
	if drainowner.BindingGroupContext(ctx) {
		if request.Input != nil || request.ExtraInput != nil || request.Replay != nil || len(request.Providers) != 0 || request.Warmup != nil || request.PrepareQuery || request.DryRun || request.IndependentChildTransactions {
			return nil, drainowner.ErrBindingGroup
		}
		groupedRoute, ok := registered.Input.ForRoute(route)
		if !ok {
			return nil, drainowner.ErrBindingGroup
		}
		var inputs []drainowner.BindingGroupInput
		for _, field := range groupedRoute.Fields() {
			binding := field.Binding()
			inputs = append(inputs, drainowner.BindingGroupInput{Kind: binding.Location.Kind, In: binding.Location.In, Type: field.DestinationType(), Adapted: binding.Transformer != nil || (binding.SourceType != nil && binding.SourceType != field.DestinationType())})
		}
		if registered.Handler == nil && registered.Reader != nil {
			err = drainowner.ValidateRegisteredBindingGroupReader(ctx, route.String(), groupRegistration(registered), inputs...)
		} else {
			var descriptor drainowner.ScopedGroupTarget
			descriptor, err = r.scopedGroupDescriptor(ctx, registered, route, request, buffered)
			if err == nil {
				err = drainowner.ValidateScopedBindingGroupTarget(ctx, route.String(), groupRegistration(registered), descriptor, inputs...)
			}
		}
		if err != nil {
			return nil, err
		}
	}
	canonicalReader := registered.Handler == nil && registered.Reader != nil && request.Warmup == nil && !request.DryRun && !request.PrepareQuery && (request.Replay == nil || !request.Replay.Only)
	if err := handlerengine.CheckComponentMutation(ctx, canonicalReader); err != nil {
		return nil, err
	}
	readContext, finishRead, readErr := handlerengine.AdmitRootRecoveryRead(ctx)
	if readErr != nil {
		return nil, readErr
	}
	ctx = readContext
	defer func() {
		if finishErr := finishRead(); finishErr != nil {
			err = errors.Join(err, finishErr)
		}
	}()
	if registered.Input == nil {
		return nil, fmt.Errorf("registered input contract not found: %s", identity)
	}
	inputRoute, ok := registered.Input.ForRoute(route)
	if !ok {
		return nil, fmt.Errorf("registered route input contract not found: %s", route.String())
	}

	scoped, policyErr := custom.ScopedInputBinding(registered.Handler)
	if policyErr != nil {
		return nil, policyErr
	}
	if scoped {
		if request.Input != nil || request.Replay != nil || request.Warmup != nil || len(request.WarmupOmitInput) != 0 || request.PrepareQuery || request.DryRun || request.ReaderOptions != nil {
			return nil, fmt.Errorf("scoped input binding does not accept prepared or reader-only invocation")
		}
		if independent {
			return nil, fmt.Errorf("scoped input binding requires an ordinary custom invocation")
		}
		if err := inputRoute.PrepareScopePlans(); err != nil {
			return nil, err
		}
	}
	var resolvedInput any
	var resolvedPaths []string
	if request.ExtraInput != nil {
		if err := validateSeedAdmission(request, registered); err != nil {
			return nil, err
		}
		resolvedInput, resolvedPaths, err = r.prepareReaderSeed(ctx, registered, inputRoute, request.ExtraInput)
		if err != nil {
			return nil, err
		}
	}
	canonicalInputRoute := inputRoute
	if len(request.WarmupOmitInput) > 0 {
		inputRoute, err = inputRoute.WithOptionalBindings(request.WarmupOmitInput...)
		if err != nil {
			return nil, err
		}
	}
	target := dexec.ComponentTarget{Component: request.Target.Component, Route: route}
	ctx, err = dexec.EnterReportOrdering(ctx, target, request.ReportOrdering)
	if err != nil {
		return nil, err
	}
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
	var protectedFlushTables []string
	if registered.Component.Settings != nil {
		sequenceStrategy = registered.Component.Settings.SequenceStrategy
		protectedFlushTables = append([]string(nil), registered.Component.Settings.ProtectedFlushTables...)
	}
	outputPlan, err := r.outputCapabilityPlan(registered)
	if err != nil {
		return nil, err
	}
	constants, err := r.constantProvider(registered.Component)
	if err != nil {
		return nil, err
	}
	values, err := (*constant.Values)(nil).For(registered.Component)
	if err != nil {
		return nil, err
	}
	ctx = constant.WithContext(ctx, values)
	execution := handlerengine.Request{
		IndependentChildTransactions: request.IndependentChildTransactions || (registered.Component.Settings != nil && registered.Component.Settings.IndependentChildTransactions),
		SequenceStrategy:             sequenceStrategy,
		BufferedComponentCalls:       buffered,
		// Keep selector-companion admission while sharing the recovery-read classification.
		CanonicalReader:      canonicalReader && (inputRoute.SelectorPlan() == nil || len(request.WarmupOmitInput) == 0) || canonicalCompanionWarmup(ctx, registered, request, canonicalInputRoute, inputRoute),
		ProtectedFlushTables: protectedFlushTables,
		Injector:             r.injector,
		Input:                inputRoute,
		OutputType:           registered.OutputType,
		OutputCapabilities:   outputPlan,
		BoundInput:           request.Input,
		ResolvedInput:        resolvedInput,
		ResolvedPaths:        resolvedPaths,
		Replay:               request.Replay,
		BindingOutput:        request.BindingOutput,
		Injectors:            (&componentInjectors{runtime: r, scope: effectiveScope}).Lookup,
		Scope:                effectiveScope,
		Capabilities:         registered.Capabilities,
		Providers:            registered.Providers,
		Constants:            constants,
		Components:           r.componentProvider(effectiveScope, registered.Component, registered),
		ComponentInvoker:     r.componentInvokerProvider(effectiveScope, buffered),
		DataSource:           dataSource,
		Handler:              handler,
		Completion:           request.Completion,
	}
	var value any
	err = bindly.RunResolutionGroupPhase(ctx, func(phaseCtx context.Context) error {
		var executeErr error
		value, executeErr = r.invoker.Execute(phaseCtx, execution)
		return executeErr
	})
	return value, err
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

// canonicalCompanionWarmup admits indexed contracts retaining canonical selector authority.
// Child selections require private evidence from the server-owned warmup operation.
func canonicalCompanionWarmup(ctx context.Context, registered *RegisteredComponent, request dexec.ComponentRequest, original, current *registry.RouteInputContract) bool {
	if registered == nil || registered.Handler != nil || registered.Reader == nil || original == nil || current == nil || current.Route() != original.Route() || current.Type() != original.Type() || !current.SelectorPlan().SameAuthority(original.SelectorPlan()) || request.Input != nil || request.ExtraInput != nil || request.Replay != nil || request.DryRun || request.ReaderOptions != nil || request.BindingOutput != nil || len(request.WarmupOmitInput) != 1 {
		return false
	}
	info := handlerexec.InvocationFromContext(ctx)
	if !info.MayBypassRowAuthorization() {
		return false
	}
	switch info.WarmupPhase() {
	case handlerexec.WarmupPhasePrepare:
		if request.Warmup != nil || !request.PrepareQuery {
			return false
		}
	case handlerexec.WarmupPhaseFill:
		if request.Warmup == nil || request.PrepareQuery || request.Warmup.Settings == nil {
			return false
		}
	default:
		return false
	}
	if selected, ok := ctx.Value(warmupSelectionKey{}).(warmupSelection); ok && selected.registered == registered && selected.target == request.Target {
		if selected.indexColumn == "" || selected.indexParameter != request.WarmupOmitInput[0] {
			return false
		}
		if request.Warmup != nil && (request.Warmup.View != selected.view || request.Warmup.Settings.IndexColumn != selected.indexColumn || request.Warmup.Settings.IndexParameter != selected.indexParameter) {
			return false
		}
		return true
	}
	if request.Warmup != nil && request.Warmup.View != "" {
		return false
	}
	targets, err := warmupTargets(registered)
	if err != nil {
		return false
	}
	matched := 0
	for _, target := range targets {
		if target.View != "" {
			return false
		}
		settings := target.Settings
		if settings == nil || settings.IndexColumn == "" || settings.IndexParameter != request.WarmupOmitInput[0] {
			continue
		}
		if request.Warmup != nil && (request.Warmup.Settings.IndexColumn != settings.IndexColumn || request.Warmup.Settings.IndexParameter != settings.IndexParameter) {
			continue
		}
		matched++
	}
	return matched == 1
}

// BindResponse runs the ordinary invocation and copies only an exact typed
// success or a declared response certified by that invocation's private projection token.
func (i *scopedComponentInvoker) BindResponse(ctx context.Context, request xhandler.ComponentRequest, destination any) (_ xhandler.BoundResponse, failure error) {
	var bound xhandler.BoundResponse
	if i == nil || i.runtime == nil {
		return bound, fmt.Errorf("component invoker is not configured")
	}
	if i.authority != nil {
		ctx = i.authority(ctx)
	}
	observeChildFailure, _ := ctx.Value(responsebinding.ChildKey{}).(func())
	defer func() {
		if failure != nil && observeChildFailure != nil {
			observeChildFailure()
		}
	}()
	// A canceled bind must not enter the child lifecycle. In particular, an
	// independent caller has no shared data owner to reject it during admission.
	if err := ctx.Err(); err != nil {
		return bound, err
	}
	key := spec.Key{Kind: spec.KindComponent, Scope: request.Target.Scope, Name: request.Target.Name}
	registered, err := i.runtime.registeredComponent(ctx, key)
	if err != nil {
		return bound, err
	}
	if registered == nil || registered.Component == nil || registered.OutputType == nil {
		return bound, fmt.Errorf("registered output contract not found: %s", key.String())
	}
	route := spec.RouteRef{Method: strings.ToUpper(strings.TrimSpace(request.Target.Method)), Path: strings.TrimSpace(request.Target.Path)}
	if !componentOwnsRoute(registered.Component, route) {
		return bound, fmt.Errorf("component %s does not own route %s", key.String(), route.String())
	}
	dest := reflect.ValueOf(destination)
	expected := registered.OutputType
	if expected.Kind() != reflect.Pointer {
		expected = reflect.PointerTo(expected)
	}
	if !dest.IsValid() || dest.Kind() != reflect.Pointer || dest.IsNil() || dest.Type() != expected {
		return bound, fmt.Errorf("response destination must be a non-nil %s, got %T", expected, destination)
	}
	eligible := registered.Reader != nil && registered.Handler == nil && route.Method == "GET" && request.Input == nil
	var token *responsebinding.Token
	if eligible {
		ctx, token = responsebinding.New(ctx, handlerengine.ResponseBindingOwner(ctx), route.String(), registered.OutputType, func(candidate *legacyresponse.Error) (any, bool, error) {
			projected, ok, err := registered.Output.ErrorEnvelopeWithCause(candidate.Message, candidate.Code, candidate)
			if err != nil || !ok {
				return nil, ok, err
			}
			if reflect.TypeOf(projected) == expected.Elem() {
				pointer := reflect.New(expected.Elem())
				pointer.Elem().Set(reflect.ValueOf(projected))
				projected = pointer.Interface()
			}
			actual := reflect.ValueOf(projected)
			if !actual.IsValid() || actual.Type() != expected || actual.IsNil() {
				return nil, false, fmt.Errorf("declared response must be %s, got %T", expected, projected)
			}
			return projected, true, nil
		})
	}
	value, err := i.Invoke(ctx, request)
	if err != nil {
		return bound, err
	}
	if candidate := token.Consume(); candidate != nil {
		if failure := handlerengine.CheckComponentMutation(ctx, true); failure != nil {
			return bound, failure
		}
		bound = xhandler.BoundResponse{StatusCode: candidate.Code, Cause: candidate}
	}

	actual := reflect.ValueOf(value)
	if !actual.IsValid() || actual.Type() != expected || actual.IsNil() {
		return xhandler.BoundResponse{}, fmt.Errorf("component response must be %s, got %T", expected, value)
	}
	if ctx.Err() != nil {
		return xhandler.BoundResponse{}, ctx.Err()
	}
	dest.Elem().Set(actual.Elem())
	return bound, nil
}

var _ xhandler.ComponentResponseBinder = (*scopedComponentInvoker)(nil)
