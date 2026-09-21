// Package engine owns the common invocation path shared by runtime handler
// flavors: one Bindly scope, one input plan, and one lifecycle.
package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

type Request struct {
	// SequenceStrategy is canonical component metadata, not a client-bound value.
	SequenceStrategy string
	OutputType       reflect.Type
	Injector         *bindly.Injector
	Input            *registry.RouteInputContract
	BoundInput       any
	Replay           *bindly.ReplayBinding
	BindingOutput    any
	Injectors        func(context.Context, any, xhandler.Route) (xhandler.Binder, error)
	Scope            dexec.ProviderScope
	Capabilities     rhandler.InvocationCapabilities
	Providers        []locator.Provider
	// Constants is runtime-derived canonical constant authority. It remains
	// separate from caller-controlled providers so authority cannot be forged.
	Constants locator.Provider
	// Components is runtime-owned route authority and cannot be shadowed by
	// component-registered or protocol-scoped providers.
	Components locator.Provider
	// ComponentInvoker is runtime-owned exact component invocation authority.
	ComponentInvoker locator.Provider
	DataSource       dexec.DataSource
	Handler          rhandler.Handler
	Completion       func(xhandler.Outcome)
}

type Engine struct{}

func New() *Engine { return &Engine{} }

func (e *Engine) Execute(ctx context.Context, request Request) (actual any, failure error) {
	if e == nil {
		return nil, fmt.Errorf("handler engine is required")
	}
	if request.Handler == nil {
		return nil, fmt.Errorf("invocation handler is required")
	}
	if request.Input == nil {
		return nil, fmt.Errorf("route input contract is required")
	}
	ctx, completeSelection := dexec.ScopeOutputSelection(ctx)
	inputType := request.Input.Type()
	if inputType == nil || inputType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("route input contract type must be a struct, got %v", inputType)
	}
	inputPlan := request.Input.Plan()
	if inputPlan == nil {
		return nil, fmt.Errorf("route input contract %s requires a Bindly plan", request.Input.Route().String())
	}
	root := request.Injector
	if root == nil {
		var err error
		root, err = bindly.NewInjector()
		if err != nil {
			return nil, err
		}
	}
	if request.Replay != nil && request.BoundInput != nil {
		return nil, fmt.Errorf("replay requires canonical binding, not BoundInput")
	}
	input, bound, err := invocationInput(inputType, request.BoundInput)
	if err != nil {
		return nil, err
	}
	var reads *inputReadMetadata
	var readAccess xhandler.ReadMetadata
	if consumer, ok := request.Handler.(xhandler.ReadMetadataConsumer); ok && consumer.RequiresReadMetadata() {
		reads = &inputReadMetadata{target: input, projections: map[string]xhandler.ReadProjection{}}
		readAccess = reads
	}
	// Every component shadows inherited evidence, including ordinary handlers.
	ctx = xhandler.WithReadMetadata(ctx, readAccess)
	runtimeProviders := handlerprovider.Capabilities(request.Capabilities)
	protocolProviders := []locator.Provider(nil)
	if request.Scope != nil {
		protocolProviders = request.Scope.Providers()
	}
	data, ownsData := invocationDataScope(ctx, request.DataSource)
	if data != nil {
		data.sequenceStrategy = request.SequenceStrategy
		data.connectors = request.Capabilities.Connector
	} else if request.SequenceStrategy != "" {
		return nil, fmt.Errorf("sequence_strategy requires a data source")
	}
	runtimeProviders = append(runtimeProviders, data.frameworkValidatorProvider())
	outcomeFinalizer, outcomeAware := request.Handler.(rhandler.OutcomeFinalizer)
	// Opted-in outputs need a neutral root even when their handler has no DB.
	// Ordinary source-less handlers retain their existing child ownership.
	if data == nil && (outcomeAware || request.Completion != nil || request.hasInjectorFinalizer()) {
		data, ownsData = neutralDataScope(), true
	}
	if data == nil && request.Capabilities.Connector != nil {
		data, ownsData = neutralDataScope(), true
	}
	if data != nil {
		if data.connectors == nil {
			data.connectors = request.Capabilities.Connector
		}
		if data.source != nil || data.parent != nil || data.connectors != nil {
			runtimeProviders = append(runtimeProviders, data.providers()...)
		}
		ctx = withDataScope(ctx, data)
	}
	if ownsData {
		defer data.releaseContexts()
	}
	invocation := rhandler.Invocation{Input: input, Response: newResponseWriter(ctx)}
	var completionFrame *outcomeFrame
	if outcomeAware {
		completionFrame, err = data.registerOutcome(ctx, request.Input.Route().String(), outcomeFinalizer)
		if err != nil {
			return finishDataScope(ctx, data, ownsData, nil, err)
		}
	}
	finishing := false
	finish := func(result any, operationErr error) (any, error) {
		finishing = true
		if data != nil {
			data.finishOutcome(completionFrame, invocation, result, operationErr)
		}
		result, completionErr := finishDataScope(ctx, data, ownsData, result, operationErr)
		if ownsData && data != nil {
			completionErr = data.finalizeOutcomes(completionErr)
		}
		if ownsData && data != nil && request.Completion != nil {
			request.Completion(data.completionOutcome())
		}
		return result, completionErr
	}
	// User callbacks may panic after opening scoped Data. Complete through the
	// canonical owner; never bypass rollback and outcome reporting in an adapter.
	defer func() {
		if value := recover(); value != nil {
			failure = dexec.NewPanicError("handler invocation", value)
			actual = nil
			if !finishing {
				actual, failure = finish(nil, failure)
			}
		}
	}()
	if request.Components != nil {
		runtimeProviders = append(runtimeProviders, request.Components)
	}
	if request.ComponentInvoker != nil {
		runtimeProviders = append(runtimeProviders, request.ComponentInvoker)
	}
	runtimeProviders = append(runtimeProviders, handlerprovider.CallerOutput(request.BindingOutput))
	runtimeProviders = append(runtimeProviders, handlerprovider.Parameter())
	runtimeProviders = append(runtimeProviders, handlerprovider.Input())
	runtimeProviders = append(runtimeProviders, handlerprovider.New(xhandler.ReadMetadataKey, func(context.Context) (any, bool, error) {
		if reads == nil {
			return nil, false, fmt.Errorf("input read metadata was not requested")
		}
		reads.mu.RLock()
		ready := reads.ready
		reads.mu.RUnlock()
		if !ready {
			return nil, false, fmt.Errorf("input read metadata is unavailable during binding")
		}
		return reads, true, nil
	}))
	var snapshot any
	var snapshotReady bool
	runtimeProviders = append(runtimeProviders, handlerprovider.New(xhandler.InputSnapshotKey, func(context.Context) (any, bool, error) {
		if !snapshotReady {
			return nil, false, fmt.Errorf("input snapshot is unavailable during input binding")
		}
		return snapshot, snapshot != nil, nil
	}))
	var scope *bindly.Injector
	runtimeProviders = append(runtimeProviders, handlerprovider.Static(dexec.ReaderInputPreparerKey, dexec.ReaderInputPreparer(func(ctx context.Context, names ...string) (*dexec.ReaderInput, error) {
		prepared, err := request.Input.Without(input, names...)
		if err != nil {
			return nil, err
		}
		return &dexec.ReaderInput{Input: prepared, Binder: rhandler.NewBinder(scope, prepared), Parameters: sqlx.ParameterResolver(request.Input.Resolver(prepared))}, nil
	})))
	providers, err := (providerComposer{}).compose(providerComposition{
		component: request.Providers, protocol: protocolProviders,
		runtime: runtimeProviders, constants: request.Constants,
	})
	if err != nil {
		return finish(nil, err)
	}
	scope, err = root.ForScope(providers...)
	if err != nil {
		return finish(nil, err)
	}
	if transaction, ok := request.Handler.(rhandler.PreBindingTransaction); ok && transaction.RequiresPreBindingTransaction() && data != nil {
		resolved, resolveErr := data.resolve(ctx)
		if resolveErr != nil {
			return finish(nil, fmt.Errorf("resolve pre-binding transaction: %w", resolveErr))
		}
		starter, ok := resolved.(xhandler.TransactionStarter)
		if !ok {
			return finish(nil, fmt.Errorf("pre-binding transaction starter is unavailable"))
		}
		if err := starter.Start(ctx); err != nil {
			return finish(nil, fmt.Errorf("start pre-binding transaction: %w", err))
		}
	}
	bindPlan := inputPlan
	if bound {
		bindPlan, err = boundSupplementalPlan(root, request.Input)
		if err != nil {
			return finish(nil, err)
		}
	}
	if !bound || bindPlan != nil {
		options := []bindly.BindOption{bindly.WithPlan(bindPlan), bindly.WithSource(input)}
		if request.Replay != nil {
			options = append(options, bindly.WithReplay(*request.Replay))
		}
		if reads != nil {
			options = append(options, bindly.WithBindingObserver(reads.observe))
		}
		if err := scope.Bind(ctx, input, options...); err != nil {
			return finish(nil, err)
		}
	}
	if request.Replay != nil && request.Replay.Only {
		return finish(input, nil)
	}
	if reads != nil {
		reads.seal()
	}
	if capturer, ok := request.Handler.(rhandler.InputCapturer); ok {
		snapshot, err = capturer.CaptureInput(ctx, input)
		invocation.Snapshot = snapshot
		if err != nil {
			return finish(nil, fmt.Errorf("capture handler input: %w", err))
		}
	}
	snapshotReady = true
	binder := rhandler.NewBinder(scope, input)
	invocation.Binder = binder
	if initializer, ok := input.(xhandler.Initializer); ok {
		if err := initializer.Init(ctx); err != nil {
			return finish(nil, fmt.Errorf("initialize handler input: %w", err))
		}
	}
	if mcpContext, ok := xmcp.LookupContext(ctx); ok {
		if initializer, ok := input.(xmcp.Initializer); ok {
			if err := initializer.InitMCP(ctx, mcpContext); err != nil {
				return finish(nil, fmt.Errorf("initialize MCP handler input: %w", err))
			}
		}
	}
	dexec.BeginOutputSelection(ctx)
	result, handlerErr := request.Handler.Execute(ctx, invocation)
	completeSelection(result, handlerErr)
	if outcomeAware {
		return finish(result, handlerErr)
	}
	var injectorPrepareErr error
	if finalizer, ok := result.(xhandler.InjectorFinalizer); ok {
		// Untyped engine adapters can discover the hook only from the result.
		// Its conditional child calls still need an owner before lookup starts.
		if data == nil {
			data, ownsData = neutralDataScope(), true
			ctx = withDataScope(ctx, data)
			defer data.releaseContexts()
		}
		if handlerErr == nil && ownsData {
			injectorPrepareErr = data.prepare(ctx)
			handlerErr = injectorPrepareErr
		}
		lease := &finalizerInjectors{data: data, ctx: ctx, output: result, lookup: request.Injectors}
		handlerErr = lease.finalize(finalizer, handlerErr)
	}
	// A transactional prepare can fail before commit. Let the single
	// error-aware finalizer observe that failure while it can still veto the
	// locally owned transaction. Plain success finalizers remain post-commit.
	var prepareErr error
	if _, errorAware := result.(xhandler.ErrorFinalizer); errorAware && handlerErr == nil && ownsData && data != nil {
		prepareErr = data.prepare(ctx)
		if prepareErr != nil {
			handlerErr = prepareErr
		}
	}
	result, handlerErr = finalizeBeforeCompletion(ctx, result, handlerErr)
	if data != nil && !ownsData && hasCompletionHooks(ctx, result) {
		var registerErr error
		completionFrame, registerErr = data.registerOutcome(ctx, request.Input.Route().String(), outputCompletion{})
		if registerErr != nil {
			handlerErr = errors.Join(handlerErr, registerErr)
		}
	}
	result, handlerErr = finish(result, handlerErr)
	if prepareErr != nil || injectorPrepareErr != nil {
		result = nil
	}
	if handlerErr != nil {
		return result, handlerErr
	}
	if data != nil && !ownsData {
		return result, nil
	}
	return finalizeAfterCompletion(ctx, result)
}

func invocationInput(inputType reflect.Type, supplied any) (any, bool, error) {
	if supplied == nil {
		return reflect.New(inputType).Interface(), false, nil
	}
	value := reflect.ValueOf(supplied)
	if value.Kind() != reflect.Ptr || value.IsNil() || value.Type().Elem() != inputType {
		return nil, false, fmt.Errorf("bound component input must be a non-nil *%s, got %T", inputType, supplied)
	}
	return supplied, true, nil
}

func boundSupplementalPlan(injector *bindly.Injector, input *registry.RouteInputContract) (*bindly.Plan, error) {
	var bindings []bindly.BindingSpec
	for _, field := range input.Fields() {
		binding := field.Binding()
		if (spec.BindSource{Kind: binding.Location.Kind}).RequestValue() {
			continue
		}
		bindings = append(bindings, binding)
	}
	if len(bindings) == 0 {
		return nil, nil
	}
	plan, err := injector.CompilePlan(input.Type(), bindings...)
	if err != nil {
		return nil, fmt.Errorf("compile trusted input supplemental bindings: %w", err)
	}
	return plan, nil
}

func (r Request) hasInjectorFinalizer() bool {
	outputType := r.OutputType
	if outputType == nil {
		if typed, ok := r.Handler.(rhandler.TypedHandler); ok {
			outputType = typed.OutputType()
		}
	}
	if outputType == nil {
		return false
	}
	contract := reflect.TypeFor[xhandler.InjectorFinalizer]()
	return outputType.Implements(contract) || (outputType.Kind() != reflect.Pointer && reflect.PointerTo(outputType).Implements(contract))
}
