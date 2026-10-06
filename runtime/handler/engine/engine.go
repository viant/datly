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
	"github.com/viant/datly/internal/drainowner"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx"
	xbind "github.com/viant/xdatly/bind"
	xhandler "github.com/viant/xdatly/handler"
	handlerexec "github.com/viant/xdatly/handler/exec"
	xmcp "github.com/viant/xdatly/handler/mcp"
	xlogger "github.com/viant/xdatly/logger"
)

type Request struct {
	mutationAttempt   int
	phaseInvocationID uint64
	// BufferedComponentCalls is dispatcher-owned canonical caller metadata.
	BufferedComponentCalls bool
	// IndependentChildTransactions retains all invocation capabilities/context but
	// prevents connector-only neutral ownership after strict source-less guards.
	IndependentChildTransactions bool
	// SequenceStrategy is canonical component metadata, not a client-bound value.
	SequenceStrategy   string
	OutputType         reflect.Type
	OutputCapabilities *bindly.Plan
	Injector           *bindly.Injector
	Input              *registry.RouteInputContract
	BoundInput         any
	// ResolvedInput/ResolvedPaths are dispatcher-owned reader seeds, not a
	// replacement for BoundInput or transport replay.
	ResolvedInput any
	ResolvedPaths []string
	Replay        *bindly.ReplayBinding
	BindingOutput any
	Injectors     func(context.Context, any, xhandler.Route) (xhandler.Binder, error)
	Scope         dexec.ProviderScope
	Capabilities  rhandler.InvocationCapabilities
	Providers     []locator.Provider
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
	callContext := ctx
	ctx, _ = rhandler.WithPhaseObserver(ctx, nil, 0, 0)
	var phases *rhandler.PhaseScope
	phaseEnded := false
	endPhases := func(cause error) {
		if !phaseEnded && phases != nil {
			phaseEnded = true
			phases.Notify(ctx, xhandler.PhaseInvocation, xhandler.PhaseEnd, cause)
		}
	}
	defer func() { endPhases(failure) }()
	if e == nil {
		return nil, fmt.Errorf("handler engine is required")
	}
	if request.BufferedComponentCalls && request.IndependentChildTransactions {
		return nil, fmt.Errorf("buffered component calls cannot use independent child transactions")
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
	if request.ResolvedInput != nil && (request.BoundInput != nil || request.Replay != nil) {
		return nil, fmt.Errorf("resolved reader input cannot combine with BoundInput or replay")
	}
	input, bound, err := invocationInput(inputType, request.BoundInput)
	if err != nil {
		return nil, err
	}
	var reads *inputReadMetadata
	var readAccess xhandler.ReadMetadata
	if consumer, ok := request.Handler.(xhandler.ReadMetadataConsumer); ok && consumer.RequiresReadMetadata() {
		reads = &inputReadMetadata{target: input, declared: request.Input.Fields(), projections: map[string]xhandler.ReadProjection{}}
		readAccess = reads
	}
	// Every component shadows inherited evidence, including ordinary handlers.
	ctx = xhandler.WithReadMetadata(ctx, readAccess)
	var runtimeProviders []locator.Provider
	var protocolProviders []locator.Provider
	if request.IndependentChildTransactions {
		if err := validateIndependentChildren(ctx, request); err != nil {
			return nil, err
		}
	}
	data, ownsData := invocationDataScope(ctx, request.DataSource)
	if data != nil {
		data.sequenceStrategy = request.SequenceStrategy
		data.connectors = request.Capabilities.Connector
	} else if request.SequenceStrategy != "" {
		return nil, fmt.Errorf("sequence_strategy requires a data source")
	}
	outcomeFinalizer, outcomeAware := request.Handler.(rhandler.OutcomeFinalizer)
	// Opted-in outputs need a neutral root even when their handler has no DB.
	// Ordinary source-less handlers retain their existing child ownership.
	if data == nil && (outcomeAware || request.Completion != nil || request.hasInjectorFinalizer() || request.BufferedComponentCalls) {
		data, ownsData = neutralDataScope(), true
	}
	if data == nil && request.Capabilities.Connector != nil && !request.IndependentChildTransactions {
		data, ownsData = neutralDataScope(), true
	}
	var activity drainowner.Activity
	activityActive := false
	finishing := false
	var finish func(any, error) (any, error)
	retireActivity := func(cause error) error {
		if !activityActive {
			return nil
		}
		activityActive = false
		return data.finishActivity(activity, cause)
	}
	// This covers roots established up front and dynamically discovered output roots.
	defer func() {
		if ownsData && data != nil {
			data.releaseContexts()
		}
	}()
	// Admission and recovery precede providers, source resolution and capture.
	defer func() {
		if value := recover(); value != nil {
			failure = dexec.NewPanicError("handler invocation", value)
			actual = nil
			if !finishing && finish != nil {
				actual, failure = finish(nil, failure)
			} else {
				if activityErr := retireActivity(failure); activityErr != nil {
					failure = errors.Join(failure, activityErr)
				}
				if !finishing {
					actual, failure = finishDataScope(ctx, data, ownsData, nil, failure)
				}
			}
		} else if activityActive {
			if activityErr := retireActivity(failure); activityErr != nil {
				failure = errors.Join(failure, activityErr)
			}
		}
	}()
	if data != nil {
		activity, err = data.admitActivity()
		if err != nil && !(errors.Is(err, drainowner.ErrDormantActivityClosed) && !request.BufferedComponentCalls) {
			return nil, err
		}
		activityActive = err == nil
	}
	runtimeProviders = handlerprovider.Capabilities(request.Capabilities)
	if request.Scope != nil {
		protocolProviders = request.Scope.Providers()
	}
	runtimeProviders = append(runtimeProviders, data.frameworkValidatorProvider())
	if data != nil {
		if request.BufferedComponentCalls {
			if err := data.enrollBufferedScope(); err != nil {
				return nil, err
			}
		}
		if data.connectors == nil {
			data.connectors = request.Capabilities.Connector
		}
		if data.source != nil || data.parent != nil || data.connectors != nil {
			runtimeProviders = append(runtimeProviders, data.providers()...)
		}
		ctx = withDataScope(ctx, data)
		ctx = dexec.WithInvocationTransactionLookup(ctx, data.transactionForDatabase)
	}
	invocation := rhandler.Invocation{Input: input, Response: newResponseWriter(ctx)}
	var mutationReplay *bindly.Replay
	var mutationInput any
	var completionFrame *outcomeFrame
	outputFinalized := false
	var snapshotReady, initializationErrorReturned, ownCapturedGuard bool
	var scope *bindly.Injector
	bindOutput := func(value any) (err error) {
		defer func() {
			if value := recover(); value != nil {
				err = dexec.NewPanicError("output capability binding", value)
			}
		}()
		if request.OutputCapabilities == nil || value == nil || scope == nil {
			return nil
		}
		return scope.Bind(ctx, value, bindly.WithPlan(request.OutputCapabilities), bindly.WithSource(input))
	}
	notifyCompletion := func(completionErr error) error {
		if !ownsData || data == nil || request.Completion == nil {
			return completionErr
		}
		if err := completionOperation("completion notification", func() error {
			request.Completion(data.completionOutcome())
			return nil
		}); err != nil {
			return errors.Join(completionErr, err)
		}
		return completionErr
	}

	finish = func(result any, operationErr error) (any, error) {
		finishing = true
		protectedPrepareFailed := false
		protectedCompletionFailed := false
		// Mutation adapters may opt into a declared error output before a
		// program exists. Their outcome callback still owns once-only dispatch
		// after root cleanup; this path never executes the output hook itself.
		if operationErr != nil && outcomeAware && result == nil {
			if invocation.Snapshot == nil {
				if early, ok := request.Handler.(rhandler.EarlyErrorOutputFinalizer); ok {
					enabled := false
					if optInErr := completionOperation("early error output opt-in", func() error {
						enabled = early.EarlyErrorOutputEnabled()
						return nil
					}); optInErr != nil {
						operationErr = errors.Join(operationErr, optInErr)
					} else if enabled {
						result = errorAwareOutput(request.OutputType)
					}
				}
			} else if snapshotReady && initializationErrorReturned {
				var bridgeErr error
				result, bridgeErr = capturedInitializationOutput(ctx, request, invocation, operationErr)
				if bridgeErr != nil {
					operationErr = errors.Join(operationErr, bridgeErr)
				}
			}
			if result != nil {
				if bindErr := bindOutput(result); bindErr != nil {
					operationErr = errors.Join(operationErr, bindErr)
				}
			}
		}

		// Opted-in typed outputs can observe early binding/initialization/read
		// failures. No successful finalizer is run on this path.
		if operationErr != nil && !outputFinalized && !outcomeAware {
			if result == nil {
				result = errorAwareOutput(request.OutputType)
			}
			if result != nil {
				if bindErr := bindOutput(result); bindErr != nil {
					operationErr = errors.Join(operationErr, bindErr)
				}
				if !ownsData || !data.protectedLifetime() {
					outputFinalized = true
					result, operationErr = finalizeBeforeCompletion(ctx, result, operationErr)
				}
			}
		}
		if ownCapturedGuard && operationErr != nil && data != nil {
			_ = data.failGuardedExecution(operationErr)
		}
		if activityErr := retireActivity(operationErr); activityErr != nil {
			operationErr = errors.Join(operationErr, activityErr)
		}
		if data != nil {
			data.finishOutcome(completionFrame, invocation, result, operationErr)
		}
		if ownsData && data.protectedLifetime() {
			if _, barrierErr := data.beginProtectedCompletion(ctx); barrierErr != nil {
				protectedCompletionFailed = operationErr == nil
				operationErr = errors.Join(operationErr, barrierErr)
			}
			if !outcomeAware && !outputFinalized {
				if _, errorAware := result.(xhandler.ErrorFinalizer); errorAware && operationErr == nil {
					if prepareErr := data.prepareProtectedFinalization(ctx); prepareErr != nil {
						operationErr = prepareErr
						protectedPrepareFailed = true
					}
				}
				outputFinalized = true
				result, operationErr = finalizeBeforeCompletion(ctx, result, operationErr)
			}
			data.finishOutcome(completionFrame, invocation, result, operationErr)
		}
		result, completionErr := finishDataScope(ctx, data, ownsData, result, operationErr)
		if ownsData && data != nil && (mutationReplay != nil || mutationInput != nil) {
			decision, recovered, recoveryErr := recoverMutation(callContext, request, data, invocation, completionErr)
			if recoveryErr != nil {
				completionErr = errors.Join(completionErr, recoveryErr)
				data.finishOutcome(completionFrame, invocation, completionFrame.result, completionErr)
			} else if recovered {
				if decision == rhandler.RecoveryRetry {
					// Finalize this attempt with truthful transaction evidence and
					// an explicit retry disposition; no success publication yet.
					data.finishOutcome(completionFrame, invocation, completionFrame.result, errMutationRetry)
					var finalizationError *FinalizationError
					if finalErr := data.finalizeOutcomes(completionErr); errors.As(finalErr, &finalizationError) || completionIntroducedError(finalErr, completionErr) {
						return result, notifyCompletion(finalErr)
					}
					endPhases(completionErr)
					retry := request
					retry.mutationAttempt++
					if mutationInput != nil {
						retry.BoundInput, err = captureMutationInput(request.Input, mutationInput)
						if err != nil {
							return nil, notifyCompletion(err)
						}
						retry.Replay = nil
					} else {
						retry.BoundInput = nil
						retry.Replay = &bindly.ReplayBinding{Replay: mutationReplay}
					}
					if guard, ok := request.Handler.(rhandler.MutationReplayContext); ok {
						callContext = guard.MutationReplayContext(callContext, invocation)
					}
					return e.Execute(callContext, retry)
				}
				result, completionErr = completionFrame.result, nil
			}
		}
		if ownsData && data != nil {
			completionErr = data.finalizeOutcomes(completionErr)
		}
		if protectedPrepareFailed || protectedCompletionFailed {
			result = nil
		}
		return result, notifyCompletion(completionErr)
	}
	if request.BufferedComponentCalls && data != nil && (data.source != nil || data.parent != nil) {
		if _, err := data.resolve(ctx); err != nil {
			return finish(nil, err)
		}
	}
	if outcomeAware {
		completionFrame, err = data.registerOutcome(ctx, request.Input.Route().String(), outcomeFinalizer)
		if err != nil {
			return finish(nil, err)
		}
	}
	if request.Components != nil {
		runtimeProviders = append(runtimeProviders, retainProviderMutationAuthority(ctx, request.Components, false))
	}
	if request.ComponentInvoker != nil {
		runtimeProviders = append(runtimeProviders, retainProviderMutationAuthority(ctx, request.ComponentInvoker, true))
	}
	runtimeProviders = append(runtimeProviders, handlerprovider.CallerOutput(request.BindingOutput))
	runtimeProviders = append(runtimeProviders, handlerprovider.Parameter())
	runtimeProviders = append(runtimeProviders, handlerprovider.Input())
	runtimeProviders = append(runtimeProviders, handlerprovider.New(invocationKind, func(ctx context.Context) (any, bool, error) {
		return handlerexec.InvocationFromContext(ctx), true, nil
	}))
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
	runtimeProviders = append(runtimeProviders, handlerprovider.New(xhandler.InputSnapshotKey, func(context.Context) (any, bool, error) {
		if !snapshotReady {
			return nil, false, fmt.Errorf("input snapshot is unavailable during input binding")
		}
		return snapshot, snapshot != nil, nil
	}))
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
	providers = withDefaultGenerator(root, providers)
	scope, err = root.ForScope(providers...)
	if err != nil {
		return finish(nil, err)
	}
	// Resolve once from the composed trusted scope, preserving the same logger
	// authority as static input/output capabilities. Rows only use typed context.
	invocationLogger, _, loggerErr := xbind.Lookup[xlogger.Logger](ctx, rhandler.NewBinder(scope, input), rhandler.LoggerCapabilityKey)
	if loggerErr != nil {
		return finish(nil, fmt.Errorf("resolve invocation logger: %w", loggerErr))
	}
	ctx = xlogger.WithContext(ctx, invocationLogger)
	if factory, ok := request.Handler.(rhandler.PhaseObserverFactory); ok {
		var observer xhandler.PhaseObserver
		func() {
			defer func() {
				if recover() != nil {
					observer = nil
					// Observation failures must not affect the invocation, including
					// when reporting through an application-provided logger.
					func() {
						defer func() { _ = recover() }()
						if invocationLogger != nil {
							invocationLogger.Error("phase observer factory panicked")
						}
					}()
				}
			}()
			observer = factory.NewPhaseObserver()
		}()
		ctx, phases = rhandler.WithPhaseObserver(ctx, observer, request.phaseInvocationID, request.mutationAttempt)
		request.phaseInvocationID = phases.InvocationID()
		phases.Notify(ctx, xhandler.PhaseInvocation, xhandler.PhaseBegin, nil)
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
	if err := phases.Run(ctx, xhandler.PhaseBinding, func() error {
		bindPlan := inputPlan
		if bound {
			if err := normalizeBoundBodyNulls(request.Input, input); err != nil {
				return err
			}
			bindPlan, err = boundSupplementalPlan(root, request.Input)
			if err != nil {
				return err
			}
		}
		if !bound || bindPlan != nil {
			options := []bindly.BindOption{bindly.WithPlan(bindPlan), bindly.WithSource(input)}
			if request.Replay != nil {
				options = append(options, bindly.WithReplay(*request.Replay))
			}
			if request.ResolvedInput != nil {
				options = append(options, bindly.WithResolvedInput(inputPlan, request.ResolvedInput, request.ResolvedPaths...))
			}
			if reads != nil {
				options = append(options, bindly.WithBindingObserver(reads.observe))
			}
			if err := scope.Bind(ctx, input, options...); err != nil {
				return err
			}
		}
		if bound {
			if err := validateRequiredBoundInputs(request.Input, input); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return finish(nil, err)
	}
	ctx = context.WithValue(ctx, reflect.TypeOf(input), input)
	if request.Replay != nil && request.Replay.Only {
		return finish(input, nil)
	}
	if reads != nil {
		reads.seal()
	}
	if recoverer, ok := request.Handler.(rhandler.MutationRecoverer); ok && recoverer.SupportsMutationRecovery() && ownsData {
		plan, captureErr := request.Input.ReplayPlan()
		if captureErr != nil {
			return finish(nil, fmt.Errorf("mutation recovery request replay: %w", captureErr))
		}
		if bound {
			mutationInput, captureErr = captureMutationInput(request.Input, input)
		} else if request.Replay != nil {
			mutationReplay = request.Replay.Replay
		} else {
			mutationReplay, captureErr = plan.CaptureSources(ctx, providers)
		}
		if captureErr != nil {
			return finish(nil, fmt.Errorf("capture mutation recovery request: %w", captureErr))
		}
	}
	if capturer, ok := request.Handler.(rhandler.InputCapturer); ok {
		snapshot, err = capturer.CaptureInput(ctx, input)
		invocation.Snapshot = snapshot
		if err != nil {
			return finish(nil, fmt.Errorf("capture handler input: %w", err))
		}
	}
	binder := rhandler.NewBinder(scope, input)
	invocation.Binder = binder
	if guarded, ok := request.Handler.(rhandler.CapturedExecutionGuard); ok {
		check, guardErr := guarded.CapturedExecutionGuard(invocation)
		if guardErr != nil {
			return finish(nil, guardErr)
		}
		if check != nil {
			ownCapturedGuard = true
			if data == nil {
				return finish(nil, fmt.Errorf("captured writer execution requires an owned data unit"))
			}
			if _, resolveErr := data.resolve(ctx); resolveErr != nil {
				return finish(nil, resolveErr)
			}
			if guardErr = data.registerExecutionGuard(ctx, check); guardErr != nil {
				return finish(nil, guardErr)
			}
			if guardErr = guarded.CapturedExecutionGuardRegistered(invocation); guardErr != nil {
				return finish(nil, guardErr)
			}
		}
	}
	snapshotReady = true
	if err := phases.Run(ctx, xhandler.PhaseInputInitialization, func() error {
		if initializer, ok := input.(xhandler.Initializer); ok {
			if err := initializer.Init(ctx); err != nil {
				initializationErrorReturned = true
				return fmt.Errorf("initialize handler input: %w", err)
			}
		}
		if mcpContext, ok := xmcp.LookupContext(ctx); ok {
			if initializer, ok := input.(xmcp.Initializer); ok {
				if err := initializer.InitMCP(ctx, mcpContext); err != nil {
					initializationErrorReturned = true
					return fmt.Errorf("initialize MCP handler input: %w", err)
				}
			}
		}
		return nil
	}); err != nil {
		return finish(nil, err)
	}

	dexec.BeginOutputSelection(ctx)
	result, handlerErr := request.Handler.Execute(ctx, invocation)
	if result == nil && handlerErr != nil && !outcomeAware {
		result = errorAwareOutput(request.OutputType)
	}
	if result != nil {
		if outputErr := bindOutput(result); outputErr != nil {
			handlerErr = errors.Join(handlerErr, outputErr)
		}
	}
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
			activity, err = data.admitActivity()
			if err != nil {
				return finish(result, err)
			}
			activityActive = true
			ctx = withDataScope(ctx, data)
		}
		if handlerErr == nil && ownsData && !data.protectedLifetime() {
			injectorPrepareErr = data.prepare(ctx)
			handlerErr = injectorPrepareErr
		}
		lease := &finalizerInjectors{data: data, ctx: ctx, output: result, lookup: request.Injectors}
		handlerErr = lease.finalize(finalizer, handlerErr)
	}
	// A transactional prepare can fail before commit. Let the single
	// error-aware finalizer observe that failure while it can still veto the
	// locally owned transaction. Plain success finalizers remain post-commit.
	// Protected roots finish output composition before closure/preparation and
	// terminal observation. Nested components retain their existing callbacks.
	protected := ownsData && data.protectedLifetime()
	var prepareErr error
	if _, errorAware := result.(xhandler.ErrorFinalizer); errorAware && !protected && handlerErr == nil && ownsData && data != nil {
		prepareErr = data.prepare(ctx)
		if prepareErr != nil {
			handlerErr = prepareErr
		}
	}
	if !protected {
		outputFinalized = true
		result, handlerErr = finalizeBeforeCompletion(ctx, result, handlerErr)
	}
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

// BoundInput skips request-provider binding. Required request fields still
// need the compiled presence marker generated by transcribe; otherwise a
// missing value could silently suppress its authorization predicate.
func validateRequiredBoundInputs(contract *registry.RouteInputContract, input any) error {
	if contract == nil || contract.Plan() == nil {
		return fmt.Errorf("bound input contract is required")
	}
	for _, field := range contract.Fields() {
		binding := field.Binding()
		if binding.Required == nil || !*binding.Required || !(spec.BindSource{Kind: binding.Location.Kind}).RequestValue() {
			continue
		}
		present, err := contract.Plan().Presence(input, field.Path())
		if err != nil {
			return err
		}
		if !present {
			return &bindly.BindingError{Path: field.Path(), Code: binding.ErrorCode, Message: binding.ErrorMessage,
				Cause: fmt.Errorf("required bound %s value %q is absent", binding.Location.Kind, binding.Location.In)}
		}
	}
	return nil
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

func errorAwareOutput(outputType reflect.Type) any {
	if outputType == nil || outputType.Kind() != reflect.Struct || !reflect.PointerTo(outputType).Implements(reflect.TypeFor[xhandler.ErrorFinalizer]()) {
		return nil
	}
	return reflect.New(outputType).Interface()
}
