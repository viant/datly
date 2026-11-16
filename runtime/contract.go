package runtime

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	dexec "github.com/viant/datly/exec"
	rcontract "github.com/viant/datly/runtime/contract"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/spec"

	xcontract "github.com/viant/xdatly/contract"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

type contractPlanKey struct {
	// Lazy lookup prepares a fresh registration copy. The component key is
	// stable within the Runtime's pinned generation and across those copies.
	component spec.Key
	route     string
}
type contractPlanEntry struct {
	once          sync.Once
	input, output *rcontract.Plan
	err           error
	proofMu       sync.Mutex
	proofReady    bool
	proof         *contractBindingProof
	source        dexec.ComponentTarget
	project       func(*dexec.EffectiveContract) (*dexec.ContractProjection, error)
	sourceMu      sync.Mutex
	sourceReady   bool
	sourceErr     error
}

// ResolveContract runs linked contract hooks using this generation's native
// dependency scope. It does not invoke the target component's business handler.
// Metadata visibility authorization remains with the calling publication owner.
func (r *Runtime) ResolveContract(ctx context.Context, target dexec.ComponentTarget, scope dexec.ProviderScope) (*dexec.EffectiveContract, error) {
	return r.resolveContractMode(ctx, target, scope, true, false)
}

// ResolveDiscoveryContract uses the same dependency scope and provider evaluation
// as invocation, adding only finite output metadata for publication.
func (r *Runtime) ResolveDiscoveryContract(ctx context.Context, target dexec.ComponentTarget, scope dexec.ProviderScope) (*dexec.EffectiveContract, error) {
	return r.resolveContractMode(ctx, target, scope, true, true)
}

func (r *Runtime) resolveContract(ctx context.Context, target dexec.ComponentTarget, scope dexec.ProviderScope, enter bool) (*dexec.EffectiveContract, error) {
	return r.resolveContractMode(ctx, target, scope, enter, false)
}

func (r *Runtime) resolveContractMode(ctx context.Context, target dexec.ComponentTarget, scope dexec.ProviderScope, enter, discovery bool) (*dexec.EffectiveContract, error) {
	result, _, err := r.resolveContractSelection(ctx, target, scope, enter, discovery)
	return result, err
}

type contractMetadataIdentity struct{ input, output rcontract.MetadataIdentity }

func (r *Runtime) resolveContractSelection(ctx context.Context, target dexec.ComponentTarget, scope dexec.ProviderScope, enter, discovery bool) (*dexec.EffectiveContract, contractMetadataIdentity, error) {
	identity := contractMetadataIdentity{input: rcontract.MetadataIdentity{Cacheable: true}, output: rcontract.MetadataIdentity{Cacheable: true}}

	if ctx == nil {
		return nil, identity, fmt.Errorf("contract context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, identity, err
	}
	if r == nil || r.injector == nil {
		return nil, identity, fmt.Errorf("contract runtime is unavailable")
	}
	entry, err := r.registeredComponent(ctx, target.Component)
	if err != nil {
		return nil, identity, err
	}
	if entry == nil || entry.Input == nil {
		return nil, identity, fmt.Errorf("component contract is unavailable")
	}
	if _, ok := entry.Input.ForRoute(target.Route); !ok {
		return nil, identity, fmt.Errorf("contract route is not registered: %s", target.Route.String())
	}
	plans, err := r.contractPlansForContext(ctx, entry, target.Route.String())
	if err != nil {
		return nil, identity, err
	}
	// Enter the native component stack so a policy dependency cannot recursively
	// invoke its own business component under a metadata request.
	if enter {
		ctx, err = enterComponent(ctx, target)
		if err != nil {
			return nil, identity, err
		}
	}
	result := &dexec.EffectiveContract{Input: entry.Input.Type(), Output: entry.OutputType}
	result.Dynamic = plans.input.Dynamic() || plans.output != nil && plans.output.Dynamic()
	var projected *dexec.ContractProjection
	var source *dexec.EffectiveContract
	if _, isMCP := xmcp.LookupContext(ctx); isMCP && plans.project != nil {
		var sourceIdentity contractMetadataIdentity
		source, sourceIdentity, err = r.resolveContractSelection(ctx, plans.source, scope, true, discovery)
		if err != nil {
			return nil, identity, err
		}
		if state := contractBindingsFor(source); state != nil && state.owner == r {
			ctx = context.WithValue(ctx, contractBindingsKey{}, state)
		}
		projected, err = plans.project(source)
		if err != nil {
			return nil, identity, err
		}
		if projected != nil {
			projected.Policy.Name = fmt.Sprintf("source=%s:input=%s:output=%s", plans.source.String(), sourceIdentity.input.Name, sourceIdentity.output.Name)
			projected.Policy.NoCache = !sourceIdentity.input.Cacheable || !sourceIdentity.output.Cacheable
		}
		result.Dynamic = true
		identity.output = sourceIdentity.output
	}
	if !result.Dynamic {
		return result, identity, nil
	}
	if entry.ContractState == nil {
		return nil, identity, fmt.Errorf("dynamic contract requires native state compilation for %s", target.Component.String())
	}
	constants, err := r.constantProvider(entry.Component)
	if err != nil {
		return nil, identity, err
	}
	providers, err := engine.ContractScope(r.injector, entry.Providers, scope, constants, r.componentProvider(scope, entry.Component, entry), target)
	if err != nil {
		return nil, identity, err
	}
	bindings, _ := ctx.Value(contractBindingsKey{}).(*contractBindings)
	if bindings == nil || bindings.owner != r {
		bindings = newContractBindings(r)
		ctx = context.WithValue(ctx, contractBindingsKey{}, bindings)
	}
	stateBinder := r.newContractStateBinder(providers, scope, bindings, target)
	binder := entry.ContractState.Binder(stateBinder)
	var inputExecution any
	result.InputRepresentation, inputExecution, identity.input, err = plans.input.ResolvePreparedInputSelection(ctx, binder, projectionPolicy(projected))
	if err != nil {
		return nil, identity, err
	}
	if projected != nil {
		source.Projection, err = identity.input.PrepareProjection(ctx, func() (any, error) { return projected.Prepare(result.InputRepresentation) })
		if err != nil {
			return nil, identity, err
		}
		result.SourceContract = source
		if normalization, ok := source.Projection.(dexec.ContractInputNormalization); ok {
			result.InputNormalization = normalization.ContractInputNormalization()
		}
	}
	if prepared, ok := inputExecution.(*preparedInputExecutionPlan); ok {
		bindings.retainPreparedInput(prepared)
	}
	if source != nil && source.Output == result.Output {
		result.OutputRepresentation = source.OutputRepresentation
		result.OutputAlternatives = source.OutputAlternatives
		result.OutputAlternativeSchemas = source.OutputAlternativeSchemas
	} else if plans.output != nil {
		if discovery {
			result.OutputRepresentation, result.OutputAlternatives, result.OutputAlternativeSchemas, identity.output, err = plans.output.ResolveDiscoverySelection(ctx, binder)
		} else {
			result.OutputRepresentation, identity.output, err = plans.output.ResolveRepresentationSelection(ctx, binder)
		}
		if err != nil {
			return nil, identity, err
		}
	}
	if err := stateBinder.complete(ctx, result, inputExecution); err != nil {
		return nil, identity, err
	}
	return result, identity, nil
}

func (r *Runtime) contractPlansForContext(ctx context.Context, entry *RegisteredComponent, route string) (*contractPlanEntry, error) {
	key := contractPlanKey{component: entry.Component.Key, route: route}
	stored, _ := r.contractPlans.LoadOrStore(key, &contractPlanEntry{})
	plans := stored.(*contractPlanEntry)
	plans.once.Do(func() {
		ref, err := spec.ParseRouteRef(route)
		if err != nil {
			plans.err = err
			return
		}
		input, ok := entry.Input.ForRoute(ref)
		if !ok {
			plans.err = fmt.Errorf("contract route is not registered: %s", route)
			return
		}
		plans.input, plans.err = rcontract.NewInput(entry.Input.Type(), 64, prepareInputExecution(input))
		if plans.err == nil && entry.OutputType != nil {
			plans.output, plans.err = rcontract.NewOutput(entry.OutputType, 64, prepareOutputDiscovery(entry))
		}

	})
	if plans.err != nil {
		return nil, plans.err
	}
	if err := r.prepareDerivedContract(ctx, entry, plans); err != nil {
		return nil, err
	}
	return plans, nil
}

// A canceled or unavailable lazy source lookup must not poison a registration's
// immutable compilation. Only completed provenance compilation is retained.
func (r *Runtime) prepareDerivedContract(ctx context.Context, entry *RegisteredComponent, plans *contractPlanEntry) error {
	derived, ok := entry.Handler.(dexec.DerivedContract)
	if !ok {
		return nil
	}
	plans.sourceMu.Lock()
	defer plans.sourceMu.Unlock()
	if plans.sourceReady {
		return plans.sourceErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	target := derived.SourceContractTarget()
	source, err := r.registeredComponent(ctx, target.Component)
	if err != nil {
		return err
	}
	if source == nil || source.Input == nil {
		return fmt.Errorf("derived source contract is unavailable: %s", target.String())
	}
	if _, ok := source.Input.ForRoute(target.Route); !ok {
		return fmt.Errorf("derived source contract route is unavailable: %s", target.String())
	}
	if _, nested := source.Handler.(dexec.DerivedContract); nested {
		return fmt.Errorf("report contract requires a base source component: %s", target.String())
	}
	sourcePlans, err := r.contractPlansForContext(ctx, source, target.Route.String())
	if err != nil {
		return err
	}
	plans.source = target
	// No-hook sources retain their existing public defaults.
	if sourcePlans.input.Dynamic() || sourcePlans.output != nil && sourcePlans.output.Dynamic() || sourceEnumMetadata(source.Component) {
		plans.project, plans.sourceErr = derived.PrepareSourceContract(source.Component, source.Input.Type(), source.OutputType)
		if plans.sourceErr == nil {
			plans.sourceErr = plans.input.PrepareProjection()
		}
	}
	plans.sourceReady = true
	return plans.sourceErr
}

// contractPlansFor retains the registration-only compilation entry point.
func (r *Runtime) contractPlansFor(entry *RegisteredComponent, route string) (*contractPlanEntry, error) {
	return r.contractPlansForContext(context.Background(), entry, route)
}

func projectionPolicy(projection *dexec.ContractProjection) *xcontract.Policy {
	if projection == nil {
		return nil
	}
	return projection.Policy
}

// Already-declared native enum metadata needs projection into a report body
// even when no caller hook is necessary. This is compilation metadata only.
func sourceEnumMetadata(component *spec.Component) bool {
	for _, parameter := range component.Parameters {
		if parameter == nil {
			continue
		}
		tag := reflect.StructTag(parameter.Tag)
		if tag.Get("mcpEnum") == "true" || tag.Get("enumAccept") != "" {
			return true
		}
	}
	return false
}
