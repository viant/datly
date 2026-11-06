package runtime

import (
	"context"
	"fmt"
	"sync"

	dexec "github.com/viant/datly/exec"
	rcontract "github.com/viant/datly/runtime/contract"
	"github.com/viant/datly/runtime/handler/engine"
)

type contractPlanKey struct {
	component *RegisteredComponent
	route     string
}
type contractPlanEntry struct {
	once          sync.Once
	input, output *rcontract.Plan
	err           error
}

// ResolveContract runs linked contract hooks using this generation's native
// dependency scope. It does not invoke the target component's business handler.
// Metadata visibility authorization remains with the calling publication owner.
func (r *Runtime) ResolveContract(ctx context.Context, target dexec.ComponentTarget, scope dexec.ProviderScope) (*dexec.EffectiveContract, error) {
	return r.resolveContract(ctx, target, scope, true)
}

func (r *Runtime) resolveContract(ctx context.Context, target dexec.ComponentTarget, scope dexec.ProviderScope, enter bool) (*dexec.EffectiveContract, error) {
	if ctx == nil {
		return nil, fmt.Errorf("contract context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.injector == nil {
		return nil, fmt.Errorf("contract runtime is unavailable")
	}
	entry, err := r.registeredComponent(ctx, target.Component)
	if err != nil {
		return nil, err
	}
	if entry == nil || entry.Input == nil {
		return nil, fmt.Errorf("component contract is unavailable")
	}
	if _, ok := entry.Input.ForRoute(target.Route); !ok {
		return nil, fmt.Errorf("contract route is not registered: %s", target.Route.String())
	}
	key := contractPlanKey{component: entry, route: target.Route.String()}
	stored, _ := r.contractPlans.LoadOrStore(key, &contractPlanEntry{})
	plans := stored.(*contractPlanEntry)
	plans.once.Do(func() {
		plans.input, plans.err = rcontract.New(entry.Input.Type(), 64)
		if plans.err == nil && entry.OutputType != nil {
			plans.output, plans.err = rcontract.New(entry.OutputType, 64)
		}
	})
	if plans.err != nil {
		return nil, plans.err
	}
	result := &dexec.EffectiveContract{Input: entry.Input.Type(), Output: entry.OutputType}
	result.Dynamic = plans.input.Dynamic() || plans.output != nil && plans.output.Dynamic()
	if !result.Dynamic {
		return result, nil
	}
	if entry.ContractState == nil {
		return nil, fmt.Errorf("dynamic contract requires native state compilation for %s", target.Component.String())
	}
	constants, err := r.constantProvider(entry.Component)
	if err != nil {
		return nil, err
	}
	// Enter the native component stack so a policy dependency cannot recursively
	// invoke its own business component under a metadata request.
	if enter {
		ctx, err = enterComponent(ctx, target)
		if err != nil {
			return nil, err
		}
	}
	providers, err := engine.ContractScope(r.injector, entry.Providers, scope, constants, r.componentProvider(scope, entry.Component, entry))
	if err != nil {
		return nil, err
	}
	binder := entry.ContractState.Binder(providers)
	result.InputRepresentation, err = plans.input.ResolveRepresentation(ctx, binder)
	if err != nil {
		return nil, err
	}
	if plans.output != nil {
		result.OutputRepresentation, err = plans.output.ResolveRepresentation(ctx, binder)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
