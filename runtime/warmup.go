package runtime

import (
	"context"
	"fmt"
	"sort"

	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/provider/values"
	"github.com/viant/datly/bootstrap/cacheconfig"
	dexec "github.com/viant/datly/exec"
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

// Warmup binds each authored root cache case through the canonical component
// engine before invoking the reader's native SQLX warmup operation.
func (r *Runtime) Warmup(ctx context.Context, target dexec.ComponentTarget) (int, error) {
	operation, err := r.NewWarmup(target)
	if err != nil {
		return 0, err
	}
	return operation.Run(ctx)
}

// Warmup is an exact, server-selected operation. Prepare validates the selected
// credentials through canonical binding/query policy before retaining providers.
// It retains no caller context or HTTP request.
type Warmup struct {
	runtime    *Runtime
	target     dexec.ComponentTarget
	registered *registry.RegisteredComponent
	targets    []dexec.ReaderWarmupTarget
	providers  []locator.Provider
}

func (r *Runtime) NewWarmup(target dexec.ComponentTarget) (*Warmup, error) {
	if r == nil {
		return nil, fmt.Errorf("runtime is required")
	}
	registered := r.registered[target.Component.String()]
	if registered == nil || registered.Component == nil || registered.Input == nil || registered.Handler != nil || registered.Reader == nil {
		return nil, fmt.Errorf("registered warmup reader not found: %s", target.String())
	}
	if _, ok := registered.Reader.(dexec.ReaderWarmer); !ok {
		return nil, fmt.Errorf("registered reader does not support cache warmup")
	}
	if !componentOwnsRoute(registered.Component, target.Route) {
		return nil, fmt.Errorf("component does not own warmup route")
	}
	targets := warmupTargets(registered)
	if len(targets) == 0 {
		return nil, fmt.Errorf("component has no authored warmup settings")
	}
	return &Warmup{runtime: r, target: target, registered: registered, targets: targets}, nil
}

// Prepare validates every selected authored case, including verified codecs and
// query predicates. Only successful preparation produces a runnable copy.
func (w *Warmup) Prepare(ctx context.Context, providers ...locator.Provider) (*Warmup, error) {
	prepared := *w
	prepared.providers = append([]locator.Provider(nil), providers...)
	if _, err := prepared.execute(ctx, true); err != nil {
		return nil, err
	}
	return &prepared, nil
}

func (w *Warmup) Run(ctx context.Context) (int, error) { return w.execute(ctx, false) }

// PlannedCases returns the number of server-expanded authored cases that Run
// will execute. It uses the exact input contract and MaxCases policy retained
// by this warmup operation; callers do not supply case values.
func (w *Warmup) PlannedCases() (int, error) {
	registered, target := w.registered, w.target
	contract, ok := registered.Input.ForRoute(target.Route)
	if !ok {
		return 0, fmt.Errorf("registered warmup route not found: %s", target.Route.String())
	}
	required := map[string]bool{}
	for _, field := range contract.Fields() {
		binding := field.Binding()
		name := field.Path()
		if parameter, ok := binding.Extension.(*spec.Parameter); ok && parameter != nil && parameter.Name != "" {
			name = parameter.Name
		}
		required[name] = binding.Required != nil && *binding.Required
	}
	count := 0
	for _, warmupTarget := range w.targets {
		settings := warmupTarget.Settings
		if settings == nil {
			return 0, fmt.Errorf("warmup target %q has no settings", warmupTarget.View)
		}
		entryCost := 1
		if warmupTarget.View == "" && settings.IndexMeta && registered.Component.RootView != nil {
			for _, relation := range registered.Component.RootView.Relations {
				if relation != nil && relation.Kind == spec.RelationKindDerived && len(relation.On) == 0 {
					entryCost++
				}
			}
		}
		if err := (cacheconfig.Cases{Settings: settings, Required: required, EntryCost: entryCost}).ForEach(func(cacheconfig.Case) error {
			count++
			return nil
		}); err != nil {
			return 0, err
		}
	}
	return count, nil
}

func (w *Warmup) execute(ctx context.Context, prepare bool) (int, error) {
	registered, target := w.registered, w.target
	contract, ok := registered.Input.ForRoute(target.Route)
	if !ok {
		return 0, fmt.Errorf("registered warmup route not found: %s", target.Route.String())
	}
	fields := map[string]registry.InputField{}
	required := map[string]bool{}
	for _, field := range contract.Fields() {
		binding := field.Binding()
		name := field.Path()
		if parameter, ok := binding.Extension.(*spec.Parameter); ok && parameter != nil && parameter.Name != "" {
			name = parameter.Name
		}
		fields[name] = field
		required[name] = binding.Required != nil && *binding.Required
	}
	total := 0
	for _, warmupTarget := range w.targets {
		count, err := w.executeTarget(ctx, prepare, fields, required, warmupTarget)
		total += count
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (w *Warmup) executeTarget(ctx context.Context, prepare bool, fields map[string]registry.InputField, required map[string]bool, warmupTarget dexec.ReaderWarmupTarget) (int, error) {
	registered, target := w.registered, w.target
	settings := warmupTarget.Settings
	if settings == nil {
		return 0, fmt.Errorf("warmup target %q has no settings", warmupTarget.View)
	}
	for _, set := range settings.Cases {
		if set == nil {
			continue
		}
		for _, parameter := range set.Set {
			if parameter == nil {
				continue
			}
			if _, ok := fields[parameter.Name]; !ok {
				return 0, fmt.Errorf("unknown warmup parameter %q", parameter.Name)
			}
		}
	}
	total := 0
	entryCost := 1
	if warmupTarget.View == "" && settings.IndexMeta && registered.Component.RootView != nil {
		for _, relation := range registered.Component.RootView.Relations {
			if relation != nil && relation.Kind == spec.RelationKindDerived && len(relation.On) == 0 {
				entryCost++
			}
		}
	}
	err := (cacheconfig.Cases{Settings: settings, Required: required, EntryCost: entryCost}).ForEach(func(item cacheconfig.Case) error {
		byKind := map[string]map[string]any{}
		for name, value := range item.Values {
			field, ok := fields[name]
			if !ok {
				return fmt.Errorf("unknown warmup parameter %q", name)
			}
			location := field.Binding().Location
			if byKind[location.Kind] == nil {
				byKind[location.Kind] = map[string]any{}
			}
			byKind[location.Kind][location.In] = value
		}
		kinds := make([]string, 0, len(byKind))
		for kind := range byKind {
			kinds = append(kinds, kind)
		}
		sort.Strings(kinds)
		providers := make([]locator.Provider, 0, len(kinds))
		for _, kind := range kinds {
			providers = append(providers, values.New(kind, byKind[kind]))
		}
		policy := settings.Clone()
		policy.Cases = nil
		policy.FieldNames = append([]string(nil), item.FieldNames...)
		scope, err := handlerengine.ComposeScope(nil, w.providers...)
		if err != nil {
			return err
		}
		request := dexec.ComponentRequest{Target: target, Providers: providers, Warmup: &dexec.ReaderWarmupRequest{View: warmupTarget.View, Settings: policy}}
		if prepare {
			request.Warmup = nil
			request.PrepareQuery = true
		}
		result, err := w.runtime.invokeComponent(ctx, request, scope)
		if prepare {
			return err
		}
		groups, ok := result.(int)
		if ok {
			total += groups
		}
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("warmup reader returned %T, expected group count", result)
		}
		return nil
	})
	return total, err
}

func warmupTargets(registered *registry.RegisteredComponent) []dexec.ReaderWarmupTarget {
	if registered == nil {
		return nil
	}
	if targeter, ok := registered.Reader.(dexec.ReaderWarmupTargeter); ok {
		targets := targeter.WarmupTargets()
		if len(targets) > 0 {
			return targets
		}
	}
	if settings := registered.Component.CacheWarmup(); settings != nil {
		return []dexec.ReaderWarmupTarget{{Settings: settings.Clone()}}
	}
	return nil
}
