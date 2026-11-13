package runtime

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
)

type contractBindingKey struct {
	route string
	typ   reflect.Type
}
type contractBindingValue struct {
	value   any
	proof   *contractBindingProof
	sources *bindly.Replay
}
type contractBindingProof struct {
	replay *bindly.ReplayPlan
	count  int
	paths  map[string]string
}
type contractBindings struct {
	owner           *Runtime
	mu              sync.Mutex
	claimed, closed bool
	values          map[contractBindingKey]contractBindingValue
	selections      map[*bindly.ReplayPlan]*bindly.Replay
	unprovable      bool
}

// contractSourceScope reuses only the protocol provider scope. Component or
// runtime providers cannot supply missing transport evidence through fallback.
type contractSourceScope struct {
	protocol dexec.ProviderScope
	once     sync.Once
	injector *bindly.Injector
	err      error
}

func (s *contractSourceScope) input() (*bindly.Injector, error) {
	s.once.Do(func() {
		var providers []locator.Provider
		if s.protocol != nil {
			providers = s.protocol.Providers()
		}
		s.injector, s.err = bindly.NewInjector(bindly.WithProviders(providers...))
	})
	return s.injector, s.err
}

type contractBindingsKey struct{}

func newContractBindings(owner *Runtime) *contractBindings {
	return &contractBindings{owner: owner, values: map[contractBindingKey]contractBindingValue{}, selections: map[*bindly.ReplayPlan]*bindly.Replay{}}
}
func (b *contractBindings) hasValues() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.closed && len(b.values) != 0
}
func (b *contractBindings) claim() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.claimed {
		return false
	}
	b.claimed = true
	return true
}
func (b *contractBindings) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.values, b.selections = nil, nil
}
func (b *contractBindings) disableSelectionReuse() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.unprovable = true
}
func reusableContractBinding(binding bindly.BindingSpec) bool {
	return binding.Location.Kind == "component" && binding.Scope == "" && binding.When == "" && binding.With == "" && binding.Transformer == nil && binding.ResolutionGroup == nil && (binding.Cacheable == nil || *binding.Cacheable)
}

// snapshot uses the native raw-source capture plan. Reuse requires every declared
// transitive transport source to be supplied by this protocol scope; component
// defaults or opaque input kinds never gain authority through absent evidence.
func (p *contractBindingProof) snapshot(ctx context.Context, scope *contractSourceScope) (*bindly.Replay, bool, error) {
	var empty *bindly.Replay
	if err := ctx.Err(); err != nil {
		return empty, false, err
	}
	if p == nil || scope == nil || scope.protocol == nil && p.count != 0 {
		return empty, false, nil
	}
	injector, err := scope.input()
	if err != nil {
		return empty, false, err
	}
	replay, err := p.replay.CaptureSourcesIn(ctx, injector)
	if err != nil {
		if binding, ok := err.(*bindly.BindingError); ok {
			if path, found := p.paths[binding.Path]; found {
				original := *binding
				original.Path = path
				err = &original
			}
		}
		return empty, false, err
	}
	if !replay.SourcesPresent() {
		return nil, false, nil
	}
	return replay, true, nil
}

// prepareContractBindingProof traverses only the declared dependency graph.
// InputCatalog remains the owner of transitive transport compatibility checks.
func (r *Runtime) prepareContractBindingProof(ctx context.Context, ref spec.RouteRef) (*contractBindingProof, error) {
	var entries []*RegisteredComponent
	seen := map[string]bool{}
	components := map[spec.Key]bool{}
	var visit func(spec.RouteRef) (bool, error)
	visit = func(route spec.RouteRef) (bool, error) {
		component, _, ok := r.bundle.ComponentByRouteWithParams(route.Method, route.Path)
		if !ok || component == nil {
			return false, nil
		}
		identity := component.Key.String() + "|" + route.String()
		if seen[identity] {
			return true, nil
		}
		seen[identity] = true
		entry, err := r.registeredComponent(ctx, component.Key)
		if err != nil {
			return false, err
		}
		if entry == nil || entry.Input == nil || entry.DataSource != nil {
			return false, nil
		}
		plans, err := r.contractPlansFor(entry, route.String())
		if err != nil {
			return false, err
		}
		// A dependency's own dynamic hook can bind sources absent from its
		// declared input graph; ordinary binding remains authoritative there.
		if plans.input.Dynamic() || plans.output != nil && plans.output.Dynamic() {
			return false, nil
		}
		if entry.Component.Settings != nil && (entry.Component.Settings.Mutation != "" || entry.Component.Settings.IndependentChildTransactions) {
			return false, nil
		}
		input, ok := entry.Input.ForRoute(route)
		if !ok {
			return false, nil
		}
		if !components[component.Key] {
			entries = append(entries, entry)
			components[component.Key] = true
		}
		for _, field := range input.Fields() {
			binding := field.Binding()
			if binding.With != "" || binding.When != "" || binding.ResolutionGroup != nil {
				return false, nil
			}
			switch binding.Location.Kind {
			case "header", "cookie", "query", "path", "body", "form", "const", "literal":
			case "component":
				child, ok := field.Dependency()
				if !ok {
					return false, nil
				}
				valid, err := visit(child)
				if err != nil || !valid {
					return valid, err
				}
			default:
				return false, nil
			}
		}
		return true, nil
	}
	ok, err := visit(ref)
	if err != nil || !ok {
		return nil, err
	}
	catalog, err := registry.NewInputCatalog(entries)
	if err != nil {
		return nil, err
	}
	fields, err := catalog.Fields(ref)
	if err != nil {
		return nil, err
	}
	var shape []reflect.StructField
	var bindings []bindly.BindingSpec
	var paths []string
	originalPaths := map[string]string{}
	for i, field := range fields {
		name := fmtBindingField(i)
		binding := field.Binding()
		originalPaths[name] = binding.Path
		binding.Path = name
		binding.Name = name
		binding.MarkerField = ""
		typ := field.SourceType()
		if typ == nil {
			return nil, nil
		}
		shape = append(shape, reflect.StructField{Name: name, Type: typ})
		binding.Transformer = nil
		binding.SourceType = typ
		bindings = append(bindings, binding)
		paths = append(paths, name)
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		return nil, err
	}
	plan, err := injector.CompilePlan(reflect.StructOf(shape), bindings...)
	if err != nil {
		return nil, err
	}
	replay, err := plan.Replay(paths...)
	if err != nil {
		return nil, err
	}
	return &contractBindingProof{replay: replay, count: len(paths), paths: originalPaths}, nil
}

// These names belong only to the prepared raw-source plan, never a public schema.
func fmtBindingField(i int) string { return "Source" + strconv.Itoa(i) }

type contractStateBinder struct {
	bindly.Binder
	runtime *Runtime
	scope   *contractSourceScope
	values  *contractBindings
}

func (r *Runtime) contractBindingProof(ctx context.Context, ref spec.RouteRef) (*contractBindingProof, error) {
	component, _, ok := r.bundle.ComponentByRouteWithParams(ref.Method, ref.Path)
	if !ok || component == nil {
		return nil, nil
	}
	entry, err := r.registeredComponent(ctx, component.Key)
	if err != nil {
		return nil, err
	}
	stored, _ := r.contractPlans.LoadOrStore(contractPlanKey{component: entry, route: ref.String()}, &contractPlanEntry{})
	plans := stored.(*contractPlanEntry)
	plans.proofMu.Lock()
	defer plans.proofMu.Unlock()
	if plans.proofReady {
		return plans.proof, nil
	}
	proof, err := r.prepareContractBindingProof(ctx, ref)
	if err != nil {
		return nil, err
	}
	plans.proof, plans.proofReady = proof, true
	return proof, nil
}
func (b *contractStateBinder) BindContractState(ctx context.Context, target any, plan *bindly.Plan, bindings []bindly.BindingSpec, selection *bindly.ReplayPlan, options ...bindly.BindOption) error {
	if len(options) != 0 {
		b.values.disableSelectionReuse()
		return b.Binder.Bind(ctx, target, append(options, bindly.WithPlan(plan))...)
	}
	// Group ownership remains with the native group binder; a metadata handoff
	// must not import values across one of its reservation boundaries.
	for _, binding := range bindings {
		if binding.ResolutionGroup != nil {
			b.values.disableSelectionReuse()
			return b.Binder.Bind(ctx, target, bindly.WithPlan(plan))
		}
	}
	var before *bindly.Replay
	if selection == nil {
		b.values.disableSelectionReuse()
	} else {
		injector, err := b.scope.input()
		if err != nil {
			return err
		}
		before, err = selection.CaptureSourcesIn(ctx, injector)
		if err != nil {
			return err
		}
		if !before.SourcesEqual(before) {
			b.values.disableSelectionReuse()
			before = nil
		}
	}
	type pending struct {
		binding bindly.BindingSpec
		proof   *contractBindingProof
		sources *bindly.Replay
	}
	var capture []pending
	for _, binding := range bindings {
		if !reusableContractBinding(binding) {
			if binding.Location.Kind == "component" {
				b.values.disableSelectionReuse()
			}
			continue
		}
		ref, err := spec.ParseRouteRef(binding.Location.In)
		if err != nil {
			return err
		}
		proof, err := b.runtime.contractBindingProof(ctx, ref)
		if err != nil {
			return err
		}
		sources, complete, err := proof.snapshot(ctx, b.scope)
		if err != nil {
			return err
		}
		if complete {
			capture = append(capture, pending{binding, proof, sources})
		} else {
			b.values.disableSelectionReuse()
		}
	}
	seed, paths, err := b.values.resolved(ctx, plan, bindings, b.scope)
	if err != nil {
		return err
	}
	opts := []bindly.BindOption{bindly.WithPlan(plan)}
	if len(paths) > 0 {
		opts = append(opts, bindly.WithResolvedInput(plan, seed, paths...))
	}
	if err = b.Binder.Bind(ctx, target, opts...); err != nil {
		return err
	}
	if before != nil {
		injector, err := b.scope.input()
		if err != nil {
			return err
		}
		after, err := selection.CaptureSourcesIn(ctx, injector)
		if err != nil {
			return err
		}
		if !after.SourcesEqual(before) {
			return &bindly.BindingError{Code: 409, Cause: fmt.Errorf("input sources changed during contract binding")}
		}
		b.values.mu.Lock()
		if !b.values.closed {
			b.values.selections[selection] = after
		}
		b.values.mu.Unlock()
	}
	for _, item := range capture {
		sources, complete, err := item.proof.snapshot(ctx, b.scope)
		if err != nil {
			return err
		}
		if !complete || !sources.SourcesEqual(item.sources) {
			return &bindly.BindingError{Path: item.binding.Path, Code: 409, Cause: fmt.Errorf("component input sources changed during contract binding")}
		}
		access, err := xshape.Linked(plan.TargetType()).Accessor(item.binding.Path)
		if err != nil {
			return err
		}
		value, found, err := access.GetOptional(target)
		if err != nil {
			return err
		}
		if !found || !value.IsValid() {
			continue
		}
		typ := value.Type()
		if typ.Kind() != reflect.Pointer || value.IsNil() {
			continue
		}
		key := contractBindingKey{route: item.binding.Location.In, typ: typ}
		b.values.mu.Lock()
		if !b.values.closed {
			b.values.values[key] = contractBindingValue{value: value.Interface(), proof: item.proof, sources: sources}
		}
		b.values.mu.Unlock()
	}
	return nil
}
func (b *contractBindings) matches(ctx context.Context, scope *contractSourceScope) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	b.mu.Lock()
	if b.closed || b.unprovable {
		b.mu.Unlock()
		return false, nil
	}
	selections := make(map[*bindly.ReplayPlan]*bindly.Replay, len(b.selections))
	for plan, selection := range b.selections {
		selections[plan] = selection
	}
	values := make([]contractBindingValue, 0, len(b.values))
	for _, value := range b.values {
		values = append(values, value)
	}
	b.mu.Unlock()
	for plan, selected := range selections {
		injector, err := scope.input()
		if err != nil {
			return false, err
		}
		// Preserve absent inputs: mode/fields omission is presentation evidence.
		// Replay owns source comparison, including opaque-value rejection.
		sources, err := plan.CaptureSourcesIn(ctx, injector)
		if err != nil {
			return false, err
		}
		if !sources.SourcesEqual(selected) {
			return false, nil
		}
	}
	for _, value := range values {
		sources, complete, err := value.proof.snapshot(ctx, scope)
		if err != nil {
			return false, err
		}
		if !complete || !sources.SourcesEqual(value.sources) {
			return false, nil
		}
	}
	return true, nil
}
func (b *contractBindings) resolved(ctx context.Context, plan *bindly.Plan, bindings []bindly.BindingSpec, scope *contractSourceScope) (any, []string, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	var seed any
	var paths []string
	for _, binding := range bindings {
		if !reusableContractBinding(binding) {
			continue
		}
		access, err := xshape.Linked(plan.TargetType()).Accessor(binding.Path)
		if err != nil {
			return nil, nil, err
		}
		// The canonical accessor supplies the destination type without tag inference.
		if seed == nil {
			seed = reflect.New(plan.TargetType()).Interface()
		}
		typ := access.Type()
		if typ == nil {
			continue
		}
		b.mu.Lock()
		saved, exists := b.values[contractBindingKey{route: binding.Location.In, typ: typ}]
		closed := b.closed
		b.mu.Unlock()
		if closed || !exists {
			continue
		}
		sources, complete, err := saved.proof.snapshot(ctx, scope)
		if err != nil {
			return nil, nil, err
		}
		if !complete || !sources.SourcesEqual(saved.sources) {
			continue
		}
		if err = access.Set(seed, saved.value); err != nil {
			return nil, nil, err
		}
		paths = append(paths, binding.Path)
	}
	return seed, paths, nil
}
