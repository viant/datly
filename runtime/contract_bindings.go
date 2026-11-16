package runtime

import (
	"context"
	"errors"
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
	path    string
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
	preparedInputs  map[*registry.RouteInputContract]*preparedInputExecutionPlan
	unprovable      bool
	frames          map[dexec.ComponentTarget]*contractFrameEvidence
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
	return &contractBindings{owner: owner, values: map[contractBindingKey]contractBindingValue{}, selections: map[*bindly.ReplayPlan]*bindly.Replay{}, frames: map[dexec.ComponentTarget]*contractFrameEvidence{}}
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
	// Captured selections may retain older frames after the target advances.
	// Release every resolution's raw sources and values at operation close.
	for _, head := range b.frames {
		for frame := head; frame != nil; {
			previous := frame.previous
			frame.previous, frame.selections, frame.values = nil, nil, nil
			frame.complete = false
			frame = previous
		}
	}
	b.values, b.selections = nil, nil
	b.preparedInputs = nil
	b.frames = nil
}
func (b *contractBindings) disableSelectionReuse(frame *contractFrameEvidence) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.unprovable = true
	if frame != nil && !b.closed {
		frame.unprovable = true
	}
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
		var binding *bindly.BindingError
		if errors.As(err, &binding) {
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
		plans, err := r.contractPlansForContext(ctx, entry, route.String())
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

type contractFrameEvidence struct {
	previous             *contractFrameEvidence
	target               dexec.ComponentTarget
	selections           map[*bindly.ReplayPlan]*bindly.Replay
	values               map[contractBindingKey]contractBindingValue
	unprovable, complete bool
}

type contractStateBinder struct {
	frame *contractFrameEvidence
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
	stored, _ := r.contractPlans.LoadOrStore(contractPlanKey{component: entry.Component.Key, route: ref.String()}, &contractPlanEntry{})
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
		b.values.disableSelectionReuse(b.frame)
		return b.Binder.Bind(ctx, target, append(options, bindly.WithPlan(plan))...)
	}
	// Group ownership remains with the native group binder; a metadata handoff
	// must not import values across one of its reservation boundaries.
	for _, binding := range bindings {
		if binding.ResolutionGroup != nil {
			b.values.disableSelectionReuse(b.frame)
			return b.Binder.Bind(ctx, target, bindly.WithPlan(plan))
		}
	}
	var before *bindly.Replay
	if selection == nil {
		b.values.disableSelectionReuse(b.frame)
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
			b.values.disableSelectionReuse(b.frame)
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
				b.values.disableSelectionReuse(b.frame)
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
			b.values.disableSelectionReuse(b.frame)
		}
	}
	seed, paths, _, err := b.values.resolved(ctx, plan, bindings, b.scope, false, nil)
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
			if b.frame != nil {
				if b.frame.selections == nil {
					b.frame.selections = map[*bindly.ReplayPlan]*bindly.Replay{}
				}
				b.frame.selections[selection] = after
			}
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
			saved := contractBindingValue{path: item.binding.Path, value: value.Interface(), proof: item.proof, sources: sources}
			b.values.values[key] = saved
			if b.frame != nil {
				if b.frame.values == nil {
					b.frame.values = map[contractBindingKey]contractBindingValue{}
				}
				b.frame.values[key] = saved
			}
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
func (b *contractBindings) resolved(ctx context.Context, plan *bindly.Plan, bindings []bindly.BindingSpec, scope *contractSourceScope, retainReceipt bool, selected *contractSelection) (any, []string, *resolvedBindingReceipt, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	var seed any
	var paths []string
	var receipt *resolvedBindingReceipt
	for _, binding := range bindings {
		if !reusableContractBinding(binding) {
			continue
		}
		access, err := xshape.Linked(plan.TargetType()).Accessor(binding.Path)
		if err != nil {
			return nil, nil, nil, err
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
		key := contractBindingKey{route: binding.Location.In, typ: typ}
		saved, exists := b.values[key]
		// A captured selection retains the actual values which governed its
		// metadata. A later resolution's map occupant cannot replace that evidence.
		if selected != nil && selected.self == selected && selected.bindings == b {
			for current := selected; current != nil; current = current.source {
				if value, found := current.frame.values[key]; found {
					saved, exists = value, true
					break
				}
			}
		}
		closed := b.closed
		b.mu.Unlock()
		if closed || !exists {
			continue
		}
		sources, complete, err := saved.proof.snapshot(ctx, scope)
		if err != nil {
			return nil, nil, nil, err
		}
		if !complete || !sources.SourcesEqual(saved.sources) {
			continue
		}
		if err = access.Set(seed, saved.value); err != nil {
			return nil, nil, nil, err
		}
		paths = append(paths, binding.Path)
		if retainReceipt {
			if receipt == nil {
				receipt = &resolvedBindingReceipt{plan: plan}
			}
			receipt.entries = append(receipt.entries, resolvedBindingEntry{path: binding.Path, access: access, saved: saved})
		}
	}
	if receipt != nil {
		receipt.seed = seed
		receipt.paths = append([]string(nil), paths...)
	}
	return seed, paths, receipt, nil
}

// These receipts retain only values actually set by resolved, after its native
// source comparison. Later occupants of the operation map cannot change them.
// Pointer identity is provenance; producer-owned result objects remain immutable.
type resolvedBindingEntry struct {
	path   string
	access *xshape.Accessor
	saved  contractBindingValue
}
type resolvedBindingReceipt struct {
	plan    *bindly.Plan
	seed    any
	paths   []string
	entries []resolvedBindingEntry
}
type frameSelectionEvidence struct {
	plan    *bindly.ReplayPlan
	proof   *contractBindingProof
	path    string
	sources *bindly.Replay
}

// contractSelection is the private identity of one completed resolution. The
// existing opaque BindingState carries this receipt through EffectiveContract's
// value copies; the context continues to carry its operation separately.
type contractSelection struct {
	self                                      *contractSelection
	bindings                                  *contractBindings
	frame                                     *contractFrameEvidence
	prepared                                  *preparedInputExecutionPlan
	source                                    *contractSelection
	sourceContract                            *dexec.EffectiveContract
	input, output                             reflect.Type
	inputRepresentation, outputRepresentation *xshape.Representation
}

func contractSelectionFor(effective *dexec.EffectiveContract) *contractSelection {
	if effective == nil {
		return nil
	}
	selected, _ := effective.BindingState.(*contractSelection)
	if selected == nil || selected.self != selected || !effective.Dynamic || selected.input != effective.Input || selected.output != effective.Output || selected.inputRepresentation != effective.InputRepresentation || selected.outputRepresentation != effective.OutputRepresentation || selected.sourceContract != effective.SourceContract {
		return nil
	}
	return selected
}
func contractBindingsFor(effective *dexec.EffectiveContract) *contractBindings {
	selected := contractSelectionFor(effective)
	if selected == nil {
		return nil
	}
	return selected.bindings
}
func (b *contractBindings) frameEvidence(target dexec.ComponentTarget, effective *dexec.EffectiveContract) ([]frameSelectionEvidence, bool) {
	if b == nil {
		return nil, false
	}
	selected := contractSelectionFor(effective)
	if selected == nil || selected.bindings != b {
		return nil, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || selected.frame == nil || selected.frame.target != target {
		return nil, false
	}
	var result []frameSelectionEvidence
	// Derived contracts retain the exact source selection which governed their
	// projection. Follow that declared chain, never the operation's target map.
	for current := selected; current != nil; current = current.source {
		frame := current.frame
		if current.self != current || current.bindings != b || frame == nil || !frame.complete || frame.unprovable {
			return nil, false
		}
		if current.source != nil && contractSelectionFor(current.sourceContract) != current.source {
			return nil, false
		}
		for plan, replay := range frame.selections {
			result = append(result, frameSelectionEvidence{plan: plan, sources: replay})
		}
		for _, value := range frame.values {
			result = append(result, frameSelectionEvidence{proof: value.proof, path: value.path, sources: value.sources})
		}
	}
	return result, true
}
func matchFrameSources(ctx context.Context, scope *contractSourceScope, evidence []frameSelectionEvidence) (bool, error) {
	valid, _, err := matchFrameSourcesWithReceipts(ctx, scope, evidence, map[*contractBindingProof]*bindly.Replay{})
	return valid, err
}
func matchFrameSourcesWithReceipts(ctx context.Context, scope *contractSourceScope, evidence []frameSelectionEvidence, checked map[*contractBindingProof]*bindly.Replay) (bool, string, error) {
	if err := ctx.Err(); err != nil {
		return false, "", err
	}
	for _, item := range evidence {
		if item.proof != nil {
			sources := checked[item.proof]
			if sources == nil {
				var complete bool
				var err error
				sources, complete, err = item.proof.snapshot(ctx, scope)
				if err != nil {
					return false, "", err
				}
				if !complete {
					return false, item.path, nil
				}
				checked[item.proof] = sources
			}
			if !sources.SourcesEqual(item.sources) {
				return false, item.path, nil
			}
			continue
		}
		injector, err := scope.input()
		if err != nil {
			return false, "", err
		}
		sources, err := item.plan.CaptureSourcesIn(ctx, injector)
		if err != nil {
			return false, "", err
		}
		if !sources.SourcesEqual(item.sources) {
			return false, item.path, nil
		}
	}
	return true, "", nil
}
func (b *contractBindings) frameMatches(ctx context.Context, target dexec.ComponentTarget, effective *dexec.EffectiveContract, scope *contractSourceScope) (bool, error) {
	evidence, valid := b.frameEvidence(target, effective)
	if !valid {
		return false, nil
	}
	return matchFrameSources(ctx, scope, evidence)
}

// preparationHandoff belongs to one synchronous dispatch, never to a metadata
// variant or returned PreparedQuery. It holds the actual dispatch tuple rather
// than looking up a possibly fresh lazy registration wrapper.
type preparationFrameKey struct{}
type preparationFrame struct{ target dexec.ComponentTarget }

type preparationHandoff struct {
	selected         *contractSelection
	frame            *preparationFrame
	bindings         *contractBindings
	owner            *Runtime
	scope            *contractSourceScope
	canonical, route *registry.RouteInputContract
	prepared         *preparedInputExecutionPlan
	representation   *xshape.Representation
	receipt          *resolvedBindingReceipt
	selection        []frameSelectionEvidence
}

func (h *preparationHandoff) MatchesPreparation(scope any, seed any, paths []string) bool {
	if h == nil || scope != h.scope.protocol {
		return false
	}
	if h.receipt == nil {
		return seed == nil && len(paths) == 0
	}
	if seed != h.receipt.seed || h.receipt.plan != h.route.Plan() || len(paths) != len(h.receipt.paths) {
		return false
	}
	for i, path := range paths {
		if path != h.receipt.paths[i] {
			return false
		}
	}
	return true
}
func (h *preparationHandoff) ValidatePreparation(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ctx.Value(contractBindingsKey{}) != h.bindings || ctx.Value(preparationFrameKey{}) != h.frame {
		return fmt.Errorf("query preparation requires its exact operation frame")
	}
	stack, _ := ctx.Value(componentStackKey{}).([]dexec.ComponentTarget)
	if h.frame == nil || len(stack) == 0 || stack[len(stack)-1] != h.frame.target {
		return fmt.Errorf("query preparation requires its exact dispatch target")
	}
	h.bindings.mu.Lock()
	active := !h.bindings.closed && h.bindings.claimed && h.bindings.owner == h.owner && h.selected != nil && h.selected.self == h.selected && h.selected.bindings == h.bindings && h.selected.frame != nil && h.selected.frame.target == h.frame.target && h.selected.frame.complete && !h.selected.frame.unprovable
	h.bindings.mu.Unlock()
	if !active {
		return &bindly.BindingError{Code: 409, Cause: fmt.Errorf("preparation operation is no longer active")}
	}
	if h.route != h.canonical && (h.prepared == nil || h.prepared.original != h.canonical || h.prepared.route != h.route || h.prepared.representation != h.representation || h.selected.prepared != h.prepared) {
		return fmt.Errorf("query preparation requires exact selected input plan")
	}
	checked := map[*contractBindingProof]*bindly.Replay{}
	valid, path, err := matchFrameSourcesWithReceipts(ctx, h.scope, h.selection, checked)
	if err != nil {
		return err
	}
	if !valid {
		return &bindly.BindingError{Path: path, Code: 409, Cause: fmt.Errorf("input sources changed during query preparation binding")}
	}
	if h.receipt != nil {
		for _, entry := range h.receipt.entries {
			actual, found, err := entry.access.GetOptional(h.receipt.seed)
			if err != nil {
				return err
			}
			if !found || !actual.IsValid() || actual.Interface() != entry.saved.value {
				return &bindly.BindingError{Path: entry.path, Code: 409, Cause: fmt.Errorf("resolved preparation input changed")}
			}
			sources := checked[entry.saved.proof]
			complete := sources != nil
			if !complete {
				var err error
				sources, complete, err = entry.saved.proof.snapshot(ctx, h.scope)
				if err != nil {
					return err
				}
				if complete {
					checked[entry.saved.proof] = sources
				}
			}
			if !complete || !sources.SourcesEqual(entry.saved.sources) {
				return &bindly.BindingError{Path: entry.path, Code: 409, Cause: fmt.Errorf("component input sources changed during query preparation binding")}
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	h.bindings.mu.Lock()
	active = !h.bindings.closed && h.bindings.claimed
	h.bindings.mu.Unlock()
	if !active {
		return &bindly.BindingError{Code: 409, Cause: fmt.Errorf("preparation operation is no longer active")}
	}
	return nil
}

// preparedInputPlan returns the exact object selected for this resolution,
// including an older captured NoCache selection. Latest-target state cannot
// substitute a different resolution's prepared artifact.
func (s *contractSelection) preparedInputPlan(original *registry.RouteInputContract, metadata *xshape.Representation) *preparedInputExecutionPlan {
	if s == nil || s.self != s {
		return nil
	}
	prepared := s.prepared
	if prepared == nil || prepared.original != original || prepared.representation != metadata {
		return nil
	}
	return prepared
}

// Each Contract resolution owns its current selection evidence, including a
// resolution whose hooks bind no state. Prior conditional bindings cannot leak
// into a later frame. The existing operation still owns dependency values.
func (r *Runtime) newContractStateBinder(binder bindly.Binder, scope dexec.ProviderScope, values *contractBindings, target dexec.ComponentTarget) *contractStateBinder {
	frame := &contractFrameEvidence{target: target}
	values.mu.Lock()
	if !values.closed {
		frame.previous = values.frames[target]
		values.frames[target] = frame
	}
	values.mu.Unlock()
	return &contractStateBinder{Binder: binder, runtime: r, scope: &contractSourceScope{protocol: scope}, values: values, frame: frame}
}

// Only successful, unsuperseded metadata resolution may publish selection
// evidence. A failed hook cannot make an earlier Contract look freshly selected.
func (b *contractStateBinder) complete(ctx context.Context, effective *dexec.EffectiveContract, inputExecution any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.values.mu.Lock()
	defer b.values.mu.Unlock()
	if b.values.closed || b.values.frames[b.frame.target] != b.frame {
		return &bindly.BindingError{Code: 409, Cause: fmt.Errorf("contract selection frame changed during resolution")}
	}
	b.frame.complete = true
	selected := &contractSelection{bindings: b.values, frame: b.frame, input: effective.Input, output: effective.Output, inputRepresentation: effective.InputRepresentation, outputRepresentation: effective.OutputRepresentation}
	selected.prepared, _ = inputExecution.(*preparedInputExecutionPlan)
	selected.sourceContract = effective.SourceContract
	selected.source = contractSelectionFor(effective.SourceContract)
	selected.self = selected
	effective.BindingState = selected
	return nil
}
