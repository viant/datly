package selectorcompanion

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/internal/selectorcompanion/model"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

// Selection is temporary compiler evidence. Component.Parameters remains the
// only parameter catalog; the entries below are those exact canonical pointers.
type Selection struct {
	Association   *model.Association
	Type          reflect.Type
	Parameters    []*spec.Parameter
	transportOnly bool
}

func Select(component *spec.Component, input reflect.Type) (*Selection, error) {
	if component == nil || component.SelectorCompanion == nil {
		return nil, nil
	}
	a := component.SelectorCompanion
	holder, err := a.Holder()
	if err != nil {
		return nil, err
	}
	descriptor, err := a.Companion()
	if err != nil {
		return nil, err
	}
	if holder.Type == nil || descriptor.Type == nil {
		return nil, fmt.Errorf("selector companion requires linked runtime contracts")
	}
	if holder.Type.Kind() != reflect.Struct {
		return nil, fmt.Errorf("selector companion holder must be a struct")
	}
	field, ok := holder.Type.FieldByName(a.Field())
	if !ok || field.Type.Kind() != reflect.Struct {
		return nil, fmt.Errorf("selector holder component field must be a struct")
	}
	in, ok := field.Type.FieldByName("Inout")
	if !ok {
		in, ok = field.Type.FieldByName("Input")
	}
	if !ok || in.Type != input {
		return nil, fmt.Errorf("selector companion canonical input mismatch")
	}
	out, ok := field.Type.FieldByName("Output")
	if !ok {
		return nil, fmt.Errorf("selector companion canonical output missing")
	}
	contract, err := CheckContracts(component, x.NewType(input), x.NewType(out.Type), nil)
	if err != nil {
		return nil, err
	}
	if contract.Descriptor.Type != descriptor.Type {
		return nil, fmt.Errorf("selector companion association mismatch")
	}
	return selectCanonical(component, contract)
}

// SelectContracts selects only exact canonical pointers after source-capable I/O validation.
func SelectContracts(component *spec.Component, input, output *x.Type, lookup xshape.Lookup) (*Selection, error) {
	if component == nil || component.SelectorCompanion == nil {
		return nil, nil
	}
	contract, err := CheckContracts(component, input, output, lookup)
	if err != nil {
		return nil, err
	}
	return selectCanonical(component, contract)
}

func selectCanonical(component *spec.Component, contract *Contract) (*Selection, error) {
	result := &Selection{Association: component.SelectorCompanion, Type: contract.Descriptor.Type, transportOnly: contract.transportOnly}
	candidates := map[string]*spec.Parameter{}
	for _, p := range spec.EffectiveParameters(component.Parameters) {
		if p == nil || p.EmitOutput || p.Source.Kind == "output" {
			continue
		}
		if _, exists := candidates[p.Identity()]; exists {
			return nil, fmt.Errorf("selector companion ambiguous canonical parameter %s", p.Name)
		}
		candidates[p.Identity()] = p
	}
	for _, f := range contract.Fields {
		m, err := tag.ParseField(f.StructField())
		if err != nil {
			return nil, err
		}
		b := m.Binding
		identity := b.Name
		if identity == "" {
			identity = f.Name
		}
		key := (&spec.Parameter{Name: identity, Source: spec.BindSource{Kind: b.Location.Kind, Name: b.Location.In}}).Identity()
		p := candidates[key]
		if p == nil {
			return nil, fmt.Errorf("selector companion canonical parameter %s missing", identity)
		}
		typ, _ := f.BuiltinType()
		name := b.Name
		if name == "" {
			name = f.Name
		}
		expected := spec.Parameter{Name: name, Source: spec.BindSource{Kind: b.Location.Kind, Name: b.Location.In}, TypeExpr: typ, QueryListCSV: m.QueryListCSV, Required: b.Required, Cacheable: b.Cacheable, Cardinality: b.Cardinality, ErrorStatusCode: b.ErrorCode, ErrorMessage: b.ErrorMessage, QuerySelector: &spec.QuerySelectorBinding{View: m.QuerySelector.View, Property: m.QuerySelector.Property}}
		if b.DefaultValue != nil {
			value := fmt.Sprint(b.DefaultValue)
			expected.Value = &value
		}
		actual := *p
		actual.Declaration = ""
		actual.Raw = ""
		actual.Tag = ""
		actual.Description = ""
		actual.Example = ""
		actualJSON, actualErr := json.Marshal(actual)
		expectedJSON, expectedErr := json.Marshal(expected)
		if actualErr != nil || expectedErr != nil || !bytes.Equal(actualJSON, expectedJSON) {
			return nil, fmt.Errorf("selector companion canonical parameter %s differs from declaration", f.Name)
		}
		result.Parameters = append(result.Parameters, p)
	}
	return result, nil
}

func (s *Selection) Contains(p *spec.Parameter) bool {
	if s == nil {
		return false
	}
	for _, c := range s.Parameters {
		if c == p {
			return true
		}
	}
	return false
}

// BindingPlan contains immutable native plans, never request values.
type BindingPlan struct {
	association               *model.Association
	input, companion          reflect.Type
	publicPlan, companionPlan *bindly.Plan
	public, selectors         []bindly.BindingSpec
	deferredPlan              *bindly.Plan
	deferredBinding           *bindly.BindingSpec
	pageBindings              []bindly.BindingSpec
	pagePlan                  *bindly.Plan
	absentPlan                *bindly.Plan
	owner                     *BindingPlan
	transportOnly             bool
}

func NewBindingPlan(component *spec.Component, selected *Selection, input reflect.Type, publicPlan, companionPlan *bindly.Plan, public, selectors []bindly.BindingSpec) (*BindingPlan, error) {
	if selected == nil {
		return nil, nil
	}
	if selected.Association != component.SelectorCompanion || publicPlan == nil || companionPlan == nil || publicPlan.TargetType() != input || companionPlan.TargetType() != selected.Type || len(selectors) != len(selected.Parameters) {
		return nil, fmt.Errorf("selector companion compiled plan mismatch")
	}
	publicCanonical, selectorCanonical := []*spec.Parameter{}, []*spec.Parameter{}
	for _, parameter := range spec.EffectiveParameters(component.Parameters) {
		if parameter == nil || parameter.EmitOutput || parameter.Source.Kind == "output" {
			continue
		}
		if selected.Contains(parameter) {
			selectorCanonical = append(selectorCanonical, parameter)
		} else {
			publicCanonical = append(publicCanonical, parameter)
		}
	}
	if len(publicCanonical) != len(public) || len(selectorCanonical) != len(selectors) {
		return nil, fmt.Errorf("selector companion requires exact canonical binding coverage")
	}
	locations := map[string]bool{}
	for i, b := range public {
		if b.Extension != publicCanonical[i] {
			return nil, fmt.Errorf("public canonical binding order changed")
		}
		locations[b.Location.Kind+"\x00"+b.Location.In] = true
	}
	for i, b := range selectors {
		if b.Extension != selectorCanonical[i] {
			return nil, fmt.Errorf("selector canonical binding order changed")
		}
		if b.Async || b.ResolutionGroup != nil || b.Scope != "" || b.When != "" || b.With != "" || b.URI != "" || b.ResourceRef != "" {
			return nil, fmt.Errorf("selector companion does not support staged selector bindings")
		}
		key := b.Location.Kind + "\x00" + b.Location.In
		if locations[key] {
			return nil, fmt.Errorf("selector companion overlaps an existing source location")
		}
		locations[key] = true
	}
	for i, b := range selectors {
		if b.Extension != selected.Parameters[i] {
			return nil, fmt.Errorf("selector companion canonical parameter identity mismatch")
		}
	}
	result := &BindingPlan{transportOnly: selected.transportOnly, association: selected.Association, input: input, companion: selected.Type, publicPlan: publicPlan, companionPlan: companionPlan, public: append([]bindly.BindingSpec(nil), public...), selectors: append([]bindly.BindingSpec(nil), selectors...)}
	if result.transportOnly {
		return result, nil
	}
	if component.Settings != nil && component.Settings.IgnoreEmptyQueryParameters != nil && !*component.Settings.IgnoreEmptyQueryParameters {
		if reflect.PointerTo(input).Implements(reflect.TypeFor[xhandler.Initializer]()) || reflect.PointerTo(input).Implements(reflect.TypeFor[xhandler.InvokerInitializer]()) || reflect.PointerTo(input).Implements(reflect.TypeFor[xmcp.Initializer]()) {
			return nil, fmt.Errorf("deferred companion Page does not support input callbacks")
		}
		executable := make([]bindly.BindingSpec, 0)
		for _, b := range selectors {
			parameter := b.Extension.(*spec.Parameter)
			if parameter.QuerySelector.Property == spec.SelectorPropertyPage && b.DefaultValue == nil && (b.Required == nil || !*b.Required) {
				field, ok := result.Field(parameter)
				if result.deferredBinding != nil || !ok || field.Type != reflect.TypeFor[int]() {
					return nil, fmt.Errorf("deferred companion Page requires one exact int field")
				}
				selected := b
				result.deferredBinding = &selected
			} else {
				executable = append(executable, b)
			}
		}
		if result.deferredBinding != nil {
			injector, err := bindly.NewInjector()
			if err != nil {
				return nil, err
			}
			result.deferredPlan, err = injector.CompilePlan(selected.Type, executable...)
			if err != nil {
				return nil, err
			}
		}
	}
	ordinary := make([]bindly.BindingSpec, 0)
	for _, binding := range selectors {
		if binding.Extension.(*spec.Parameter).QuerySelector.Property == spec.SelectorPropertyPage && binding.DefaultValue == nil && (binding.Required == nil || !*binding.Required) && (result.IsDeferredPage(binding.Extension.(*spec.Parameter)) || unlimitedPageView(component, binding.Extension.(*spec.Parameter))) {
			result.pageBindings = append(result.pageBindings, binding)
		} else {
			ordinary = append(ordinary, binding)
		}
	}
	if len(result.pageBindings) > 0 {
		injector, err := bindly.NewInjector()
		if err != nil {
			return nil, err
		}
		result.pagePlan, err = injector.CompilePlan(selected.Type, ordinary...)
		if err != nil {
			return nil, err
		}
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		return nil, err
	}
	defaults := make([]bindly.BindingSpec, 0)
	for _, binding := range selectors {
		if binding.DefaultValue != nil {
			defaults = append(defaults, binding)
		}
	}
	result.absentPlan, err = injector.CompilePlan(selected.Type, defaults...)
	if err != nil {
		return nil, err
	}
	return result, nil

}

func (p *BindingPlan) ValidateProviders(providers []locator.Provider, resolvedPaths ...string) error {
	if p.DeferredPage() {
		if _, err := p.pageSource(providers); err != nil {
			return err
		}
	}
	priorities := map[string]int{}
	needed := map[string]bool{}
	for _, bindings := range [][]bindly.BindingSpec{p.public, p.selectors} {
		for _, binding := range bindings {
			needed[binding.Location.Kind] = true
		}
	}
	for _, provider := range providers {
		if provider != nil && needed[provider.Kind()] {
			priorities[provider.Kind()] = provider.Priority()
		}
	}
	havePublic := false
	maximum := 0
	for i, bindings := range [][]bindly.BindingSpec{p.public, p.selectors} {
		for _, b := range bindings {
			priority, ok := priorities[b.Location.Kind]
			if !ok {
				// Public missing-provider/default/required semantics belong to the
				// canonical Bindly plan, including its authored error status.
				if i == 0 {
					continue
				}
				if b.Location.Kind == "query" && (b.Required == nil || !*b.Required) && !p.DeferredPage() {
					continue
				}
				return fmt.Errorf("selector companion provider %s is not in the composed invocation", b.Location.Kind)
			}
			if b.Priority != 0 {
				priority = b.Priority
			}
			if i == 0 {
				if !havePublic || priority > maximum {
					maximum = priority
				}
				havePublic = true
			} else if havePublic && maximum > priority {
				return fmt.Errorf("selector companion provider priorities interleave canonical binding order")
			}
		}
	}
	return nil
}

// TransportOnly identifies compiler-validated custom-handler declarations.
// Their raw query values remain owned by the handler's downstream invocation.
func (p *BindingPlan) TransportOnly() bool { return p != nil && p.transportOnly }

func (p *BindingPlan) PublicPlan() *bindly.Plan { return p.publicPlan }
func (p *BindingPlan) Type() reflect.Type       { return p.companion }
func (p *BindingPlan) Bind(ctx context.Context, scope *bindly.Injector, input any, providers ...locator.Provider) (any, error) {
	if reflect.TypeOf(input) != reflect.PointerTo(p.input) {
		return nil, fmt.Errorf("selector companion requires exact canonical input")
	}
	target := reflect.New(p.companion).Interface()
	plan := p.companionPlan
	if len(providers) > 0 {
		hasQuery := false
		for _, provider := range providers {
			if provider != nil && provider.Kind() == "query" {
				hasQuery = true
				break
			}
		}
		if !hasQuery {
			plan = p.absentPlan
			// No query provider belongs to this invocation. Evaluate canonical defaults
			// without inheriting an ambient parent query provider.
			var err error
			scope, err = bindly.NewInjector()
			if err != nil {
				return nil, err
			}
		}
	}
	for _, provider := range providers {
		if p.pagePlan != nil && provider != nil && reflect.TypeOf(provider) == nativeQueryProviderType {
			plan = p.pagePlan
			break
		}
	}
	if p.deferredPlan != nil {
		plan = p.deferredPlan
	}
	if err := scope.Bind(ctx, target, bindly.WithPlan(plan), bindly.WithSource(input)); err != nil {
		return nil, err
	}
	return target, nil
}
func (p *BindingPlan) Bindings() []bindly.BindingSpec {
	return append([]bindly.BindingSpec(nil), p.selectors...)
}

// carrier exists only for one reader invocation. It introduces no capability
// key, context state, public interface or replacement public Input.
type carrier struct {
	xhandler.Binder
	plan         *BindingPlan
	input, value any
	page         *pageSource
	pages        map[*spec.Parameter]*pageSource
}

func (p *BindingPlan) Carry(binder xhandler.Binder, input, value any) (xhandler.Binder, error) {
	if binder == nil || reflect.TypeOf(input) != reflect.PointerTo(p.input) || reflect.ValueOf(input).IsNil() || reflect.TypeOf(value) != reflect.PointerTo(p.companion) || reflect.ValueOf(value).IsNil() {
		return nil, fmt.Errorf("selector companion invocation identity mismatch")
	}
	return &carrier{Binder: binder, plan: p, input: input, value: value}, nil
}

// CarryBound associates fresh zero side state with an already bound public
// input. Absent Page sources cannot acquire ambient request/query authority.
func (p *BindingPlan) CarryBound(binder xhandler.Binder, input any) (xhandler.Binder, error) {
	wrapped, err := p.Carry(binder, input, reflect.New(p.companion).Interface())
	if err != nil {
		return nil, err
	}
	c := wrapped.(*carrier)
	c.pages = map[*spec.Parameter]*pageSource{}
	if p.DeferredPage() {
		c.page = &pageSource{binding: *p.deferredBinding, legacy: true}
	}
	for _, binding := range p.pageBindings {
		c.pages[binding.Extension.(*spec.Parameter)] = &pageSource{binding: binding, legacy: p.DeferredPage()}
	}
	return wrapped, nil
}

// CarryPrepared transfers invocation-only selector state after canonical Without.
// It performs no binding, initialization, or deferred Page evaluation.
func (p *BindingPlan) CarryPrepared(source xhandler.Binder, original, prepared any, binder xhandler.Binder) (xhandler.Binder, error) {
	value, err := p.Value(source, original)
	if err != nil {
		return nil, err
	}
	if binder == nil || reflect.TypeOf(prepared) != reflect.PointerTo(p.input) || reflect.ValueOf(prepared).IsNil() || prepared == original || !value.IsValid() || value.Type() != reflect.PointerTo(p.companion) || value.IsNil() {
		return nil, fmt.Errorf("selector companion prepared input identity mismatch")
	}
	copyValue := reflect.New(p.companion)
	copyValue.Elem().Set(value.Elem())
	for i := 0; i < copyValue.Elem().NumField(); i++ {
		field := copyValue.Elem().Field(i)
		switch field.Kind() {
		case reflect.Slice:
			if field.Type() != reflect.TypeFor[[]string]() {
				return nil, fmt.Errorf("unsupported prepared companion field")
			}
			if !field.IsNil() {
				detached := reflect.MakeSlice(field.Type(), field.Len(), field.Len())
				reflect.Copy(detached, field)
				field.Set(detached)
			}
		case reflect.Int:
		default:
			return nil, fmt.Errorf("unsupported prepared companion field")
		}
	}
	return &carrier{Binder: binder, plan: p, input: prepared, value: copyValue.Interface(), page: source.(*carrier).page, pages: source.(*carrier).pages}, nil
}

func (p *BindingPlan) Value(binder xhandler.Binder, input any) (reflect.Value, error) {
	c, ok := binder.(*carrier)
	if !ok || c.plan.authority() != p.authority() || c.plan.association != p.association || c.input != input {
		return reflect.Value{}, fmt.Errorf("selector companion invocation carrier mismatch")
	}
	return reflect.ValueOf(c.value), nil
}

func (p *BindingPlan) Field(parameter *spec.Parameter) (reflect.StructField, bool) {
	for _, binding := range p.selectors {
		if binding.Extension == parameter {
			return p.companion.FieldByName(binding.Path)
		}
	}
	return reflect.StructField{}, false
}
func (p *BindingPlan) Matches(component *spec.Component) bool {
	if p == nil || component == nil || p.association != component.SelectorCompanion {
		return false
	}
	matched := 0
	for _, parameter := range spec.EffectiveParameters(component.Parameters) {
		if _, ok := p.Field(parameter); ok {
			matched++
		}
	}
	return matched == len(p.selectors)
}

func (p *BindingPlan) InputType() reflect.Type { return p.input }

// DeferredPage reports the compile-time explicit-false policy path. Its Page
// remains unevaluated; consumers must never read the companion's int slot.
func (p *BindingPlan) DeferredPage() bool { return p != nil && p.deferredPlan != nil }
func (p *BindingPlan) IsDeferredPage(parameter *spec.Parameter) bool {
	return p.DeferredPage() && p.deferredBinding.Extension == parameter
}

// HasPageSources retains Page conversion until the reader has merged per-view selectors.
func (p *BindingPlan) HasPageSources() bool { return p != nil && len(p.pageBindings) > 0 }
func (p *BindingPlan) PageParameters() []*spec.Parameter {
	var result []*spec.Parameter
	for _, b := range p.pageBindings {
		result = append(result, b.Extension.(*spec.Parameter))
	}
	return result
}
func (p *BindingPlan) CarryPage(binder xhandler.Binder, input, value any, providers []locator.Provider) (xhandler.Binder, error) {
	wrapped, err := p.Carry(binder, input, value)
	if err != nil {
		return nil, err
	}
	c := wrapped.(*carrier)
	c.pages = map[*spec.Parameter]*pageSource{}
	if p.DeferredPage() {
		c.page, err = p.pageSource(providers)
		if err != nil {
			return nil, err
		}
	}
	for _, binding := range p.pageBindings {
		var query locator.Provider
		for _, candidate := range providers {
			if candidate != nil && candidate.Kind() == "query" {
				if query != nil {
					return nil, fmt.Errorf("ambiguous query provider")
				}
				query = candidate
			}
		}
		if query == nil && binding.Required != nil && *binding.Required {
			return nil, fmt.Errorf("selector Page query provider missing")
		}
		c.pages[binding.Extension.(*spec.Parameter)] = &pageSource{provider: query, binding: binding, legacy: p.DeferredPage()}
	}
	return wrapped, nil
}
func (p *BindingPlan) ResolvePageParameter(ctx context.Context, binder xhandler.Binder, input any, parameter *spec.Parameter) (int, error) {
	if _, err := p.Value(binder, input); err != nil {
		return 0, err
	}
	source := binder.(*carrier).pages[parameter]
	if source == nil {
		return 0, fmt.Errorf("selector Page source missing")
	}
	return source.resolve(ctx)
}
func (p *BindingPlan) ResolvePage(ctx context.Context, binder xhandler.Binder, input any) (int, error) {
	if _, err := p.Value(binder, input); err != nil {
		return 0, err
	}
	c := binder.(*carrier)
	if !p.DeferredPage() || c.page == nil {
		return 0, fmt.Errorf("deferred Page invocation source missing")
	}
	return c.page.resolve(ctx)
}

func (p *BindingPlan) pageSource(providers []locator.Provider) (*pageSource, error) {
	var query locator.Provider
	for _, provider := range providers {
		if provider != nil && provider.Kind() == "query" {
			if query != nil {
				return nil, fmt.Errorf("ambiguous query provider")
			}
			query = provider
		}
	}
	return newPageSource(query, *p.deferredBinding)
}

func (p *BindingPlan) authority() *BindingPlan {
	if p.owner != nil {
		return p.owner
	}
	return p
}

// ForPublicPlan retains exact side authority while projecting ordinary URI-
// activated public bindings for one already validated native route.
func (p *BindingPlan) ForPublicPlan(plan *bindly.Plan, bindings []bindly.BindingSpec) (*BindingPlan, error) {
	if plan == nil || plan.TargetType() != p.input {
		return nil, fmt.Errorf("selector public route plan mismatch")
	}
	at := 0
	for _, binding := range bindings {
		found := false
		for at < len(p.public) {
			candidate := p.public[at]
			at++
			if binding.Extension == candidate.Extension && binding.Path == candidate.Path {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("selector public route binding is not a canonical projection")
		}
	}
	copy := *p
	copy.owner = p.authority()
	copy.publicPlan = plan
	copy.public = append([]bindly.BindingSpec(nil), bindings...)
	return &copy, nil
}

func (p *BindingPlan) RawPageField(index []int) bool {
	for _, binding := range p.pageBindings {
		field, ok := p.companion.FieldByName(binding.Path)
		if !ok || len(field.Index) != len(index) {
			continue
		}
		same := true
		for i := range index {
			if index[i] != field.Index[i] {
				same = false
				break
			}
		}
		if same {
			return true
		}
	}
	return false
}

func (p *BindingPlan) SameAuthority(other *BindingPlan) bool {
	return p != nil && other != nil && p.authority() == other.authority() && p.association == other.association && p.input == other.input && p.companion == other.companion
}

func unlimitedPageView(component *spec.Component, parameter *spec.Parameter) bool {
	if parameter == nil || parameter.QuerySelector == nil {
		return false
	}
	target := parameter.QuerySelector.View
	seen := map[*spec.View]bool{}
	var find func(*spec.View) bool
	find = func(view *spec.View) bool {
		if view == nil || seen[view] {
			return false
		}
		seen[view] = true
		if strings.EqualFold(view.CanonicalName(), target) || strings.EqualFold(view.Name, target) {
			if view.Selector != nil && view.Selector.PermissionSpecified(spec.SelectorPropertyPage) && !view.Selector.AllowPage {
				return false
			}
			return view.Selector != nil && view.Selector.NoLimit || view.Source != nil && view.Source.Controls != nil && view.Source.Controls.Limit != nil && *view.Source.Controls.Limit == 0
		}
		for _, relation := range view.Relations {
			if relation != nil && find(relation.View) {
				return true
			}
		}
		return false
	}
	if find(component.RootView) {
		return true
	}
	for _, view := range component.Views {
		if find(view) {
			return true
		}
	}
	return false
}
