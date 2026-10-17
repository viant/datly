package selectorcompanion

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"

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
	Association *model.Association
	Type        reflect.Type
	Parameters  []*spec.Parameter
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
	result := &Selection{Association: component.SelectorCompanion, Type: contract.Descriptor.Type}
	candidates := map[string]*spec.Parameter{}
	for _, p := range spec.EffectiveParameters(component.Parameters) {
		if p == nil || p.EmitOutput || p.Source.Kind == "output" {
			continue
		}
		if _, exists := candidates[p.Name]; exists {
			return nil, fmt.Errorf("selector companion ambiguous canonical parameter %s", p.Name)
		}
		candidates[p.Name] = p
	}
	for _, f := range contract.Fields {
		p := candidates[f.Name]
		if p == nil {
			return nil, fmt.Errorf("selector companion canonical parameter %s missing", f.Name)
		}
		m, err := tag.ParseField(f.StructField())
		if err != nil {
			return nil, err
		}
		b := m.Binding
		typ, _ := f.BuiltinType()
		name := b.Name
		if name == "" {
			name = f.Name
		}
		expected := spec.Parameter{Name: name, Source: spec.BindSource{Kind: b.Location.Kind, Name: b.Location.In}, TypeExpr: typ, QueryListCSV: m.QueryListCSV, Required: b.Required, Cacheable: b.Cacheable, Cardinality: b.Cardinality, ErrorStatusCode: b.ErrorCode, ErrorMessage: b.ErrorMessage, QuerySelector: &spec.QuerySelectorBinding{View: m.QuerySelector.View, Property: m.QuerySelector.Property}}
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
}

func NewBindingPlan(component *spec.Component, selected *Selection, input reflect.Type, publicPlan, companionPlan *bindly.Plan, public, selectors []bindly.BindingSpec) (*BindingPlan, error) {
	if selected == nil {
		return nil, nil
	}
	if selected.Association != component.SelectorCompanion || publicPlan == nil || companionPlan == nil || publicPlan.TargetType() != input || companionPlan.TargetType() != selected.Type || len(selectors) != len(selected.Parameters) {
		return nil, fmt.Errorf("selector companion compiled plan mismatch")
	}
	ordered := append(append([]bindly.BindingSpec(nil), public...), selectors...)
	canonical := []*spec.Parameter{}
	for _, p := range spec.EffectiveParameters(component.Parameters) {
		if p != nil && !p.EmitOutput && p.Source.Kind != "output" {
			canonical = append(canonical, p)
		}
	}
	if len(ordered) != len(canonical) {
		return nil, fmt.Errorf("selector companion requires exact canonical binding declaration order")
	}
	locations := map[string]bool{}
	for i, b := range ordered {
		if b.Extension != canonical[i] {
			return nil, fmt.Errorf("selector companion requires trailing canonical selectors and reflected binding order")
		}
		if b.Async || b.ResolutionGroup != nil || b.Scope != "" || b.When != "" || b.With != "" || b.URI != "" || b.ResourceRef != "" {
			return nil, fmt.Errorf("selector companion does not support staged binding responsibilities")
		}
		key := b.Location.Kind + "\x00" + b.Location.In
		if i < len(public) {
			locations[key] = true
		} else {
			if locations[key] {
				return nil, fmt.Errorf("selector companion overlaps an existing source location")
			}
			locations[key] = true
		}
	}
	for i, b := range selectors {
		if b.Extension != selected.Parameters[i] {
			return nil, fmt.Errorf("selector companion canonical parameter identity mismatch")
		}
	}
	result := &BindingPlan{association: selected.Association, input: input, companion: selected.Type, publicPlan: publicPlan, companionPlan: companionPlan, public: append([]bindly.BindingSpec(nil), public...), selectors: append([]bindly.BindingSpec(nil), selectors...)}
	if component.Settings != nil && component.Settings.IgnoreEmptyQueryParameters != nil && !*component.Settings.IgnoreEmptyQueryParameters {
		if reflect.PointerTo(input).Implements(reflect.TypeFor[xhandler.Initializer]()) || reflect.PointerTo(input).Implements(reflect.TypeFor[xhandler.InvokerInitializer]()) || reflect.PointerTo(input).Implements(reflect.TypeFor[xmcp.Initializer]()) {
			return nil, fmt.Errorf("deferred companion Page does not support input callbacks")
		}
		var executable []bindly.BindingSpec
		for _, b := range selectors {
			parameter := b.Extension.(*spec.Parameter)
			if parameter.QuerySelector.Property == spec.SelectorPropertyPage {
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
	return result, nil

}

func (p *BindingPlan) ValidateProviders(providers []locator.Provider) error {
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
func (p *BindingPlan) PublicPlan() *bindly.Plan { return p.publicPlan }
func (p *BindingPlan) Type() reflect.Type       { return p.companion }
func (p *BindingPlan) Bind(ctx context.Context, scope *bindly.Injector, input any) (any, error) {
	if reflect.TypeOf(input) != reflect.PointerTo(p.input) {
		return nil, fmt.Errorf("selector companion requires exact canonical input")
	}
	target := reflect.New(p.companion).Interface()
	plan := p.companionPlan
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
}

func (p *BindingPlan) Carry(binder xhandler.Binder, input, value any) (xhandler.Binder, error) {
	if binder == nil || reflect.TypeOf(input) != reflect.PointerTo(p.input) || reflect.ValueOf(input).IsNil() || reflect.TypeOf(value) != reflect.PointerTo(p.companion) || reflect.ValueOf(value).IsNil() {
		return nil, fmt.Errorf("selector companion invocation identity mismatch")
	}
	return &carrier{Binder: binder, plan: p, input: input, value: value}, nil
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
	return &carrier{Binder: binder, plan: p, input: prepared, value: copyValue.Interface(), page: source.(*carrier).page}, nil
}

func (p *BindingPlan) Value(binder xhandler.Binder, input any) (reflect.Value, error) {
	c, ok := binder.(*carrier)
	if !ok || c.plan != p || c.plan.association != p.association || c.input != input {
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

func (p *BindingPlan) CarryPage(binder xhandler.Binder, input, value any, providers []locator.Provider) (xhandler.Binder, error) {
	wrapped, err := p.Carry(binder, input, value)
	if err != nil {
		return nil, err
	}
	if !p.DeferredPage() {
		return nil, fmt.Errorf("deferred Page plan missing")
	}
	source, err := p.pageSource(providers)
	if err != nil {
		return nil, err
	}
	wrapped.(*carrier).page = source
	return wrapped, nil
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
