package report

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	xhandler "github.com/viant/xdatly/handler"
	xstate "github.com/viant/xdatly/state"
)

// Handler translates one typed report input into selector and filter providers,
// then delegates to the exact source component.
type Handler struct {
	plan *Plan
}

func NewHandler(plan *Plan) *Handler {
	return &Handler{plan: plan}
}

func (h *Handler) Execute(ctx context.Context, invocation rhandler.Invocation) (any, error) {
	if h == nil || h.plan == nil {
		return nil, fmt.Errorf("report handler is not initialized")
	}
	input, err := reportInput(invocation.Input, h.plan.inputType)
	if err != nil {
		return nil, err
	}
	fields, err := h.plan.selectedFields(input)
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("report requires at least one dimension or measure")
	}
	selector, err := h.plan.selector(input, fields)
	if err != nil {
		return nil, err
	}
	providers, err := h.plan.providers(input, selector)
	if err != nil {
		return nil, err
	}
	value, ok, err := invocation.Binder.Lookup(ctx, exec.ComponentInvokerKey)
	if err != nil {
		return nil, err
	}
	invoker, ok := value.(exec.ComponentInvoker)
	if !ok || invoker == nil {
		return nil, fmt.Errorf("report component invoker is unavailable")
	}
	return invoker.InvokeComponent(ctx, exec.ComponentRequest{Target: h.plan.target, Providers: providers})
}

func (p *Plan) selectedFields(input reflect.Value) ([]string, error) {
	seen := map[string]bool{}
	result := make([]string, 0, len(p.dimensions)+len(p.measures))
	appendSelection := func(item selection) error {
		value, present := fieldValue(input, item.index)
		if !present {
			return nil
		}
		value = indirect(value)
		if !value.IsValid() || value.Kind() != reflect.Bool {
			return fmt.Errorf("report selection %s is not boolean", item.name)
		}
		if !value.Bool() || seen[item.name] {
			return nil
		}
		seen[item.name] = true
		result = append(result, item.name)
		for _, holder := range p.holderByName[item.name] {
			if !seen[holder] {
				seen[holder] = true
				result = append(result, holder)
			}
		}
		return nil
	}
	for _, item := range append(append([]selection(nil), p.dimensions...), p.measures...) {
		if err := appendSelection(item); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (p *Plan) selector(input reflect.Value, fields []string) (xstate.Selectors, error) {
	order, err := stringSlice(input, p.orderIndex)
	if err != nil {
		return nil, err
	}
	limit, err := optionalInt(input, p.limitIndex)
	if err != nil {
		return nil, err
	}
	offset, err := optionalInt(input, p.offsetIndex)
	if err != nil {
		return nil, err
	}
	return xstate.Selectors{&xstate.NamedSelector{
		Name: p.view,
		Selector: xstate.Selector{
			Fields: append([]string(nil), fields...), OrderBy: strings.Join(order, ", "),
			Limit: limit, Offset: offset,
		},
	}}, nil
}

func (p *Plan) providers(input reflect.Value, selectors xstate.Selectors) ([]locator.Provider, error) {
	result := []locator.Provider{handlerprovider.Static(xhandler.SelectorsKey, selectors)}
	byKind := map[string]map[string]typedValue{}
	for _, item := range p.filters {
		values := byKind[item.location.Kind]
		if values == nil {
			values = map[string]typedValue{}
			byKind[item.location.Kind] = values
		}
		value, present, err := filterSourceValue(input, item.index, item.sourceType)
		if err != nil {
			return nil, fmt.Errorf("report filter %s: %w", item.name, err)
		}
		if !present {
			continue
		}
		values[item.location.In] = typedValue{typeOf: item.sourceType, value: value.Interface()}
	}
	kinds := make([]string, 0, len(byKind))
	for kind := range byKind {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		values := byKind[kind]
		result = append(result, handlerprovider.Named(kind, func(_ context.Context, targetType reflect.Type, name string) (any, bool, error) {
			value, ok := values[name]
			if !ok {
				return nil, false, nil
			}
			if targetType != value.typeOf {
				return nil, false, fmt.Errorf("report %s/%s requires source type %s, got %s", kind, name, value.typeOf, targetType)
			}
			return value.value, true, nil
		}))
	}
	return result, nil
}
