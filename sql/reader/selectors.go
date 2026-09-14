package reader

import (
	"context"
	"fmt"
	"reflect"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
	xstate "github.com/viant/xdatly/state"
)

type invocationSelectors map[*data.View]*xstate.Selector

func resolveInvocationSelectors(ctx context.Context, session *Session, input reflect.Value, binder xhandler.Binder) (invocationSelectors, error) {
	result := invocationSelectors{}
	for _, binding := range session.Artifact.SelectorBindings {
		field, ok := selectorField(input, binding.FieldIndex)
		if !ok {
			continue
		}
		selector := result[binding.View]
		if selector == nil {
			selector = &xstate.Selector{}
			result[binding.View] = selector
		}
		if err := applySelectorBinding(selector, binding, field); err != nil {
			return nil, err
		}
	}
	selectors, err := lookupSelectors(ctx, binder)
	if err != nil {
		return nil, err
	}
	for _, named := range selectors {
		if named == nil {
			continue
		}
		view, err := session.Artifact.ViewIndex.Resolve(named.Name)
		if err != nil {
			return nil, fmt.Errorf("query selector: %w", err)
		}
		result[view] = named.Selector.Clone()
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

func lookupSelectors(ctx context.Context, binder xhandler.Binder) (xstate.Selectors, error) {
	if binder == nil {
		return nil, nil
	}
	value, ok, err := binder.Lookup(ctx, xhandler.SelectorsKey)
	if err != nil || !ok || value == nil {
		return nil, err
	}
	selectors, ok := value.(xstate.Selectors)
	if !ok {
		return nil, fmt.Errorf("selector capability must be state.Selectors, got %T", value)
	}
	return selectors, nil
}

func (s invocationSelectors) forView(view *data.View) *xstate.Selector {
	if s == nil {
		return nil
	}
	return s[view]
}

func selectorField(input reflect.Value, index []int) (reflect.Value, bool) {
	if input.Kind() == reflect.Ptr {
		if input.IsNil() {
			return reflect.Value{}, false
		}
		input = input.Elem()
	}
	for _, fieldIndex := range index {
		for input.Kind() == reflect.Ptr {
			if input.IsNil() {
				return reflect.Value{}, false
			}
			input = input.Elem()
		}
		if input.Kind() != reflect.Struct || fieldIndex < 0 || fieldIndex >= input.NumField() {
			return reflect.Value{}, false
		}
		input = input.Field(fieldIndex)
	}
	for input.Kind() == reflect.Ptr {
		if input.IsNil() {
			return reflect.Value{}, false
		}
		input = input.Elem()
	}
	return input, input.IsValid()
}

func applySelectorBinding(selector *xstate.Selector, binding SelectorBindingPlan, value reflect.Value) error {
	switch binding.Property {
	case spec.SelectorPropertyFields:
		fields := make([]string, value.Len())
		for i := range fields {
			fields[i] = value.Index(i).String()
		}
		selector.Fields = fields
	case spec.SelectorPropertyOrderBy:
		selector.OrderBy = value.String()
	case spec.SelectorPropertyCriteria:
		selector.Criteria = value.String()
	case spec.SelectorPropertyOffset:
		selector.Offset = int(value.Int())
	case spec.SelectorPropertyLimit:
		selector.Limit = int(value.Int())
	case spec.SelectorPropertyPage:
		selector.Page = int(value.Int())
	default:
		return fmt.Errorf("unsupported query selector property %q", binding.Property)
	}
	return nil
}
