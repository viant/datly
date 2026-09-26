package compiler

import (
	"fmt"
	"reflect"

	"github.com/viant/bindly"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	sqlreader "github.com/viant/datly/sql/reader"
)

type selectorCompiler struct {
	component *spec.Component
	inputType reflect.Type
	views     *sqlreader.ViewIndex
	bindings  map[*spec.Parameter]bindly.BindingSpec
}

func CompileSelectorBindings(component *spec.Component, inputType reflect.Type, root *data.View, bindings []bindly.BindingSpec) ([]sqlreader.SelectorBindingPlan, error) {
	return compileSelectorBindings(component, inputType, sqlreader.NewViewIndex(component, root), bindings)
}

func compileSelectorBindings(component *spec.Component, inputType reflect.Type, views *sqlreader.ViewIndex, bindings []bindly.BindingSpec) ([]sqlreader.SelectorBindingPlan, error) {
	byParam := make(map[*spec.Parameter]bindly.BindingSpec, len(bindings))
	for _, binding := range bindings {
		if param, ok := binding.Extension.(*spec.Parameter); ok && param != nil {
			byParam[param] = binding
		}
	}
	compiler := &selectorCompiler{
		component: component,
		inputType: inputType,
		views:     views,
		bindings:  byParam,
	}
	return compiler.compile()
}

func (c *selectorCompiler) compile() ([]sqlreader.SelectorBindingPlan, error) {
	if c.component == nil || c.inputType == nil || c.inputType.Kind() != reflect.Struct {
		return nil, nil
	}
	seen := map[*data.View]map[spec.SelectorProperty]bool{}
	var result []sqlreader.SelectorBindingPlan
	for _, param := range spec.EffectiveParameters(c.component.Parameters) {
		if param == nil || param.QuerySelector == nil {
			continue
		}
		selector := param.QuerySelector
		view, err := c.views.Resolve(selector.View)
		if err != nil {
			return nil, fmt.Errorf("query selector %s: %w", param.Name, err)
		}
		binding, ok := c.bindings[param]
		if !ok {
			return nil, fmt.Errorf("query selector %s is not bound by Bindly", param.Name)
		}
		field, ok := c.inputType.FieldByName(binding.Path)
		if !ok {
			return nil, fmt.Errorf("query selector Bindly field %s for param %s not found", binding.Path, param.Name)
		}
		if !field.IsExported() {
			return nil, fmt.Errorf("query selector input field %s is not exported", field.Name)
		}
		if err := validateSelectorField(selector.Property, field.Type); err != nil {
			return nil, fmt.Errorf("query selector input field %s: %w", field.Name, err)
		}
		if seen[view] == nil {
			seen[view] = map[spec.SelectorProperty]bool{}
		}
		if seen[view][selector.Property] {
			return nil, fmt.Errorf("query selector view %s has duplicate %s property", view.Spec.Name, selector.Property)
		}
		seen[view][selector.Property] = true
		// A linked Go output can contribute a view after DQL transcription.
		// Grant only its explicitly bound selector property at the final view
		// index, just as transcription does for views already in the DQL graph.
		if err := view.Spec.EnableQuerySelector(selector.Property); err != nil {
			return nil, fmt.Errorf("query selector %s: %w", param.Name, err)
		}
		result = append(result, sqlreader.SelectorBindingPlan{
			View:       view,
			Property:   selector.Property,
			FieldIndex: append([]int(nil), field.Index...),
		})
	}
	return result, nil
}

func validateSelectorField(property spec.SelectorProperty, fieldType reflect.Type) error {
	for fieldType.Kind() == reflect.Ptr {
		fieldType = fieldType.Elem()
	}
	switch property {
	case spec.SelectorPropertyFields:
		if fieldType.Kind() == reflect.Slice && fieldType.Elem().Kind() == reflect.String {
			return nil
		}
	case spec.SelectorPropertyOrderBy, spec.SelectorPropertyCriteria:
		if fieldType.Kind() == reflect.String {
			return nil
		}
	case spec.SelectorPropertyOffset, spec.SelectorPropertyLimit, spec.SelectorPropertyPage:
		if fieldType.Kind() >= reflect.Int && fieldType.Kind() <= reflect.Int64 {
			return nil
		}
	default:
		return fmt.Errorf("unsupported selector property %q", property)
	}
	return fmt.Errorf("selector property %s cannot use %s", property, fieldType)
}
