package compiler

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
	sqlreader "github.com/viant/datly/sql/reader"
)

// CompileViewDependencies compiles independent canonical views referenced by
// Bindly input bindings into the same reader plans used by route reads.
func CompileViewDependencies(input Input) ([]*sqlreader.ViewDependency, error) {
	if input.Component == nil || len(input.Component.Views) == 0 {
		return nil, nil
	}
	views, err := indexIndependentViews(input.Component.Views)
	if err != nil {
		return nil, err
	}
	result := make([]*sqlreader.ViewDependency, 0)
	compiled := map[string]reflect.Type{}
	for _, binding := range input.Bindings {
		if !strings.EqualFold(strings.TrimSpace(binding.Location.Kind), sqlreader.ViewDependencyKind) {
			continue
		}
		name := strings.TrimSpace(binding.Location.In)
		if name == "" {
			name = strings.TrimSpace(binding.Name)
		}
		view := views[name]
		if view == nil {
			return nil, fmt.Errorf("view binding %s references unknown independent view %q", binding.Path, name)
		}
		targetType, err := bindingTargetType(input.InputType, binding)
		if err != nil {
			return nil, err
		}
		if previous, ok := compiled[name]; ok {
			if previous != targetType {
				return nil, fmt.Errorf("independent view %s has conflicting targets %s and %s", name, previous, targetType)
			}
			continue
		}
		component := componentWithRootView(input.Component, view)
		viewInput := input
		viewInput.Component = component
		viewInput.OutputType = targetType
		viewInput.DirectViewField = ""
		viewInput.DirectViewType = targetType
		plan, err := Compile(viewInput)
		if err != nil {
			return nil, fmt.Errorf("compile independent view %s: %w", name, err)
		}
		result = append(result, &sqlreader.ViewDependency{
			Name: name, TargetType: targetType,
			Component: component, InputType: input.InputType, Plan: plan,
		})
		compiled[name] = targetType
	}
	return result, nil
}

func indexIndependentViews(source []*spec.View) (map[string]*spec.View, error) {
	result := make(map[string]*spec.View, len(source)*2)
	for _, view := range source {
		if view == nil {
			continue
		}
		names := []string{strings.TrimSpace(view.Name), strings.TrimSpace(view.Key.Name)}
		for _, name := range names {
			if name == "" {
				continue
			}
			if existing := result[name]; existing != nil && existing != view {
				return nil, fmt.Errorf("duplicate independent view name %q", name)
			}
			result[name] = view
		}
	}
	return result, nil
}

func bindingTargetType(inputType reflect.Type, binding bindly.BindingSpec) (reflect.Type, error) {
	current := inputType
	for current != nil && current.Kind() == reflect.Ptr {
		current = current.Elem()
	}
	segments := strings.Split(strings.TrimSpace(binding.Path), ".")
	for index, segment := range segments {
		if current == nil || current.Kind() != reflect.Struct {
			return nil, fmt.Errorf("view binding path %s does not resolve through a struct", binding.Path)
		}
		field, ok := current.FieldByName(segment)
		if !ok {
			return nil, fmt.Errorf("view binding field %s was not found on %s", binding.Path, current)
		}
		current = field.Type
		for current.Kind() == reflect.Ptr && index < len(segments)-1 {
			current = current.Elem()
		}
	}
	return current, nil
}

func componentWithRootView(source *spec.Component, view *spec.View) *spec.Component {
	result := *source
	result.RootView = view
	result.Views = nil
	result.Parameters = make([]*spec.Parameter, 0, len(source.Parameters))
	for _, param := range source.Parameters {
		if param == nil || strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output") {
			continue
		}
		result.Parameters = append(result.Parameters, param)
	}
	return &result
}
