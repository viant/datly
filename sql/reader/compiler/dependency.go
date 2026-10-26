package compiler

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
	sqlreader "github.com/viant/datly/sql/reader"
	dtag "github.com/viant/datly/tag"
)

// CompileViewDependencies compiles independent canonical views referenced by
// Bindly input bindings into the same reader plans used by route reads.
func CompileViewDependencies(input Input) ([]*sqlreader.ViewDependency, error) {
	if input.Component == nil || len(input.Component.Views) == 0 {
		return nil, nil
	}
	if input.Component.ReaderGraph != nil {
		return nil, fmt.Errorf("reader metadata independent input views are unsupported")
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
		viewInput.sharedBodyTypes, err = independentBodyTypes(input.Component, input.InputType, targetType)
		if err != nil {
			return nil, err
		}
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
	// Independent views bind internal rows, not the component's public response.
	// Keep connector/input settings but do not compile envelope redaction or
	// presentation against a different row type. Never mutate the parent policy.
	if source.Settings != nil {
		settings := *source.Settings
		settings.Output = nil
		result.Settings = &settings
	}
	result.Parameters = make([]*spec.Parameter, 0, len(source.Parameters))
	// A prerequisite has its own view graph. Parent/default-root selectors
	// must not page it or require the parent's transport-only bindings.
	aliases := map[string]bool{}
	visited := map[*spec.View]bool{}
	var visit func(*spec.View)
	visit = func(current *spec.View) {
		if current == nil || visited[current] {
			return
		}
		visited[current] = true
		for _, name := range []string{current.Name, current.Key.Name} {
			if name = strings.ToLower(strings.TrimSpace(name)); name != "" {
				aliases[name] = true
			}
		}
		for _, relation := range current.Relations {
			if relation != nil {
				visit(relation.View)
			}
		}
	}
	visit(view)
	for _, param := range source.Parameters {
		if param == nil || strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output") {
			continue
		}
		if param.QuerySelector != nil && !aliases[strings.ToLower(strings.TrimSpace(param.QuerySelector.View))] {
			continue
		}
		result.Parameters = append(result.Parameters, param)
	}
	return &result
}

// The native body contract is selected before the independent read replaces its
// root. Follow only declared view/on edges, using exact reflected row identity.
func independentBodyTypes(component *spec.Component, inputType, targetType reflect.Type) (map[reflect.Type]bool, error) {
	if component == nil || component.Settings == nil || component.RootView == nil {
		return nil, nil
	}
	switch component.Settings.Mutation {
	case "patch", "post", "put":
	default:
		return nil, nil
	}
	for _, route := range component.Routes {
		if route != nil && route.Handler != "" {
			return nil, nil
		}
	}
	target, err := directViewElemType(targetType)
	if err != nil {
		return nil, err
	}
	for inputType != nil && inputType.Kind() == reflect.Pointer {
		inputType = inputType.Elem()
	}
	if inputType == nil || inputType.Kind() != reflect.Struct {
		return nil, nil
	}
	rows := map[reflect.Type]bool{}
	var visit func(reflect.Type) error
	visit = func(row reflect.Type) error {
		row, err := directViewElemType(row)
		if err != nil {
			return err
		}
		if rows[row] {
			return nil
		}
		rows[row] = true
		for _, field := range reflect.VisibleFields(row) {
			metadata, err := dtag.ParseField(field)
			if err != nil {
				return err
			}
			if metadata.View != nil && len(metadata.Relation) > 0 {
				if err = visit(field.Type); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for _, param := range spec.EffectiveParameters(component.Parameters) {
		if param == nil || !param.IsMutationInput() {
			continue
		}
		field, ok := inputType.FieldByName(param.Name)
		if !ok {
			continue
		}
		metadata, err := dtag.ParseField(field)
		if err != nil {
			return nil, err
		}
		if metadata.View == nil || metadata.View.Name != component.RootView.Name {
			continue
		}
		if err = visit(field.Type); err != nil {
			return nil, err
		}
	}
	if rows[target] {
		return rows, nil
	}
	return nil, nil
}
