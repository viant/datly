package transcribe

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

type linkedInputView struct {
	identity   string
	view       *spec.View
	descriptor *x.Type
}

// packageViewResolver owns package-linked view discovery for either linked
// runtime contracts or descriptor-only package metadata.
type packageViewResolver struct {
	component *spec.Component
	types     *typecatalog.Resolver
}

func newPackageViewResolver(component *spec.Component, types *typecatalog.Resolver) *packageViewResolver {
	return &packageViewResolver{component: component, types: types}
}

func (r *packageViewResolver) rootDescriptorFromMetadata() (*x.Type, error) {
	if r == nil || r.component == nil || r.types == nil {
		return nil, nil
	}
	var result *x.Type
	for _, param := range spec.EffectiveParameters(r.component.Parameters) {
		if param == nil || !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output") ||
			!strings.EqualFold(strings.TrimSpace(param.Source.Name), "view") {
			continue
		}
		descriptor, err := r.types.Descriptor(param.TypeExpr)
		if err != nil {
			return nil, fmt.Errorf("resolve output view type %q: %w", param.TypeExpr, err)
		}
		if descriptor == nil {
			return nil, fmt.Errorf("output view type %q was not found", param.TypeExpr)
		}
		if result != nil && result.Key() != descriptor.Key() {
			return nil, fmt.Errorf("output contract has more than one output/view type")
		}
		result = descriptor
	}
	return result, nil
}

func (r *packageViewResolver) inputDescriptorsFromMetadata() ([]linkedInputView, error) {
	if r == nil || r.component == nil || r.types == nil {
		return nil, nil
	}
	byIdentity := map[string]linkedInputView{}
	for _, param := range spec.EffectiveParameters(r.component.Parameters) {
		if param == nil || !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "view") {
			continue
		}
		name := strings.TrimSpace(param.Source.Name)
		if name == "" {
			name = strings.TrimSpace(param.Name)
		}
		descriptor, err := r.types.Descriptor(param.TypeExpr)
		if err != nil {
			return nil, fmt.Errorf("resolve independent view %q type %q: %w", name, param.TypeExpr, err)
		}
		if descriptor == nil {
			return nil, fmt.Errorf("independent view %q type %q was not found", name, param.TypeExpr)
		}
		if err = r.add(byIdentity, name, descriptor); err != nil {
			return nil, err
		}
	}
	return r.ordered(byIdentity)
}

func (r *packageViewResolver) inputDescriptors(inputType reflect.Type) ([]linkedInputView, error) {
	if r == nil || r.component == nil || inputType == nil {
		return nil, nil
	}
	fields, err := xshape.Linked(inputType).Fields()
	if err != nil {
		return nil, fmt.Errorf("resolve package input fields: %w", err)
	}
	byIdentity := map[string]linkedInputView{}
	for _, field := range fields {
		if !field.Exported {
			continue
		}
		metadata, parseErr := dtag.ParseField(field.StructField())
		if parseErr != nil {
			return nil, fmt.Errorf("parse input field %s: %w", field.Name, parseErr)
		}
		if metadata.Binding == nil || !strings.EqualFold(strings.TrimSpace(metadata.Binding.Location.Kind), "view") {
			continue
		}
		name := strings.TrimSpace(metadata.Binding.Location.In)
		if name == "" {
			name = strings.TrimSpace(metadata.Binding.Name)
		}
		if name == "" {
			name = field.Name
		}
		descriptor, descriptorErr := r.rowDescriptor(field.ReflectedType)
		if descriptorErr != nil {
			return nil, fmt.Errorf("view field %s: %w", field.Name, descriptorErr)
		}
		if err = r.add(byIdentity, name, descriptor); err != nil {
			return nil, fmt.Errorf("resolve input field %s: %w", field.Name, err)
		}
	}
	return r.ordered(byIdentity)
}

func (r *packageViewResolver) rootDescriptor(outputType reflect.Type) (*x.Type, error) {
	if outputType == nil {
		return nil, nil
	}
	fields, err := xshape.Linked(outputType).Fields()
	if err != nil {
		return nil, fmt.Errorf("resolve package output fields: %w", err)
	}
	var result *x.Type
	for _, field := range fields {
		if !field.Exported {
			continue
		}
		metadata, parseErr := dtag.ParseField(field.StructField())
		if parseErr != nil {
			return nil, fmt.Errorf("parse output field %s: %w", field.Name, parseErr)
		}
		if metadata.Binding == nil || !strings.EqualFold(strings.TrimSpace(metadata.Binding.Location.Kind), "output") ||
			!strings.EqualFold(strings.TrimSpace(metadata.Binding.Location.In), "view") {
			continue
		}
		if result != nil {
			return nil, fmt.Errorf("output contract has more than one output/view field")
		}
		result, err = r.rowDescriptor(field.ReflectedType)
		if err != nil {
			return nil, fmt.Errorf("output/view field: %w", err)
		}
	}
	return result, nil
}

func (r *packageViewResolver) linkedDescriptor(typeOf reflect.Type) (*x.Type, error) {
	shape := xshape.Linked(typeOf)
	if shape == nil || shape.NamedDescriptor() == nil {
		return nil, fmt.Errorf("linked contract type is required")
	}
	descriptor := shape.NamedDescriptor()
	if strings.TrimSpace(descriptor.Name) == "" || strings.TrimSpace(descriptor.PkgPath) == "" {
		return nil, fmt.Errorf("linked contract must resolve to a named package type, got %s", typeOf)
	}
	return descriptor, nil
}

func (r *packageViewResolver) rowDescriptor(typeOf reflect.Type) (*x.Type, error) {
	shape := xshape.Linked(typeOf)
	if shape == nil || shape.BaseDescriptor() == nil {
		return nil, fmt.Errorf("linked view type is required")
	}
	descriptor := shape.BaseDescriptor()
	if descriptor.Type == nil || descriptor.Type.Kind() != reflect.Struct ||
		strings.TrimSpace(descriptor.Name) == "" || strings.TrimSpace(descriptor.PkgPath) == "" {
		return nil, fmt.Errorf("view must resolve to a named package struct, got %s", typeOf)
	}
	return descriptor, nil
}

func (r *packageViewResolver) add(target map[string]linkedInputView, name string, descriptor *x.Type) error {
	view, err := r.independent(name)
	if err != nil {
		return err
	}
	if view == nil {
		return fmt.Errorf("independent view %q has no canonical package view", name)
	}
	if explicit := strings.TrimSpace(view.TypeName); explicit != "" && explicit != descriptor.Name {
		return fmt.Errorf("view %q type %q does not match package type %q", name, explicit, descriptor.Name)
	}
	identity, err := view.Identity()
	if err != nil {
		return err
	}
	linked := linkedInputView{identity: identity, view: view, descriptor: descriptor}
	if existing, ok := target[identity]; ok && existing.descriptor.Key() != descriptor.Key() {
		return fmt.Errorf("independent view %q is backed by both %q and %q", identity, existing.descriptor.Key(), descriptor.Key())
	}
	target[identity] = linked
	return nil
}

func (r *packageViewResolver) ordered(byIdentity map[string]linkedInputView) ([]linkedInputView, error) {
	result := make([]linkedInputView, 0, len(byIdentity))
	for _, view := range r.component.Views {
		if view == nil {
			continue
		}
		identity, err := view.Identity()
		if err != nil {
			return nil, err
		}
		if linked, ok := byIdentity[identity]; ok {
			result = append(result, linked)
		}
	}
	return result, nil
}

func (r *packageViewResolver) independent(name string) (*spec.View, error) {
	var matches []*spec.View
	for _, view := range r.component.Views {
		if view != nil && strings.EqualFold(view.CanonicalName(), name) {
			matches = append(matches, view)
		}
	}
	packagePath := strings.TrimSpace(r.component.Key.Scope)
	var scoped *spec.View
	if packagePath != "" {
		for _, view := range matches {
			if strings.TrimSpace(view.Key.Scope) != packagePath {
				continue
			}
			if scoped != nil {
				return nil, fmt.Errorf("canonical independent view %q is ambiguous in package %q", name, packagePath)
			}
			scoped = view
		}
		if scoped != nil {
			return scoped, nil
		}
		if len(matches) > 0 {
			return nil, fmt.Errorf("canonical independent view %q is not owned by package %q", name, packagePath)
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) > 1 {
		return nil, fmt.Errorf("canonical independent view %q is ambiguous", name)
	}
	return nil, nil
}

func (r *packageViewResolver) byIdentity(identity string) *spec.View {
	if r == nil || r.component == nil {
		return nil
	}
	for _, view := range r.component.Views {
		candidate, err := view.Identity()
		if err == nil && candidate == identity {
			return view
		}
	}
	return nil
}

func (r *packageViewResolver) equal(left, right *spec.View) bool {
	comparison := &contractLinker{}
	leftJSON, leftErr := json.Marshal(comparison.normalizedOwnershipView(left, "", false))
	rightJSON, rightErr := json.Marshal(comparison.normalizedOwnershipView(right, "", false))
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}
