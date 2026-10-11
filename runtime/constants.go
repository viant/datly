package runtime

import (
	"fmt"
	"strings"

	"github.com/viant/bindly/locator"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
)

type componentConstants struct{ provider locator.Provider }

// constantProvider gives lazy registrations the same immutable authority as
// eager registrations. Cache empty sets too; the pinned component is immutable.
func (r *Runtime) constantProvider(component *spec.Component) (locator.Provider, error) {
	key := component.Key.String()
	if provider, ok := r.canonicalConstants[key]; ok {
		return provider, nil
	}
	if cached, ok := r.lazyConstants.Load(key); ok {
		return cached.(componentConstants).provider, nil
	}
	values, err := canonicalConstantValues(component)
	if err != nil {
		return nil, fmt.Errorf("component %s constants: %w", key, err)
	}
	provider, err := handlerprovider.Constants(values)
	if err != nil {
		return nil, fmt.Errorf("component %s constants: %w", key, err)
	}
	cached, _ := r.lazyConstants.LoadOrStore(key, componentConstants{provider: provider})
	return cached.(componentConstants).provider, nil
}

func canonicalConstantValues(component *spec.Component) (map[string]string, error) {
	if component == nil {
		return nil, nil
	}
	values := map[string]string{}
	names := map[string]string{}
	add := func(name, value string) error {
		name = strings.TrimSpace(name)
		canonical := strings.ToLower(name)
		if canonical == "" {
			return fmt.Errorf("constant name is required")
		}
		if previous, ok := values[canonical]; ok && previous != value {
			return fmt.Errorf("constant %q has conflicting values %q and %q", names[canonical], previous, value)
		}
		values[canonical] = value
		names[canonical] = name
		return nil
	}
	if component.Settings != nil {
		for name, value := range component.Settings.Const {
			if err := add(name, value); err != nil {
				return nil, err
			}
		}
	}
	for _, param := range spec.EffectiveParameters(component.Parameters) {
		if param == nil || !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "const") || param.Value == nil {
			continue
		}
		name := strings.TrimSpace(param.Source.Name)
		if name == "" {
			name = strings.TrimSpace(param.Name)
		}
		if err := add(name, *param.Value); err != nil {
			return nil, err
		}
	}
	if len(values) == 0 {
		return nil, nil
	}
	result := make(map[string]string, len(values))
	for canonical, value := range values {
		result[names[canonical]] = value
	}
	return result, nil
}
