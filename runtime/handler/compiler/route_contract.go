package compiler

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

type routeContractCompiler struct {
	component     *spec.Component
	inputType     reflect.Type
	bindings      []bindly.BindingSpec
	injector      *bindly.Injector
	canonicalPlan *bindly.Plan
}

func (c routeContractCompiler) compile(projection *bindly.Projection) (*registry.InputContract, error) {
	routes := c.componentRoutes()
	if len(routes) == 0 {
		return nil, fmt.Errorf("typed component input %s requires at least one route", c.inputType)
	}
	if err := validateRouteActivations(routes, c.bindings); err != nil {
		return nil, err
	}
	injector := c.injector
	if injector == nil {
		var err error
		injector, err = bindly.NewInjector()
		if err != nil {
			return nil, err
		}
	}
	hasActivation := c.hasRouteActivation()
	compiled := make([]registry.RouteInput, 0, len(routes))
	for _, route := range routes {
		ref := spec.RouteRef{Method: route.Method, Path: route.Path}
		routeBindings, bindingErr := effectiveRouteBindings(ref, c.bindings)
		if bindingErr != nil {
			return nil, bindingErr
		}
		plan := c.canonicalPlan
		if hasActivation || plan == nil {
			var planErr error
			plan, planErr = injector.CompilePlan(c.inputType, routeBindings...)
			if planErr != nil {
				return nil, fmt.Errorf("compile input binding plan for %s: %w", ref.String(), planErr)
			}
		}
		compiled = append(compiled, registry.RouteInput{Route: ref, Plan: plan, Bindings: routeBindings})
	}
	contract, err := registry.NewInputContract(c.inputType, projection, compiled...)
	if err != nil {
		return nil, fmt.Errorf("compile component input contract: %w", err)
	}
	return contract, nil
}

func (c routeContractCompiler) hasRouteActivation() bool {
	for _, binding := range c.bindings {
		if strings.TrimSpace(binding.URI) != "" {
			return true
		}
	}
	return false
}

func (c routeContractCompiler) componentRoutes() []*spec.Route {
	if c.component == nil {
		return nil
	}
	result := make([]*spec.Route, 0, len(c.component.Routes))
	for _, route := range c.component.Routes {
		if route != nil {
			result = append(result, route)
		}
	}
	return result
}

func validateRouteActivations(routes []*spec.Route, bindings []bindly.BindingSpec) error {
	for _, binding := range bindings {
		activation := strings.TrimSpace(binding.URI)
		if activation == "" {
			continue
		}
		if !strings.HasPrefix(activation, "/") {
			return fmt.Errorf("binding %s route activation %q must be an absolute path", binding.Path, activation)
		}
		matches := 0
		for _, route := range routes {
			if route != nil && (&spec.RouteActivation{URI: activation}).Matches(route.Path) {
				matches++
			}
		}
		switch {
		case matches == 0:
			return fmt.Errorf("binding %s route activation %q does not match a component route", binding.Path, activation)
		case matches > 1 && !strings.HasPrefix(activation, "/{"):
			return fmt.Errorf("binding %s route activation %q is ambiguous across component routes", binding.Path, activation)
		}
	}
	return nil
}

func effectiveRouteBindings(route spec.RouteRef, bindings []bindly.BindingSpec) ([]bindly.BindingSpec, error) {
	result := make([]bindly.BindingSpec, 0, len(bindings))
	for _, binding := range bindings {
		activation := strings.TrimSpace(binding.URI)
		if activation == "" {
			result = append(result, binding)
			continue
		}
		if !strings.HasPrefix(activation, "/") {
			return nil, fmt.Errorf("binding %s route activation %q must be an absolute path", binding.Path, activation)
		}
		if (&spec.RouteActivation{URI: activation}).Matches(route.Path) {
			binding.URI = route.Path
			result = append(result, binding)
		}
	}
	return result, nil
}
