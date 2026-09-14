package http

import (
	"context"
	"fmt"
	"github.com/viant/bindly"
	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/runtime/jobs"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xasync "github.com/viant/xdatly/async"
	"reflect"
)

type asyncRoute struct {
	targetAPIKeyHeader, targetAPIKeyValue string
	policy                                *asyncInputPolicy
	output                                *bindly.Plan
	injector                              *bindly.Injector
	outputType                            reflect.Type
	mainView, module, jobType             string
}
type asyncRoutes struct {
	routes    map[string]*asyncRoute
	admission AsyncAdmission
}

func (h *Handler) newAsyncRoutes(config []AsyncRoute, input HandlerInput) (*asyncRoutes, error) {
	if len(config) == 0 {
		return nil, nil
	}
	invalidAdmission := input.Async == nil
	if !invalidAdmission {
		value := reflect.ValueOf(input.Async)
		switch value.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			invalidAdmission = value.IsNil()
		}
	}
	if invalidAdmission {
		return nil, fmt.Errorf("HTTP async requires application-owned jobs admission")
	}
	result := &asyncRoutes{routes: map[string]*asyncRoute{}, admission: input.Async}
	public := map[string]bool{}
	for _, route := range h.runtime.Routes() {
		public[(spec.RouteRef{Method: route.Method, Path: route.Path}).String()] = true
	}
	for _, config := range config {
		var normalizeErr error
		config.Route, normalizeErr = spec.ParseRouteRef(config.Route.String())
		if normalizeErr != nil {
			return nil, normalizeErr
		}
		if !public[config.Route.String()] {
			return nil, fmt.Errorf("async route is not an exact public registered route: %s", config.Route.String())
		}
		if result.routes[config.Route.String()] != nil {
			return nil, fmt.Errorf("duplicate HTTP async route")
		}
		if config.Inspect != nil {
			copy := *config.Inspect
			copy.Target, normalizeErr = spec.ParseRouteRef(copy.Target.String())
			if normalizeErr != nil {
				return nil, normalizeErr
			}
			config.Inspect = &copy
			if !public[copy.Target.String()] {
				return nil, fmt.Errorf("async inspection target must be a public registered route")
			}
		}
		var registered *registry.RegisteredComponent
		var contract *registry.RouteInputContract
		for _, component := range input.Components {
			if component == nil || component.Input == nil {
				continue
			}
			if candidate, ok := component.Input.ForRoute(config.Route); ok {
				registered, contract = component, candidate
				break
			}
		}
		if contract == nil {
			return nil, fmt.Errorf("async canonical route input is unavailable")
		}
		if _, err := contract.ReplayPlan(); err != nil {
			return nil, err
		}
		mainView := registered.Component.Name
		if mainView == "" {
			mainView = registered.Component.Key.Name
		}
		if registered.Component.RootView != nil {
			mainView = registered.Component.RootView.Name
		}
		policy := &asyncInputPolicy{config: config, input: contract, mainView: mainView}
		target := config.Route
		if config.Inspect != nil {
			target = config.Inspect.Target
		}
		policy.owns = func(job *xasync.Job) bool { return h.ownsAsyncJob(target, job) }
		if err := policy.validate(); err != nil {
			return nil, err
		}
		bindings, err := (handlercompiler.OutputBindingCompiler{Component: registered.Component, Type: registered.OutputType, Kinds: []string{"async", "output"}}).CompileBindings()
		if err != nil {
			return nil, err
		}
		metadata := false
		filtered := bindings[:0]
		for _, binding := range bindings {
			if binding.Location.Kind == "output" && binding.Location.In != "status" {
				continue
			}
			if binding.Location.Kind == "async" {
				metadata = true
				if _, _, err := (&jobs.Presentation{Job: &xasync.Job{}}).Value(context.Background(), nil, binding.Location.In); err != nil {
					return nil, err
				}
			}
			filtered = append(filtered, binding)
		}
		if !metadata {
			return nil, fmt.Errorf("HTTP async requires authored async output metadata")
		}
		injector, err := bindly.NewInjector()
		if err != nil {
			return nil, err
		}
		plan, err := injector.CompilePlan(registered.OutputType, filtered...)
		if err != nil {
			return nil, err
		}
		kind := "Reader"
		if registered.Handler != nil {
			kind = "Executor"
		}
		compiled := &asyncRoute{policy: policy, output: plan, injector: injector, outputType: registered.OutputType, mainView: mainView, jobType: kind}
		if config.Inspect != nil && config.Inspect.Result {
			var owner *registry.RegisteredComponent
			for _, candidate := range input.Components {
				if candidate != nil && candidate.Input != nil {
					if _, ok := candidate.Input.ForRoute(config.Inspect.Target); ok {
						owner = candidate
						break
					}
				}
			}
			if owner == nil || owner.Handler != nil || owner.Reader == nil || owner.OutputType != registered.OutputType {
				return nil, fmt.Errorf("reader result inspection requires a registered reader with the same output contract type")
			}
			for _, route := range owner.Component.Routes {
				if (spec.RouteRef{Method: route.Method, Path: route.Path}) == config.Inspect.Target {
					compiled.targetAPIKeyHeader, compiled.targetAPIKeyValue = route.APIKeyHeader, route.APIKeyValue
				}
			}
		}
		result.routes[config.Route.String()] = compiled
	}
	return result, nil
}
