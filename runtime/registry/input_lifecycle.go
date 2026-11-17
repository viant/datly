package registry

import (
	"context"
	"fmt"
	"reflect"
)

// inputLifecyclePlan contains only immutable registration-owned metadata.
type inputLifecyclePlan struct {
	owner    reflect.Type
	init     reflect.Method
	prepared reflect.Value
}

func compileInputLifecycle(owner, input reflect.Type) (*inputLifecyclePlan, error) {
	if input == nil || input.Kind() != reflect.Struct || input.Name() != "" {
		return nil, fmt.Errorf("reflected reader lifecycle requires an anonymous runtime input; linked inputs retain their existing lifecycle")
	}
	if owner == nil || owner.Kind() != reflect.Struct {
		return nil, fmt.Errorf("input lifecycle requires a linked struct type")
	}
	method, ok := reflect.PointerTo(owner).MethodByName("Init")
	if !ok {
		return nil, fmt.Errorf("input lifecycle %v requires Init", owner)
	}
	t := method.Type
	if t.IsVariadic() || t.NumIn() != 3 || t.NumOut() != 1 || t.In(1) != reflect.TypeFor[context.Context]() || t.Out(0) != reflect.TypeFor[error]() {
		return nil, fmt.Errorf("input lifecycle %v requires Init(context.Context, *Input or any) error", owner)
	}
	if t.In(2) != reflect.PointerTo(input) && !(input.Name() == "" && t.In(2) == reflect.TypeFor[any]()) {
		return nil, fmt.Errorf("input lifecycle %v does not accept canonical input %v", owner, input)
	}
	prepared := reflect.New(owner)
	if hook, ok := prepared.Interface().(interface{ PrepareInput(reflect.Type) error }); ok {
		if err := hook.PrepareInput(input); err != nil {
			return nil, fmt.Errorf("prepare input lifecycle %v: %w", owner, err)
		}
	}
	return &inputLifecyclePlan{owner: owner, init: method, prepared: prepared.Elem()}, nil
}

// WithLifecycle prepares a separate lifecycle for this route without modifying
// another route or its shared binding contract.
func (c *RouteInputContract) WithLifecycle(owner reflect.Type) (*RouteInputContract, error) {
	if c == nil {
		return nil, fmt.Errorf("input lifecycle requires a route contract")
	}
	plan, err := compileInputLifecycle(owner, c.inputType)
	if err != nil {
		return nil, err
	}
	result := *c
	result.lifecycle = plan
	return &result, nil
}

// WithLifecycle prepares one immutable plan shared by this input's routes.
func (c *InputContract) WithLifecycle(owner reflect.Type) (*InputContract, error) {
	if c == nil {
		return nil, fmt.Errorf("input lifecycle requires an input contract")
	}
	plan, err := compileInputLifecycle(owner, c.typeOf)
	if err != nil {
		return nil, err
	}
	result := *c
	result.routes = make(map[string]*RouteInputContract, len(c.routes))
	for key, route := range c.routes {
		cloned := *route
		cloned.contract = &result
		cloned.lifecycle = plan
		result.routes[key] = &cloned
	}
	return &result, nil
}

type inputLifecycleInvocation struct {
	plan  *inputLifecyclePlan
	input any
}

// LifecycleType exposes only the linked owner identity to contract preparation.
func (c *RouteInputContract) LifecycleType() reflect.Type {
	if c == nil || c.lifecycle == nil {
		return nil
	}
	return c.lifecycle.owner
}

func (v *inputLifecycleInvocation) Init(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	owner := reflect.New(v.plan.owner)
	owner.Elem().Set(v.plan.prepared)
	result := v.plan.init.Func.Call([]reflect.Value{owner, reflect.ValueOf(ctx), reflect.ValueOf(v.input)})
	if result[0].IsNil() {
		return nil
	}
	return result[0].Interface().(error)
}
