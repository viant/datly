package runtime

import (
	"fmt"
	"reflect"

	"github.com/viant/bindly"
	"github.com/viant/datly/runtime/handler/compiler"
)

// outputCapabilityPlan compiles once per immutable registration. It binds only
// output-declared server capabilities and constants; payload/view/status slots remain owned
// by their existing execution and encoding paths.
func (r *Runtime) outputCapabilityPlan(component *RegisteredComponent) (*bindly.Plan, error) {
	if component == nil || component.OutputType == nil || component.OutputType.Kind() != reflect.Struct {
		return nil, nil
	}
	if cached, ok := r.outputCapabilityPlans.Load(component); ok {
		return cached.(*outputCapabilityEntry).plan, cached.(*outputCapabilityEntry).err
	}
	entry := &outputCapabilityEntry{}
	bindings, err := (compiler.OutputBindingCompiler{Component: component.Component, Type: component.OutputType, Kinds: []string{"logger", "const"}}).CompileBindings()
	if err != nil {
		entry.err = err
	} else if len(bindings) > 0 {
		var injector *bindly.Injector
		injector, entry.err = bindly.NewInjector()
		if entry.err == nil {
			entry.plan, entry.err = injector.CompilePlan(component.OutputType, bindings...)
		}
	}
	if entry.err != nil {
		entry.err = fmt.Errorf("compile output capabilities for %s: %w", component.Component.Key.String(), entry.err)
	}
	cached, _ := r.outputCapabilityPlans.LoadOrStore(component, entry)
	result := cached.(*outputCapabilityEntry)
	return result.plan, result.err
}

type outputCapabilityEntry struct {
	plan *bindly.Plan
	err  error
}
