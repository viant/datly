package runtime

import (
	"context"
	"reflect"
	"strings"

	"github.com/viant/bindly"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/drainowner"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

// Unlike the legacy finite group, this policy admits native readers with nested
// read dependencies. Runtime revalidates every actual descendant, including calls
// made by handwritten hooks; no authority is inferred from the HTTP method.
func readOnlyGroupReader(entry *RegisteredComponent, route spec.RouteRef) bool {
	if entry == nil || entry.Component == nil || entry.Input == nil || entry.OutputType == nil || entry.Reader == nil || entry.Handler != nil || entry.DataSource != nil || !componentOwnsRoute(entry.Component, route) || !groupSettingsAllowed(entry.Component) || groupManagedProviderOverride(entry) {
		return false
	}
	input, ok := entry.Input.ForRoute(route)
	if !ok {
		return false
	}
	for _, field := range input.Fields() {
		binding := field.Binding()
		if binding.Async || binding.Scope != "" || binding.ResolutionGroup != nil && binding.ResolutionGroup.Mode != bindly.ResolutionGroupDependencies {
			return false
		}
	}
	output := entry.OutputType
	if output.Kind() != reflect.Pointer {
		output = reflect.PointerTo(output)
	}
	// Post-commit callbacks cannot truthfully complete before a binding result is
	// exposed while retaining an inherited transaction. Never run them early.
	return !output.Implements(reflect.TypeFor[xhandler.Finalizer]()) && !output.Implements(reflect.TypeFor[xmcp.Finalizer]())
}

// Recorder/default-provider wrappers retain these compiled identities. The
// wrapper's address is deliberately not an identity for indexed registrations.
type bindingGroupRegistration struct {
	component  *spec.Component
	input      *registry.InputContract
	output     *registry.OutputContract
	outputType reflect.Type
	handler    rhandler.Handler
}

func groupRegistration(entry *RegisteredComponent) bindingGroupRegistration {
	return bindingGroupRegistration{component: entry.Component, input: entry.Input, output: entry.Output, outputType: entry.OutputType, handler: entry.Handler}
}
func groupManagedProviderOverride(entry *RegisteredComponent) bool {
	for _, provider := range entry.Providers {
		if provider == nil {
			continue
		}
		switch provider.Kind() {
		case "component", string(xhandler.ComponentInvokerKey), string(xhandler.InputKey), string(xhandler.DataKey), string(xhandler.DMLKey), string(xhandler.SequencerKey), string(xhandler.FlusherKey), string(xhandler.TransactionStarterKey), string(xhandler.MutationReporterKey), string(rhandler.TransactionSQLCapabilityKey):
			return true
		}
	}
	return false
}
func groupSettingsAllowed(c *spec.Component) bool {
	if c == nil {
		return false
	}
	s := c.Settings
	return s == nil || (s.Mutation == "" && s.SequenceStrategy == "" && len(s.ProtectedFlushTables) == 0 && !s.IndependentChildTransactions && (s.ComponentCallPolicy == "" || s.ComponentCallPolicy == "buffered"))
}
func groupFieldsSupported(input *registry.RouteInputContract) bool {
	if input == nil {
		return false
	}
	for _, field := range input.Fields() {
		b := field.Binding()
		if b.When != "" || b.With != "" || b.ResolutionGroup != nil || b.Async {
			return false
		}
	}
	return true
}
func groupReaderLeaf(entry *RegisteredComponent, route spec.RouteRef) (*registry.RouteInputContract, error) {
	if entry == nil || entry.Component == nil || entry.Input == nil || entry.Handler != nil || entry.Reader == nil || entry.DataSource != nil || entry.OutputType == nil || !componentOwnsRoute(entry.Component, route) || !groupSettingsAllowed(entry.Component) || groupManagedProviderOverride(entry) {
		return nil, drainowner.ErrBindingGroup
	}
	input, ok := entry.Input.ForRoute(route)
	if !ok || !groupFieldsSupported(input) {
		return nil, drainowner.ErrBindingGroup
	}
	for _, field := range input.Fields() {
		if _, ok := field.Dependency(); ok || field.Binding().Location.Kind == string(spec.KindComponent) {
			return nil, drainowner.ErrBindingGroup
		}
	}
	return input, nil
}
func (r *Runtime) scopedGroupDescriptor(ctx context.Context, entry *RegisteredComponent, route spec.RouteRef, request dexec.ComponentRequest, buffered bool) (drainowner.ScopedGroupTarget, error) {
	reject := func() (drainowner.ScopedGroupTarget, error) {
		return drainowner.ScopedGroupTarget{}, drainowner.ErrBindingGroup
	}
	if !buffered || entry == nil || entry.Component == nil || entry.Input == nil || entry.Handler == nil || entry.Reader != nil || entry.DataSource != nil || entry.OutputType == nil || !groupSettingsAllowed(entry.Component) || groupManagedProviderOverride(entry) || request.Completion != nil || request.BindingOutput != nil || request.ReaderOptions != nil || request.ReportOrdering != nil || len(request.WarmupOmitInput) != 0 {
		return reject()
	}
	if root := entry.Component.RootView; root != nil {
		if source := root.RuntimeSource(); source != nil && (strings.TrimSpace(source.SQL) != "" || strings.TrimSpace(source.URI) != "" || strings.TrimSpace(source.Table) != "") {
			return reject()
		}
	}
	scoped, err := custom.ScopedInputBinding(entry.Handler)
	if err != nil || !scoped {
		return reject()
	}
	typed, ok := entry.Handler.(rhandler.TypedHandler)
	if !ok || typed.InputType() != entry.Input.Type() || typed.OutputType() != entry.OutputType {
		return reject()
	}
	input, ok := entry.Input.ForRoute(route)
	if !ok || !groupFieldsSupported(input) || input.PrepareScopePlans() != nil {
		return reject()
	}
	var dependency *registry.InputField
	for _, field := range input.Fields() {
		if _, ok := field.Dependency(); ok {
			if dependency != nil {
				return reject()
			}
			f := field
			dependency = &f
		}
	}
	if dependency == nil || strings.TrimSpace(dependency.Binding().Scope) == "" || dependency.Binding().Transformer != nil || dependency.SourceType() != dependency.DestinationType() {
		return reject()
	}
	target, _ := dependency.Dependency()
	component, _, ok := r.bundle.ComponentByRouteWithParams(target.Method, target.Path)
	if !ok || component == nil || component.Key == entry.Component.Key {
		return reject()
	}
	child, err := r.registeredComponent(ctx, component.Key)
	if err != nil {
		return drainowner.ScopedGroupTarget{}, err
	}
	if _, err = groupReaderLeaf(child, target); err != nil {
		return reject()
	}
	destination := dependency.DestinationType()
	output := child.OutputType
	if destination != output && (output.Kind() == reflect.Pointer || destination != reflect.PointerTo(output)) {
		return reject()
	}
	return drainowner.NewScopedGroupTarget(groupRegistration(entry), groupRegistration(child), route.String(), target.String(), dependency.Binding().Scope, destination)
}
