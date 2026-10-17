package runtime

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
	"github.com/viant/xdatly/connector"
	"github.com/viant/xdatly/differ"
	xhandler "github.com/viant/xdatly/handler"
)

func validateSeedAdmission(request dexec.ComponentRequest, component *RegisteredComponent) error {
	if component != nil && component.Component != nil && component.Component.Settings != nil && (component.Component.Settings.IndependentChildTransactions || strings.TrimSpace(component.Component.Settings.Mutation) != "") {
		return fmt.Errorf("extra input cannot use mutation or independent child transactions")
	}
	if request.Input != nil || request.Replay != nil {
		return fmt.Errorf("extra reader input cannot combine with Input or Replay")
	}
	if component == nil {
		return fmt.Errorf("extra input requires an ordinary registered native reader or scoped typed custom handler")
	}
	if component.Handler != nil {
		scoped, err := custom.ScopedInputBinding(component.Handler)
		if err != nil {
			return err
		}
		if !scoped || component.Reader != nil || component.DataSource != nil {
			return fmt.Errorf("extra input requires an ordinary registered native reader or source-less scoped typed custom handler")
		}
	} else if component.Reader == nil {
		return fmt.Errorf("extra input requires an ordinary registered native reader or scoped typed custom handler")
	}
	if request.Warmup != nil || request.PrepareQuery || request.DryRun || len(request.WarmupOmitInput) > 0 || request.IndependentChildTransactions {
		return fmt.Errorf("extra reader input cannot combine with operational preparation or independent transactions")
	}
	return nil
}

// Ordinary data kinds are an explicit boundary. An unknown kind or capability
// never gains seed authority from an assignable value or a presence marker.
func readerSeedDataKind(kind string) bool {
	switch kind {
	case "query", "path", "header", "cookie", "form", "body", "component", "view", "param", "state", "const", "literal", "env", "generator":
		return true
	}
	return false
}

func (r *Runtime) prepareReaderSeed(ctx context.Context, component *RegisteredComponent, contract *registry.RouteInputContract, seed *dexec.InputSeed) (any, []string, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if contract == nil || contract.Plan() == nil {
		return nil, nil, fmt.Errorf("canonical reader seed contract is required")
	}
	value := seed.Value()
	actual := reflect.ValueOf(value)
	typ := contract.Type()
	if !actual.IsValid() || actual.Type() != reflect.PointerTo(typ) || actual.IsNil() {
		return nil, nil, fmt.Errorf("extra reader input must be non-nil *%s", typ)
	}
	selection := xshape.CloneOptions{}
	if err := selection.Select(typ); err != nil {
		return nil, nil, err
	}
	var paths []string
	borrowed := map[string]any{}
	for _, field := range contract.Fields() {
		binding := field.Binding()
		kind := strings.ToLower(strings.TrimSpace(binding.Location.Kind))
		if !readerSeedDataKind(kind) || readerSeedCapabilityType(field.DestinationType()) || binding.Scope != "" || (binding.Cacheable != nil && !*binding.Cacheable) || (kind == "state" && binding.Cacheable == nil) {
			continue
		}
		present, marker, err := contract.Plan().ExplicitPresence(value, field.Path())
		if err != nil {
			return nil, nil, err
		}
		if !marker || !present {
			continue
		}
		access, err := xshape.Linked(typ).Accessor(field.Path())
		if err != nil {
			return nil, nil, err
		}
		supplied, found, err := access.GetOptional(value)
		if err != nil {
			return nil, nil, err
		}
		if !found {
			continue
		}
		if (kind == "view" || kind == "param" || kind == "state") && seedNil(supplied) {
			continue
		}
		borrow := false
		if kind == "component" {
			ref, err := spec.ParseRouteRef(binding.Location.In)
			if err != nil {
				return nil, nil, err
			}
			dependency, _, found := r.bundle.ComponentByRouteWithParams(ref.Method, ref.Path)
			if !found || dependency == nil {
				return nil, nil, fmt.Errorf("seed dependency route is not registered: %s", ref.String())
			}
			registered, err := r.registeredComponent(ctx, dependency.Key)
			if err != nil {
				return nil, nil, err
			}
			if registered == nil || registered.OutputType != (xshape.Runtime{}).Indirect(field.DestinationType()) {
				return nil, nil, fmt.Errorf("seed dependency output type does not match %s", field.Path())
			}
			stripped, err := selectDependencySeedData(&selection, registered)
			if err != nil {
				return nil, nil, err
			}
			if !stripped && supplied.Kind() == reflect.Pointer {
				if err := rejectBorrowedSeedCapabilities(supplied, map[seedReference]bool{}); err != nil {
					return nil, nil, err
				}
				borrowed[field.Path()] = supplied.Interface()
				borrow = true
			}
		}
		if !borrow {
			if err := selection.Select(typ, field.Path()); err != nil {
				return nil, nil, err
			}
		}
		paths = append(paths, field.Path())
	}
	detached, err := (xshape.Runtime{}).CloneValue(value, selection)
	if err != nil {
		return nil, nil, fmt.Errorf("clone extra reader input: %w", err)
	}
	if err := rejectSeedCapabilities(reflect.ValueOf(detached), map[seedReference]bool{}); err != nil {
		return nil, nil, err
	}
	// Borrowed component outputs remain caller-owned read-only references. The
	// child still traverses canonical binding, predicates and lifecycle phases.
	for path, ref := range borrowed {
		access, err := xshape.Linked(typ).Accessor(path)
		if err != nil {
			return nil, nil, err
		}
		if err := access.Set(detached, ref); err != nil {
			return nil, nil, err
		}
	}
	return detached, paths, nil
}

func seedNil(value reflect.Value) bool {
	for value.IsValid() && value.Kind() == reflect.Interface {
		if value.IsNil() {
			return true
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return true
	}
	switch value.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
		return value.IsNil()
	}
	return false
}

// Use the same output binding compiler that owns native output capabilities.
// Exclude declared capability slots, never JSON-hidden authorization data.
func selectDependencySeedData(selection *xshape.CloneOptions, dependency *RegisteredComponent) (bool, error) {
	kinds := []string{"logger", "binder", "component_invoker", "connector", "differ", "validator", "mbus", "message_bus", "executor", "transaction", "session", "input_snapshot", "input_read_metadata", "read_metadata", "reader_input", "http_request", "request"}
	for _, p := range spec.EffectiveParameters(dependency.Component.Parameters) {
		if p != nil && p.EmitOutput && !readerSeedDataKind(p.Source.Kind) && p.Source.Kind != "output" {
			kinds = append(kinds, p.Source.Kind)
		}
	}
	bindings, err := (compiler.OutputBindingCompiler{Component: dependency.Component, Type: dependency.OutputType, Kinds: kinds}).CompileBindings()
	if err != nil {
		return false, err
	}
	if len(bindings) == 0 {
		return false, nil
	}
	excluded := map[reflect.Type]map[int]bool{}
	for _, binding := range bindings {
		member, err := xshape.Linked(dependency.OutputType).StructField(binding.Path)
		if err != nil {
			return false, err
		}
		current := dependency.OutputType
		for n, index := range member.Index {
			current = (xshape.Runtime{}).Indirect(current)
			if excluded[current] == nil {
				excluded[current] = map[int]bool{}
			}
			if n == len(member.Index)-1 {
				excluded[current][index] = true
			}
			current = current.Field(index).Type
		}
	}
	for typ, blocked := range excluded {
		var names []string
		for n := 0; n < typ.NumField(); n++ {
			member := typ.Field(n)
			if !member.IsExported() {
				return false, fmt.Errorf("seed output selection cannot omit private field %s.%s", typ, member.Name)
			}
			if !blocked[n] {
				names = append(names, member.Name)
			}
		}
		if err := selection.Select(typ, names...); err != nil {
			return false, err
		}
	}
	return true, nil
}

// Capability exclusion follows declared interface/type identity even if a
// trusted caller has mislabeled a service as an ordinary data binding.
func readerSeedCapabilityType(typ reflect.Type) bool {
	if typ == nil {
		return false
	}
	for _, service := range []reflect.Type{
		reflect.TypeFor[dexec.ComponentInvoker](), reflect.TypeFor[xhandler.Binder](),
		reflect.TypeFor[xhandler.Data](), reflect.TypeFor[xhandler.DML](),
		reflect.TypeFor[xhandler.Logger](), reflect.TypeFor[xhandler.Validator](),
		reflect.TypeFor[xhandler.MessageBus](), reflect.TypeFor[connector.Provider](),
		reflect.TypeFor[differ.Differ](), reflect.TypeFor[xhandler.ReadMetadata](),
		reflect.TypeFor[context.Context](),
	} {
		if typ.Implements(service) || (typ.Kind() != reflect.Pointer && reflect.PointerTo(typ).Implements(service)) {
			return true
		}
	}
	typ = (xshape.Runtime{}).Indirect(typ)
	return typ == reflect.TypeFor[sql.DB]() || typ == reflect.TypeFor[sql.Tx]() || typ == reflect.TypeFor[http.Request]() || typ == reflect.TypeFor[dexec.ReaderInput]()
}
