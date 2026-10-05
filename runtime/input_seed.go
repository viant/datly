package runtime

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
)

func validateSeedAdmission(request dexec.ComponentRequest, registered *registry.RegisteredComponent) error {
	if registered == nil || registered.Component == nil || registered.Reader == nil || registered.Handler != nil {
		return fmt.Errorf("extra input requires an ordinary registered native reader")
	}
	if request.Input != nil || request.Replay != nil || request.Warmup != nil || request.PrepareQuery || request.DryRun || request.IndependentChildTransactions || len(request.WarmupOmitInput) != 0 {
		return fmt.Errorf("extra input cannot combine with typed input, replay or operational preparation")
	}
	if registered.Component.Settings != nil && (registered.Component.Settings.IndependentChildTransactions || strings.TrimSpace(registered.Component.Settings.Mutation) != "") {
		return fmt.Errorf("extra input cannot use independent child transactions")
	}
	return nil
}

// prepareReaderSeed captures only exact compiled data bindings with explicit
// presence. Supplied Jwt/Auth data is trusted application data, never a verifier
// receipt. Missing/scoped/noncacheable values and capabilities bind normally.
func (r *Runtime) prepareReaderSeed(_ context.Context, registered *registry.RegisteredComponent, route *registry.RouteInputContract, seed *dexec.InputSeed) (any, []string, error) {
	if registered == nil || registered.Input == nil || route == nil || route.Plan() == nil || seed == nil {
		return nil, nil, fmt.Errorf("extra input requires the canonical reader input contract")
	}
	typ := route.Type()
	value := reflect.ValueOf(seed.Input)
	if !value.IsValid() || value.Type() != reflect.PointerTo(typ) || value.IsNil() {
		return nil, nil, fmt.Errorf("extra input must be non-nil exact reader input *%s", typ)
	}
	var paths []string
	for _, field := range route.Fields() {
		binding := field.Binding()
		if binding.Scope != "" || binding.Cacheable != nil && !*binding.Cacheable || !seedDataSource(binding.Location.Kind) {
			continue
		}
		present, marked, err := route.Plan().ExplicitPresence(seed.Input, field.Path())
		if err != nil {
			return nil, nil, fmt.Errorf("extra input presence %s: %w", field.Path(), err)
		}
		if marked && present {
			paths = append(paths, field.Path())
		}
	}
	selection := xshape.CloneOptions{}
	if err := selection.Select(typ, paths...); err != nil {
		return nil, nil, err
	}
	detached, err := (xshape.Runtime{}).CloneValue(seed.Input, selection)
	if err != nil {
		return nil, nil, fmt.Errorf("clone extra reader input: %w", err)
	}
	if err := rejectSeedCapabilities(reflect.ValueOf(detached), map[seedReference]bool{}); err != nil {
		return nil, nil, err
	}
	return detached, paths, nil
}

// An explicit data-kind allowlist prevents new runtime capability kinds from
// becoming seedable by accident. Derived data remains canonical typed input.
func seedDataSource(kind string) bool {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "query", "path", "header", "cookie", "form", "body", "param", "component", "view":
		return true
	default:
		return false
	}
}

type seedReference struct {
	typ     reflect.Type
	pointer uintptr
}

var seedCapabilityTypes = []reflect.Type{
	reflect.TypeFor[dexec.Reader](), reflect.TypeFor[dexec.DataSource](), reflect.TypeFor[dexec.ProviderScope](), reflect.TypeFor[dexec.ComponentInvoker](),
	reflect.TypeFor[locator.Provider](), reflect.TypeFor[rhandler.Handler](), reflect.TypeFor[xhandler.Data](), reflect.TypeFor[xhandler.Binder](), reflect.TypeFor[context.Context](),
}

// CloneValue rejects functions/channels/opaque mutable internals; this guard
// additionally rejects even stateless executable runtime capabilities nested
// behind a data interface or pointer. It examines only the detached selection.
func rejectSeedCapabilities(value reflect.Value, seen map[seedReference]bool) error {
	if !value.IsValid() {
		return nil
	}
	for _, capability := range seedCapabilityTypes {
		if value.Type().Implements(capability) {
			return fmt.Errorf("extra reader input contains runtime capability %s", value.Type())
		}
	}
	switch value.Kind() {
	case reflect.Interface:
		if !value.IsNil() {
			return rejectSeedCapabilities(value.Elem(), seen)
		}
	case reflect.Pointer, reflect.Map, reflect.Slice:
		if value.IsNil() {
			return nil
		}
		pointer := uintptr(0)
		if value.Kind() == reflect.Map {
			pointer = uintptr(value.UnsafePointer())
		} else {
			pointer = value.Pointer()
		}
		ref := seedReference{value.Type(), pointer}
		if seen[ref] {
			return nil
		}
		seen[ref] = true
		switch value.Kind() {
		case reflect.Pointer:
			return rejectSeedCapabilities(value.Elem(), seen)
		case reflect.Map:
			it := value.MapRange()
			for it.Next() {
				if err := rejectSeedCapabilities(it.Key(), seen); err != nil {
					return err
				}
				if err := rejectSeedCapabilities(it.Value(), seen); err != nil {
					return err
				}
			}
		case reflect.Slice:
			for i := 0; i < value.Len(); i++ {
				if err := rejectSeedCapabilities(value.Index(i), seen); err != nil {
					return err
				}
			}
		}
	case reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if err := rejectSeedCapabilities(value.Index(i), seen); err != nil {
				return err
			}
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			if value.Type().Field(i).IsExported() {
				if err := rejectSeedCapabilities(value.Field(i), seen); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
