package runtime

import (
	"context"
	"fmt"
	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
)

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
