package runtime

import (
	"context"
	"fmt"
	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
	"time"
)

type seedReference struct {
	typ      reflect.Type
	pointer  uintptr
	capacity int
}

var seedCapabilityTypes = []reflect.Type{
	reflect.TypeFor[dexec.Reader](), reflect.TypeFor[dexec.DataSource](), reflect.TypeFor[dexec.ProviderScope](), reflect.TypeFor[dexec.ComponentInvoker](), reflect.TypeFor[xhandler.ComponentInvoker](),
	reflect.TypeFor[locator.Provider](), reflect.TypeFor[rhandler.Handler](), reflect.TypeFor[xhandler.Data](), reflect.TypeFor[xhandler.Binder](), reflect.TypeFor[context.Context](),
}

// CloneValue rejects functions/channels/opaque mutable internals; this guard
// additionally rejects even stateless executable runtime capabilities nested
// behind a data interface or pointer. It examines only the detached selection.
func rejectSeedCapabilities(value reflect.Value, seen map[seedReference]bool) error {
	return inspectSeedCapabilities(value, seen, false)
}

// Borrowed references retain private data, so inspect that data as well as
// exported fields. No values are copied, exposed or modified by this walk.
func rejectBorrowedSeedCapabilities(value reflect.Value, seen map[seedReference]bool) error {
	return inspectSeedCapabilities(value, seen, true)
}

func inspectSeedCapabilities(value reflect.Value, seen map[seedReference]bool, borrowed bool) error {
	if !value.IsValid() || (value.Kind() == reflect.Interface && value.IsNil()) {
		return nil
	}
	if borrowed {
		if value.Type() == reflect.TypeFor[time.Time]() {
			return nil
		}
		if readerSeedCapabilityType(value.Type()) {
			return fmt.Errorf("extra reader input contains runtime capability %s", value.Type())
		}
		switch value.Type().PkgPath() {
		case "sync", "sync/atomic", "internal/sync", "internal/runtime/atomic", "runtime/internal/atomic":
			return fmt.Errorf("borrowed component output contains synchronization state %s", value.Type())
		}
		switch value.Kind() {
		case reflect.Func, reflect.Chan, reflect.UnsafePointer:
			return fmt.Errorf("borrowed component output contains opaque mutable type %s", value.Type())
		}
	}
	for _, capability := range seedCapabilityTypes {
		if value.Type().Implements(capability) {
			return fmt.Errorf("extra reader input contains runtime capability %s", value.Type())
		}
	}
	switch value.Kind() {
	case reflect.Interface:
		return inspectSeedCapabilities(value.Elem(), seen, borrowed)
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
		ref := seedReference{typ: value.Type(), pointer: pointer}
		if value.Kind() == reflect.Slice {
			ref.capacity = value.Cap()
		}
		if seen[ref] {
			return nil
		}
		seen[ref] = true
		switch value.Kind() {
		case reflect.Pointer:
			return inspectSeedCapabilities(value.Elem(), seen, borrowed)
		case reflect.Map:
			it := value.MapRange()
			for it.Next() {
				if err := inspectSeedCapabilities(it.Key(), seen, borrowed); err != nil {
					return err
				}
				if err := inspectSeedCapabilities(it.Value(), seen, borrowed); err != nil {
					return err
				}
			}
		case reflect.Slice:
			// CloneValue retains the full backing capacity, which the reader can
			// recover by reslicing. Inspect that same detached graph.
			value = value.Slice(0, value.Cap())
			for i := 0; i < value.Len(); i++ {
				if err := inspectSeedCapabilities(value.Index(i), seen, borrowed); err != nil {
					return err
				}
			}
		}
	case reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if err := inspectSeedCapabilities(value.Index(i), seen, borrowed); err != nil {
				return err
			}
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			if borrowed || value.Type().Field(i).IsExported() {
				if err := inspectSeedCapabilities(value.Field(i), seen, borrowed); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
