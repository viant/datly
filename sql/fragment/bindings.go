package fragment

import (
	"fmt"
	"reflect"
)

type Bindings struct {
	args          []any
	nullableTypes []reflect.Type
}

// NewBindings retains immutable declared pointer types for emitted nullable
// members. An absent member remains SQL NULL without losing its native type.
func NewBindings(nullableTypes ...reflect.Type) *Bindings {
	return &Bindings{nullableTypes: append([]reflect.Type(nil), nullableTypes...)}
}

func (b *Bindings) AddNullable(value any, typeIndex int) (string, error) {
	if typeIndex < 0 || typeIndex >= len(b.nullableTypes) || b.nullableTypes[typeIndex] == nil || b.nullableTypes[typeIndex].Kind() != reflect.Pointer {
		return "", fmt.Errorf("declared nullable SQL binding type is unavailable")
	}
	if value == nil {
		value = reflect.Zero(b.nullableTypes[typeIndex]).Interface()
	}
	return b.Add(value)
}

func (b *Bindings) Add(value any) (string, error) {
	b.Append(value)
	return "?", nil
}

func (b *Bindings) Append(values ...any) {
	for _, value := range values {
		b.args = append(b.args, snapshotBindingValue(value))
	}
}

// snapshotBindingValue detaches an emitted value from Velty's reusable loop
// slot before the next iteration mutates that storage.
func snapshotBindingValue(value any) any {
	reflected := reflect.ValueOf(value)
	for reflected.IsValid() && reflected.Kind() == reflect.Interface {
		if reflected.IsNil() {
			return nil
		}
		reflected = reflected.Elem()
	}
	if !reflected.IsValid() {
		return nil
	}
	snapshot := reflect.New(reflected.Type()).Elem()
	switch reflected.Kind() {
	case reflect.Bool:
		snapshot.SetBool(reflected.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		snapshot.SetInt(reflected.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		snapshot.SetUint(reflected.Uint())
	case reflect.Float32, reflect.Float64:
		snapshot.SetFloat(reflected.Float())
	case reflect.Complex64, reflect.Complex128:
		snapshot.SetComplex(reflected.Complex())
	case reflect.String:
		snapshot.SetString(reflected.String())
	case reflect.Slice:
		if reflected.IsNil() {
			return snapshot.Interface()
		}
		cloned := reflect.MakeSlice(reflected.Type(), reflected.Len(), reflected.Len())
		reflect.Copy(cloned, reflected)
		return cloned.Interface()
	default:
		// Pointers retain their concrete type so pointer-receiver driver.Valuer
		// implementations remain available to database/sql.
		snapshot.Set(reflected)
	}
	return snapshot.Interface()
}

func (b *Bindings) Args() []any { return append([]any(nil), b.args...) }
