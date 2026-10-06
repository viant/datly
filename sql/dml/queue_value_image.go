package dml

import (
	"database/sql/driver"
	"encoding"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
)

// Pure detached comparison evidence. Execution payloads remain shallow.
// Unsupported/cyclic values fail closed rather than dropping mapped evidence.
type queueValueImage struct {
	typ      reflect.Type
	identity uintptr
	nilValue bool
	scalar   any
	children []*queueValueImage
	keys     []*queueValueImage
}

func captureQueueValue(v reflect.Value, depth int) (*queueValueImage, error) {
	return captureQueueValueForEncoding(v, depth, false)
}

// CSV binding formats consumed values with fmt.Sprint after the final journal
// check. Detect its callback interfaces statically; evidence never invokes them.
func captureQueueValueForEncoding(v reflect.Value, depth int, csv bool) (*queueValueImage, error) {
	if depth > 128 {
		return nil, fmt.Errorf("queue SQL value is cyclic or exceeds supported depth")
	}
	if !v.IsValid() {
		return &queueValueImage{}, nil
	}
	for _, callback := range []reflect.Type{reflect.TypeFor[driver.Valuer](), reflect.TypeFor[json.Marshaler](), reflect.TypeFor[encoding.TextMarshaler]()} {
		if v.Type().Implements(callback) || reflect.PointerTo(v.Type()).Implements(callback) {
			return nil, fmt.Errorf("queue SQL value callback %v requires explicit pure native mapping support", callback)
		}
	}
	if csv {
		for _, callback := range []reflect.Type{reflect.TypeFor[fmt.Stringer](), reflect.TypeFor[error](), reflect.TypeFor[fmt.Formatter]()} {
			if v.Type().Implements(callback) || reflect.PointerTo(v.Type()).Implements(callback) {
				return nil, fmt.Errorf("queue CSV value formatting callback %v requires explicit pure native mapping support", callback)
			}
		}
	}
	r := &queueValueImage{typ: v.Type()}
	child := func(value reflect.Value) error {
		n, e := captureQueueValueForEncoding(value, depth+1, csv)
		if e == nil {
			r.children = append(r.children, n)
		}
		return e
	}
	switch v.Kind() {
	case reflect.Bool:
		r.scalar = v.Bool()
	case reflect.String:
		r.scalar = v.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		r.scalar = v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		r.scalar = v.Uint()
	case reflect.Float32, reflect.Float64:
		r.scalar = math.Float64bits(v.Float())
	case reflect.Complex64, reflect.Complex128:
		r.scalar = v.Complex()
	case reflect.Pointer, reflect.Interface:
		r.nilValue = v.IsNil()
		if !r.nilValue {
			if v.Kind() == reflect.Pointer {
				r.identity = v.Pointer()
			}
			if e := child(v.Elem()); e != nil {
				return nil, e
			}
		}
	case reflect.Slice:
		r.nilValue = v.IsNil()
		r.identity = v.Pointer()
		for i := 0; i < v.Len(); i++ {
			if e := child(v.Index(i)); e != nil {
				return nil, e
			}
		}
	case reflect.Array, reflect.Struct:
		n := v.Len
		if v.Kind() == reflect.Struct {
			n = v.NumField
		}
		for i := 0; i < n(); i++ {
			value := reflect.Value{}
			if v.Kind() == reflect.Struct {
				value = v.Field(i)
			} else {
				value = v.Index(i)
			}
			if e := child(value); e != nil {
				return nil, e
			}
		}
	case reflect.Map:
		r.nilValue = v.IsNil()
		if !r.nilValue {
			r.identity = uintptr(v.UnsafePointer())
		}
		it := v.MapRange()
		for it.Next() {
			k, e := captureQueueValueForEncoding(it.Key(), depth+1, csv)
			if e != nil {
				return nil, e
			}
			r.keys = append(r.keys, k)
			if e = child(it.Value()); e != nil {
				return nil, e
			}
		}
	default:
		return nil, fmt.Errorf("queue SQL value kind %s is unsupported", v.Kind())
	}
	return r, nil
}

func (r *queueValueImage) equal(v reflect.Value, depth int) bool {
	if depth > 128 {
		return false
	}
	if r.typ == nil {
		return !v.IsValid()
	}
	if !v.IsValid() || v.Type() != r.typ {
		return false
	}
	switch v.Kind() {
	case reflect.Bool:
		return r.scalar == v.Bool()
	case reflect.String:
		return r.scalar == v.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return r.scalar == v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return r.scalar == v.Uint()
	case reflect.Float32, reflect.Float64:
		return r.scalar == math.Float64bits(v.Float())
	case reflect.Complex64, reflect.Complex128:
		return r.scalar == v.Complex()
	case reflect.Pointer, reflect.Interface:
		if r.nilValue != v.IsNil() {
			return false
		}
		if r.nilValue {
			return true
		}
		if v.Kind() == reflect.Pointer && r.identity != v.Pointer() {
			return false
		}
		return r.children[0].equal(v.Elem(), depth+1)
	case reflect.Slice:
		if r.nilValue != v.IsNil() || r.identity != v.Pointer() {
			return false
		}
		fallthrough
	case reflect.Array:
		if len(r.children) != v.Len() {
			return false
		}
		for i, c := range r.children {
			if !c.equal(v.Index(i), depth+1) {
				return false
			}
		}
	case reflect.Struct:
		for i, c := range r.children {
			if !c.equal(v.Field(i), depth+1) {
				return false
			}
		}
	case reflect.Map:
		if r.nilValue != v.IsNil() || len(r.keys) != v.Len() {
			return false
		}
		if r.nilValue {
			return true
		}
		if r.identity != uintptr(v.UnsafePointer()) {
			return false
		}
		matched := make([]bool, len(r.keys))
		it := v.MapRange()
		for it.Next() {
			found := false
			for i, k := range r.keys {
				if !matched[i] && k.equal(it.Key(), depth+1) {
					if !r.children[i].equal(it.Value(), depth+1) {
						return false
					}
					matched[i] = true
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	default:
		return false
	}
	return true
}
