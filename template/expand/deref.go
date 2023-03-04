package expand

import (
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
)

var dereferencer = &Dereferencer{index: map[reflect.Type][]*xunsafe.Field{}}

type Dereferencer struct {
	mux   sync.Mutex
	index map[reflect.Type][]*xunsafe.Field
}

func (f *Dereferencer) Fields(rType reflect.Type) []*xunsafe.Field {
	fields, ok := f.index[rType]
	if ok {
		return fields
	}

	fieldsLen := rType.NumField()
	result := make([]*xunsafe.Field, 0, fieldsLen)

	for i := 0; i < fieldsLen; i++ {
		result = append(result, xunsafe.NewField(rType.Field(i)))
	}

	f.mux.Lock()
	defer f.mux.Unlock()
	f.index[rType] = result
	return result
}

func (f *Dereferencer) derefArgs(args ...interface{}) []interface{} {
	for i, arg := range args {
		args[i] = f.deref(arg)
	}
	return args
}

func (f *Dereferencer) deref(arg interface{}) interface{} {
	if arg == nil {
		return arg
	}

	switch actual := arg.(type) {
	case string:
		return actual
	case int:
		return actual
	case uint:
		return actual
	case int8:
		return actual
	case uint8:
		return actual
	case int16:
		return actual
	case uint16:
		return actual
	case int32:
		return actual
	case uint32:
		return actual
	case int64:
		return actual
	case uint64:
		return actual
	case bool:
		return actual

	case *string:
		if actual == nil {
			return ""
		}
		return *actual
	case *int:
		if actual == nil {
			return 0
		}
		return *actual
	case *uint:
		if actual == nil {
			return 0
		}
		return *actual
	case *int8:
		if actual == nil {
			return 0
		}
		return *actual
	case *uint8:
		if actual == nil {
			return 0
		}
		return *actual
	case *int16:
		if actual == nil {
			return 0
		}
		return *actual
	case *uint16:
		if actual == nil {
			return 0
		}
		return *actual
	case *int32:
		if actual == nil {
			return 0
		}
		return *actual
	case *uint32:
		if actual == nil {
			return 0
		}
		return *actual
	case *int64:
		if actual == nil {
			return 0
		}
		return *actual
	case *uint64:
		if actual == nil {
			return 0
		}
		return *actual
	case *bool:
		if actual == nil {
			return false
		}
		return *actual
	}

	rValue := reflect.ValueOf(arg)
	switch rValue.Kind() {
	case reflect.Ptr:
		if rValue.IsNil() || rValue.IsZero() {
			return f.deref(reflect.New(rValue.Elem().Type()).Elem().Interface())
		}

		return f.deref(rValue.Elem().Interface())

	case reflect.Slice:
		slice := xunsafe.NewSlice(rValue.Type())
		slicePtr := xunsafe.AsPointer(arg)
		sliceLen := slice.Len(slicePtr)

		result := make([]interface{}, 0, sliceLen)

		for i := 0; i < sliceLen; i++ {
			result = append(result, f.deref(slice.ValuePointerAt(slicePtr, i)))
		}
		return result

	case reflect.Struct:
		fields := f.Fields(rValue.Type())
		ptr := xunsafe.AsPointer(arg)

		result := map[string]interface{}{}
		for _, field := range fields {
			result[field.Name] = f.deref(field.ValuePointer(ptr))
		}

		return result
	}

	return arg
}
