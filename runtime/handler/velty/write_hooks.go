package velty

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
)

// WriteHooks adapts generated optional entity/relation hooks to one invocation.
// Static paths are resolved by x/shape; indexes retain actual slice addresses.
type WriteHooks struct {
	ctx   context.Context
	input any
}
type hookPathKey struct {
	owner reflect.Type
	path  string
}

var hookPaths sync.Map

func (w *WriteHooks) record(path string, indexes []int) (any, bool, error) {
	if w == nil || w.input == nil {
		return nil, false, fmt.Errorf("write hook input is not configured")
	}
	key := hookPathKey{owner: reflect.TypeOf(w.input), path: path}
	compiled, ok := hookPaths.Load(key)
	if !ok {
		accessor, err := xshape.Linked(key.owner).IndexedAccessor(path)
		if err != nil {
			return nil, false, err
		}
		compiled, _ = hookPaths.LoadOrStore(key, accessor)
	}
	value, err := compiled.(*xshape.Accessor).GetAt(w.input, indexes...)
	if err != nil {
		return nil, false, err
	}
	if !value.IsValid() {
		return nil, false, nil
	}
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, false, nil
		}
		if value.Elem().Kind() != reflect.Pointer {
			return value.Interface(), true, nil
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return nil, false, fmt.Errorf("write hook path %s does not address a record", path)
	}
	if !value.CanAddr() {
		return nil, false, fmt.Errorf("write hook record %s is not addressable", path)
	}
	return value.Addr().Interface(), true, nil
}

func (w *WriteHooks) Present(path string, indexes ...int) (bool, error) {
	_, present, err := w.record(path, indexes)
	return present, err
}

func (w *WriteHooks) Value(path string, indexes ...int) (any, error) {
	value, present, err := w.record(path, indexes)
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, fmt.Errorf("write hook record %s is nil", path)
	}
	return value, nil
}

func (w *WriteHooks) Entity(path string, indexes ...int) (string, error) {
	value, present, err := w.record(path, indexes)
	if err != nil || !present {
		return "", err
	}
	if hook, ok := value.(xhandler.WriteInitializer); ok {
		if err = hook.InitWrite(w.ctx); err != nil {
			return "", err
		}
	}
	if hook, ok := value.(xhandler.WriteValidator); ok {
		if err = hook.ValidateWrite(w.ctx); err != nil {
			return "", err
		}
	}
	return "", nil
}

func (w *WriteHooks) Relation(path, holderName string, indexes ...int) (string, error) {
	value, present, err := w.record(path, indexes)
	if err != nil || !present {
		return "", err
	}
	accessor, err := xshape.Linked(reflect.TypeOf(value)).Accessor(holderName)
	if err != nil {
		return "", err
	}
	holder, err := accessor.Get(value)
	if err != nil {
		return "", err
	}
	method := reflect.ValueOf(value).MethodByName("Before" + holderName + "Write")
	if !method.IsValid() {
		return "", nil
	}
	methodType := method.Type()
	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	// Match exactly the optional interface emitted by the Go lowerer.
	if methodType.NumIn() != 2 || methodType.In(0) != ctxType || methodType.In(1) != holder.Type() || methodType.NumOut() != 1 || methodType.Out(0) != errorType {
		return "", nil
	}
	result := method.Call([]reflect.Value{reflect.ValueOf(w.ctx), holder})
	if !result[0].IsNil() {
		return "", result[0].Interface().(error)
	}
	return "", nil
}

func (w *WriteHooks) Mark(path, field string, indexes ...int) (string, error) {
	value, present, err := w.record(path, indexes)
	if err != nil || !present {
		return "", err
	}
	accessor, err := xshape.Linked(reflect.TypeOf(value)).Accessor("Has." + field)
	if err != nil {
		return "", err
	}
	return "", accessor.Set(value, true)
}

// Link preserves the exact parent field address when a relation key changes
// pointer cardinality. Both records come from Value, never foreach copies.
func (w *WriteHooks) Link(child, parent any, childField, parentField string) (string, error) {
	target, err := xshape.Linked(reflect.TypeOf(child)).Accessor(childField)
	if err != nil {
		return "", err
	}
	source, err := xshape.Linked(reflect.TypeOf(parent)).Accessor(parentField)
	if err != nil {
		return "", err
	}
	value, err := source.Get(parent)
	if err != nil {
		return "", err
	}
	wanted := target.Type()
	if value.Type() != wanted {
		switch {
		case wanted.Kind() == reflect.Pointer && wanted.Elem() == value.Type() && value.CanAddr():
			value = value.Addr()
		case value.Kind() == reflect.Pointer && value.Type().Elem() == wanted:
			if value.IsNil() {
				return "", fmt.Errorf("missing parent key %s for relation field %s", parentField, childField)
			}
			value = value.Elem()
		default:
			return "", fmt.Errorf("unsupported relation key conversion %s to %s", value.Type(), wanted)
		}
	}
	return "", target.Set(child, value.Interface())
}

func (w *WriteHooks) Require(value any, field, relation string) (string, error) {
	missing := value == nil
	if !missing {
		actual := reflect.ValueOf(value)
		missing = actual.Kind() == reflect.Pointer && actual.IsNil()
	}
	if missing {
		return "", fmt.Errorf("missing parent key %s for relation %s", field, relation)
	}
	return "", nil
}
