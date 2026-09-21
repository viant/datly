package report

import (
	"context"
	"fmt"
	"reflect"

	"github.com/viant/bindly/locator"
	"github.com/viant/structology"
)

type typedValue struct {
	typeOf reflect.Type
	value  any
	owns   bool
	found  bool
}

type typedValueProvider struct {
	kind   string
	values map[string]typedValue
}

type typedValueLocator struct {
	provider *typedValueProvider
}

func newTypedValueProvider(kind string, values map[string]typedValue) locator.Provider {
	return &typedValueProvider{kind: kind, values: values}
}

func (p *typedValueProvider) Kind() string  { return p.kind }
func (p *typedValueProvider) Priority() int { return 0 }
func (p *typedValueProvider) DefaultCacheable() bool {
	return true
}
func (p *typedValueProvider) Locate(*structology.State) locator.Locator {
	return &typedValueLocator{provider: p}
}

func (l *typedValueLocator) Kind() string {
	if l == nil || l.provider == nil {
		return ""
	}
	return l.provider.kind
}

func (l *typedValueLocator) Value(_ context.Context, targetType reflect.Type, name string) (any, bool, error) {
	if l == nil || l.provider == nil {
		return nil, false, nil
	}
	value, ok := l.provider.values[name]
	if !ok {
		return nil, false, nil
	}
	if targetType != value.typeOf {
		return nil, false, fmt.Errorf("report %s/%s requires source type %s, got %s", l.provider.kind, name, value.typeOf, targetType)
	}
	if !value.found {
		return nil, false, nil
	}
	return value.value, true, nil
}

func (l *typedValueLocator) Owns(name string) bool {
	if l == nil || l.provider == nil {
		return false
	}
	value, ok := l.provider.values[name]
	return ok && value.owns
}

func reportInput(input any, expected reflect.Type) (reflect.Value, error) {
	value := reflect.ValueOf(input)
	if value.Kind() != reflect.Pointer || value.IsNil() || value.Type().Elem() != expected {
		return reflect.Value{}, fmt.Errorf("report input must be non-nil *%s, got %T", expected, input)
	}
	return value.Elem(), nil
}

func fieldValue(root reflect.Value, index []int) (reflect.Value, bool) {
	value := root
	for _, fieldIndex := range index {
		value = indirect(value)
		if !value.IsValid() || value.Kind() != reflect.Struct || fieldIndex < 0 || fieldIndex >= value.NumField() {
			return reflect.Value{}, false
		}
		value = value.Field(fieldIndex)
	}
	if value.Kind() == reflect.Pointer && value.IsNil() {
		return reflect.Value{}, false
	}
	return value, value.IsValid()
}

func filterSourceValue(root reflect.Value, index []int, sourceType reflect.Type) (reflect.Value, bool, error) {
	value, present := fieldValue(root, index)
	if !present {
		return reflect.Value{}, false, nil
	}
	if value.Type() == sourceType {
		if nilable(value.Kind()) && value.IsNil() {
			return reflect.Value{}, false, nil
		}
		return value, true, nil
	}
	if value.Kind() == reflect.Pointer && value.Type().Elem() == sourceType {
		if value.IsNil() {
			return reflect.Value{}, false, nil
		}
		return value.Elem(), true, nil
	}
	return reflect.Value{}, false, fmt.Errorf("field type %s cannot provide source type %s", value.Type(), sourceType)
}

func nilable(kind reflect.Kind) bool {
	switch kind {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return true
	default:
		return false
	}
}

func indirect(value reflect.Value) reflect.Value {
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return reflect.Value{}
		}
		value = value.Elem()
	}
	return value
}

func stringSlice(root reflect.Value, index []int) ([]string, error) {
	value, present := fieldValue(root, index)
	if !present {
		return nil, nil
	}
	if value.Type() != reflect.TypeOf([]string{}) {
		return nil, fmt.Errorf("report order field must be []string, got %s", value.Type())
	}
	return append([]string(nil), value.Interface().([]string)...), nil
}

func optionalInt(root reflect.Value, index []int) (int, error) {
	value, present := fieldValue(root, index)
	if !present {
		return 0, nil
	}
	value = indirect(value)
	if !value.IsValid() || value.Kind() != reflect.Int {
		return 0, fmt.Errorf("report window field must be *int")
	}
	return int(value.Int()), nil
}
