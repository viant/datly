package expand

import (
	"github.com/viant/xunsafe"
	"reflect"
)

func NewValue(p reflect.Type) interface{} {
	if p.Kind() == reflect.Ptr {
		return reflect.New(p.Elem()).Interface()
	}

	result := reflect.New(p)
	if p.Kind() == reflect.Slice {
		return result.Elem().Interface()
	}

	ptr := xunsafe.ValuePointer(&result)
	//initialise pointers
	//if struct has one filed, go returns value of the first _field, if pointer it would return nil
	//to workaround we initialise value of the struct
	for i := 0; i < p.NumField(); i++ {
		field := p.Field(i)
		if field.Type.Kind() == reflect.Ptr {
			newValue := reflect.New(field.Type.Elem()).Interface()
			xField := xunsafe.NewField(field)
			xField.SetValue(ptr, newValue)
		}
	}
	ret := result.Elem().Interface()
	return ret
}
