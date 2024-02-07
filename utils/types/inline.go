package types

import "reflect"

func InlineStruct(p reflect.Type, onField func(f *reflect.StructField)) reflect.Type {
	isPtr := p.Kind() == reflect.Ptr
	rawType := p
	if isPtr {
		rawType = rawType.Elem()
	}
	if rawType.Kind() != reflect.Struct {
		return p
	}
	var fields = make([]reflect.StructField, rawType.NumField())
	for i := range fields {
		fields[i] = rawType.Field(i)
		if onField != nil {
			onField(&fields[i])
		}
	}
	result := reflect.StructOf(fields)
	if isPtr {
		result = reflect.PtrTo(result)
	}
	return result
}
