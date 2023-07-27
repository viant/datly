package types

import "reflect"

func Traverse(any interface{}, visitor func(value reflect.Value) error) error {
	of := reflect.ValueOf(any)
	return traverse(of, visitor)
}

func traverse(of reflect.Value, visitor func(value reflect.Value) error) error {
	if err := visitor(of); err != nil {
		return err
	}

	switch of.Kind() {
	case reflect.Ptr:
		if of.IsNil() {
			return nil
		}

		return traverse(of.Elem(), visitor)

	case reflect.Slice, reflect.Array:
		size := of.Len()
		for i := 0; i < size; i++ {
			if err := traverse(of.Index(i), visitor); err != nil {
				return err
			}
		}

	case reflect.Map:
		iter := of.MapRange()
		for iter.Next() {
			iterValue := iter.Value()
			if err := traverse(iterValue, visitor); err != nil {
				return err
			}
		}

	case reflect.Struct:
		numFields := of.NumField()
		ofType := of.Type()
		for i := 0; i < numFields; i++ {
			aField := ofType.Field(i)
			if aField.PkgPath != "" {
				continue
			}

			if err := traverse(of.Field(i), visitor); err != nil {
				return err
			}
		}
	}

	return nil
}
