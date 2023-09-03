package json

import (
	"fmt"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
)

type presenceUpdater struct {
	xField *xunsafe.Field
	fields map[string]*xunsafe.Field
}

func newPresenceUpdater(field reflect.StructField) (*presenceUpdater, error) {
	presenceFields, err := getFields(field.Type)
	if err != nil {
		return nil, err
	}

	presenceFieldsIndex := map[string]*xunsafe.Field{}
	for i, presenceField := range presenceFields {
		presenceFieldsIndex[presenceField.Name] = presenceFields[i]
	}

	iUpdater := &presenceUpdater{
		xField: xunsafe.NewField(field),
		fields: presenceFieldsIndex,
	}
	return iUpdater, nil
}

func getFields(rType reflect.Type) ([]*xunsafe.Field, error) {
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	if rType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("index has to be type of struct")
	}

	numField := rType.NumField()
	result := make([]*xunsafe.Field, 0, numField)
	for i := 0; i < numField; i++ {
		aField := rType.Field(i)
		if aField.Type != xreflect.BoolType {
			continue
		}

		result = append(result, xunsafe.NewField(aField))
	}
	return result, nil
}
