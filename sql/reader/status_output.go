package reader

import (
	"fmt"
	"reflect"

	"github.com/viant/x/shape"
	"github.com/viant/xdatly/response"
)

func (a *outputAccessors) compileStatus(name string, output reflect.Type) error {
	if name == "" {
		return nil
	}
	owner := shape.Linked(output)
	field, err := owner.Accessor(name)
	if err != nil {
		return err
	}
	typ := field.Type()
	if typ == reflect.TypeFor[response.Status]() || typ == reflect.TypeFor[*response.Status]() {
		// Set only the status member, preserving messages and other metadata.
		field, err = owner.Accessor(name + ".Status")
		if err != nil {
			return err
		}
		typ = field.Type()
	}
	if typ.Kind() != reflect.String {
		return fmt.Errorf("status output %s must be a string or response.Status", name)
	}
	a.status = field
	a.success = reflect.ValueOf("ok").Convert(typ).Interface()
	return nil
}

func (a *outputAccessors) writeSuccess(output any) error {
	if a == nil || a.status == nil || output == nil {
		return nil
	}
	return a.status.Set(output, a.success)
}
