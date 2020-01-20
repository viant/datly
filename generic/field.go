package generic

import (
	"github.com/viant/toolbox"
	"reflect"
)

//Field represents dynamic filed
type Field struct {
	Name  string
	Type  reflect.Type
	provider *Provider
	index int
	formattedName string
	hidden bool
}

//Init sets a field value
func (f *Field) Set(value interface{}, result *[]interface{})  {
	if value != nil {
		if toolbox.IsSlice(value) {
			slice := toolbox.AsSlice(value)
			if len(slice) > 0 && toolbox.IsMap(slice[0]) {
				value = f.provider.NewSlice(slice...)
			}

		} else if toolbox.IsMap(value) {
			object := f.provider.NewObject()
			object.Init(toolbox.AsMap(value))
		}
	}
	values := *result
	values = reallocateIfNeeded(f.index + 1,  values)
	values[f.index] = value
	*result = values
}


//Get returns field value
func (f *Field) Get(values []interface{})  interface{} {
	if f.index <  len(values) {
		return values[f.index]
	}
	return nil
}
