package gojay

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/generic"
	"github.com/viant/toolbox"
	"reflect"
)

type Slice struct {
	_data interface{}
}

func (s Slice) IsNil() bool {
	if s._data == nil {
		return true
	}
	var sliceLen = reflect.ValueOf(s._data).Len()
	return sliceLen == 0
}

func (s Slice) MarshalJSONArray(enc *gojay.Encoder) {
	toolbox.ProcessSlice(s._data, func(item interface{}) bool {
		if item != nil {
			if toolbox.IsStruct(item) && toolbox.IsPointer(item) {
				provider := generic.NewProvider()
				if object, err := provider.Object(item); err == nil {
					item = &Object{object}
				}
			}
		}
		enc.AddInterface(item)
		return true
	})
}

//NewSlice creates a slice
func NewSlice(data interface{}) *Slice {
	return &Slice{
		_data: data,
	}
}
