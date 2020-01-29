package data

import (
	"github.com/viant/datly/generic"
)

//Value represents visitor value
type Value struct {
	*generic.Object
	Prev *generic.Object
}

//NewValue creates a value
func NewValue(object, prev *generic.Object) *Value {
	return &Value{
		Object: object,
		Prev:   prev,
	}
}
