package metadata

import (
	"github.com/viant/gtly"
)

//Value represents visitor value
type Value struct {
	*gtly.Object
	Prev *gtly.Object
}

//NewValue creates a value
func NewValue(object, prev *gtly.Object) *Value {
	return &Value{
		Object: object,
		Prev:   prev,
	}
}
