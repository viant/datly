package state

import (
	"github.com/viant/xunsafe"
	"reflect"
)

type Schema struct {
	Package     string `json:",omitempty" yaml:"package,omitempty"`
	Name        string `json:",omitempty" yaml:"name,omitempty"`
	DataType    string `json:",omitempty" yaml:"dataType,omitempty"`
	Cardinality Cardinality
	Methods     []reflect.Method
	compType    reflect.Type
	sliceType   reflect.Type
	slice       *xunsafe.Slice
	xType       *xunsafe.Type
	autoGen     bool
	initialized bool
}
