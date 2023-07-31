package state

import (
	"github.com/viant/xunsafe"
	"reflect"
)

type (
	Schema struct {
		Package     string `json:",omitempty" yaml:"package,omitempty"`
		Name        string `json:",omitempty" yaml:"name,omitempty"`
		DataType    string `json:",omitempty" yaml:"dataType,omitempty"`
		Cardinality Cardinality
		Methods     []reflect.Method
		compType    reflect.Type
		sliceType   reflect.Type
		slice       *xunsafe.Slice
		xType       *xunsafe.Type
		initialized bool
	}

	GetSchema func() (*Schema, error)
)

func (s *Schema) IsInitialized() bool {
	return s.initialized
}

func (s *Schema) SetInitialized(flag bool) {
	s.initialized = flag
}

func (s *Schema) Clone() *Schema {
	ret := *s
	return &ret
}

func (s *Schema) Type() reflect.Type {
	return s.compType
}

func NewSchema(compType reflect.Type) *Schema {
	result := &Schema{initialized: true}
	result.SetType(compType)
	return result
}

func (s *Schema) SetType(rType reflect.Type) {
	if rType.Kind() == reflect.Slice { //i.e []int
		s.Cardinality = CardinalityMany
	}
	if s.Cardinality == "" {
		s.Cardinality = CardinalityOne
	}
	if s.Cardinality == CardinalityMany && rType.Kind() != reflect.Slice {
		rType = reflect.SliceOf(rType)
	}
	s.compType = rType
	s.updateSliceType()
}

func (s *Schema) updateSliceType() {
	s.slice = xunsafe.NewSlice(s.compType)
	s.sliceType = s.slice.Type
}
