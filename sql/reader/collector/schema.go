package collector

import (
	"reflect"

	"github.com/viant/xunsafe"
)

// Schema is the reader-owned compiled row and slice access for one view.
type Schema struct {
	sliceType reflect.Type
	rowType   reflect.Type
	slice     *xunsafe.Slice
}

func NewSchema(rowType reflect.Type) Schema {
	if rowType == nil {
		return Schema{}
	}
	sliceType := reflect.SliceOf(rowType)
	return Schema{
		sliceType: sliceType,
		rowType:   rowType,
		slice:     xunsafe.NewSlice(sliceType),
	}
}

func (s *Schema) SliceType() reflect.Type { return s.sliceType }

func (s *Schema) RowType() reflect.Type { return s.rowType }

func (s *Schema) Slice() *xunsafe.Slice { return s.slice }
