// Package readmeta carries immutable evidence from actual typed SQL reads.
// It contains no row values, SQL execution, or binding policy.
package readmeta

import (
	"reflect"
	"slices"

	xshape "github.com/viant/x/shape"
)

// Fields records native mapped destination indexes for one actual read schema.
// Nil means unknown, not all fields. Loaded SQL NULL is still a loaded field.
type Fields struct {
	rowType reflect.Type
	indexes [][]int
}

func NewFields(rowType reflect.Type, indexes [][]int) *Fields {
	rowType = (xshape.Runtime{}).Indirect(rowType)
	if rowType == nil || rowType.Kind() != reflect.Struct {
		return nil
	}
	result := &Fields{rowType: rowType, indexes: make([][]int, len(indexes))}
	for i, index := range indexes {
		result.indexes[i] = append([]int(nil), index...)
	}
	return result
}

// Has resolves a Go selector through native shape, preserving promoted aliases
// without interpreting SQL tags or comparing scalar zero values.
func (f *Fields) Has(name string) bool {
	if f == nil || f.rowType == nil {
		return false
	}
	field, err := xshape.Linked(f.rowType).StructField(name)
	if err != nil || !field.IsExported() {
		return false
	}
	for _, index := range f.indexes {
		if slices.Equal(index, field.Index) {
			return true
		}
	}
	return false
}

func (f *Fields) Known() bool { return f != nil }
