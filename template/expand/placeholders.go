package expand

import (
	"fmt"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"unsafe"
)

type (
	ColumnsSource interface {
		ColumnName(key string) (string, error)
	}

	CriteriaSanitizer struct {
		Columns            ColumnsSource
		ParamsGroup        []interface{}
		Mock               bool
		PlaceholderCounter int
		sliceIndex         map[reflect.Type]*xunsafe.Slice
		TemplateSQL        string
	}
)

func (p *CriteriaSanitizer) AsBinding(value interface{}) string {
	return p.Add(0, value)
}

func (p *CriteriaSanitizer) AppendBinding(value interface{}) string {
	return p.Add(0, value)
}

func (p *CriteriaSanitizer) AsColumn(columnName string) (string, error) {
	return p.Columns.ColumnName(columnName)
}

func (p *CriteriaSanitizer) Add(_ int, value interface{}) string {
	if value == nil {
		return ""
	}

	valueCopy, expanded := p.expandCopy(value)
	if valueCopy == nil {
		return ""
	}

	p.ParamsGroup = append(p.ParamsGroup, valueCopy...)
	return expanded
}

func (p *CriteriaSanitizer) expandCopy(value interface{}) ([]interface{}, string) {
	valueType := reflect.TypeOf(value)
	valuePtr := xunsafe.AsPointer(value)

	if valueType.Kind() == reflect.Slice {
		return p.copyAndExpandSlice(valueType, valuePtr)
	}

	valueCopy := reflect.New(valueType).Elem().Interface()
	if valuePtr != nil {
		xunsafe.Copy(xunsafe.AsPointer(valueCopy), valuePtr, int(valueType.Size()))
	}

	return []interface{}{valueCopy}, "?"
}

func (p *CriteriaSanitizer) copyAndExpandSlice(sliceType reflect.Type, valuePtr unsafe.Pointer) ([]interface{}, string) {
	p.ensureSliceIndex()
	xslice := p.xunsafeSlice(sliceType.Elem())
	sliceLen := xslice.Len(valuePtr)
	switch sliceLen {
	case 0:
		return nil, ""
	case 1:
		return []interface{}{xslice.ValueAt(valuePtr, 0)}, "?"
	default:
		builder := strings.Builder{}
		builder.WriteByte('?')
		placeholders := make([]interface{}, sliceLen)
		placeholders[0] = xslice.ValueAt(valuePtr, 0)

		for i := 1; i < sliceLen; i++ {
			builder.WriteString(", ?")
			placeholders[i] = xslice.ValueAt(valuePtr, i)
		}

		return placeholders, builder.String()
	}
}

func (p *CriteriaSanitizer) At(_ int) []interface{} {
	return p.ParamsGroup
}

func (p *CriteriaSanitizer) Next() (interface{}, error) {
	if p.Mock {
		return 0, nil
	}

	if p.PlaceholderCounter < len(p.ParamsGroup) {
		index := p.PlaceholderCounter
		p.PlaceholderCounter++
		return p.ParamsGroup[index], nil
	}

	return nil, fmt.Errorf("expected to got binding parameter, but noone was found")
}

func (p *CriteriaSanitizer) ensureSliceIndex() {
	if p.sliceIndex != nil {
		return
	}

	p.sliceIndex = map[reflect.Type]*xunsafe.Slice{}
}

func (p *CriteriaSanitizer) xunsafeSlice(valueType reflect.Type) *xunsafe.Slice {
	slice, ok := p.sliceIndex[valueType]
	if !ok {
		slice = xunsafe.NewSlice(reflect.SliceOf(valueType))
		p.sliceIndex[valueType] = slice
	}

	return slice
}

func (p *CriteriaSanitizer) Insert() (string, []interface{}) {
	return p.TemplateSQL, p.ParamsGroup
}

func (p *CriteriaSanitizer) addAll(args ...interface{}) {
	p.ParamsGroup = append(p.ParamsGroup, args...)
}
