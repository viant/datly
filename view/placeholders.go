package view

import (
	"fmt"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"unsafe"
)

type CriteriaSanitizer struct {
	Columns            ColumnIndex
	ParamsGroup        [][]interface{}
	Mock               bool
	GroupCounter       int
	PlaceholderCounter int
	sliceIndex         map[reflect.Type]*xunsafe.Slice
}

func (p *CriteriaSanitizer) AsBinding(value interface{}) string {
	return p.Add(0, value)
}

func (p *CriteriaSanitizer) AsColumn(columnName string) (string, error) {
	lookup, err := p.Columns.Lookup(columnName)
	if err != nil {
		return "", err
	}

	return lookup.Name, nil
}

func (p *CriteriaSanitizer) Add(at int, value interface{}) string {
	if value == nil {
		return ""
	}

	valueCopy, expanded := p.expandCopy(value)
	if valueCopy == nil {
		return ""
	}

	p.growIfNeeded(at)
	p.ParamsGroup[at] = append(p.ParamsGroup[at], valueCopy...)
	return expanded
}

func (p *CriteriaSanitizer) expandCopy(value interface{}) ([]interface{}, string) {
	valueType := reflect.TypeOf(value)
	valueCopy := reflect.New(valueType).Elem().Interface()
	valuePtr := xunsafe.AsPointer(value)

	if valueType.Kind() == reflect.Slice {
		return p.copyAndExpandSlice(valueType, valuePtr)
	}

	if valuePtr != nil {
		xunsafe.Copy(xunsafe.AsPointer(valueCopy), valuePtr, int(valueType.Size()))
	}

	return []interface{}{valueCopy}, "?"
}

func (p *CriteriaSanitizer) copyAndExpandSlice(valueType reflect.Type, valuePtr unsafe.Pointer) ([]interface{}, string) {
	p.ensureSliceIndex()
	xslice := p.xunsafeSlice(valueType)
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

func (p *CriteriaSanitizer) growIfNeeded(at int) {
	if len(p.ParamsGroup) > at {
		return
	}

	newParams := make([][]interface{}, at+1)
	for i, group := range p.ParamsGroup {
		newParams[i] = append(newParams[i], group...)
	}

	p.ParamsGroup = newParams
}

func (p *CriteriaSanitizer) At(i int) []interface{} {
	if len(p.ParamsGroup) <= i {
		return []interface{}{}
	}

	return p.ParamsGroup[i]
}

func (p *CriteriaSanitizer) Next() (interface{}, error) {
	if p.Mock {
		return 0, nil
	}

	for {
		if p.GroupCounter >= len(p.ParamsGroup) {
			return nil, fmt.Errorf("not found next binding variable")
		}

		if p.PlaceholderCounter < len(p.ParamsGroup[p.GroupCounter]) {
			index := p.PlaceholderCounter
			p.PlaceholderCounter++
			return p.ParamsGroup[p.GroupCounter][index], nil
		}

		p.GroupCounter++
		p.PlaceholderCounter = 0
	}
}

func (p *CriteriaSanitizer) ensureSliceIndex() {
	if p.sliceIndex != nil {
		return
	}
}

func (p *CriteriaSanitizer) xunsafeSlice(valueType reflect.Type) *xunsafe.Slice {
	slice, ok := p.sliceIndex[valueType]
	if !ok {
		slice = xunsafe.NewSlice(reflect.SliceOf(valueType))
		p.sliceIndex[valueType] = slice
	}

	return slice
}
