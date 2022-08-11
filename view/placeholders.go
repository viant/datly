package view

import (
	"github.com/viant/xunsafe"
	"reflect"
)

type CriteriaSanitizer struct {
	Columns     ColumnIndex
	ParamsGroup [][]interface{}
	Mock        bool
}

func (p *CriteriaSanitizer) AsBinding(value interface{}) string {
	p.Add(0, value)
	return "?"
}

func (p *CriteriaSanitizer) AsColumn(columnName string) (string, error) {
	lookup, err := p.Columns.Lookup(columnName)
	if err != nil {
		return "", err
	}

	return lookup.Name, nil
}

func (p *CriteriaSanitizer) Add(at int, value interface{}) string {
	p.growIfNeeded(at)

	valueCopy := p.copy(value)

	p.ParamsGroup[at] = append(p.ParamsGroup[at], valueCopy)
	return "?"
}

func (p *CriteriaSanitizer) copy(value interface{}) interface{} {
	valueType := reflect.TypeOf(value)
	valueCopy := reflect.New(valueType).Elem().Interface()
	valuePtr := xunsafe.AsPointer(value)

	if valuePtr != nil {
		xunsafe.Copy(xunsafe.AsPointer(valueCopy), valuePtr, int(valueType.Size()))
	}

	return valueCopy
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
