package generic

import (
	"github.com/pkg/errors"
	"github.com/viant/toolbox"
	"reflect"
)

//Provider provides shares _proto data across all dynamic types
type Provider struct {
	*Proto
}

//NewArray creates a slice
func (p *Provider) NewArray(items ...interface{}) *Array {
	slice := &Array{_data: [][]interface{}{}, _proto: p.Proto}
	for _, items := range items {
		slice.Add(toolbox.AsMap(items))
	}
	return slice
}

//NewObject creates an object
func (p *Provider) NewObject() *Object {
	return &Object{_data: []interface{}{}, _proto: p.Proto}
}

//Object creates an object from struct or map
func (p *Provider) Object(value interface{}) (*Object, error) {
	result := &Object{_data: []interface{}{}, _proto: p.Proto}
	if toolbox.IsStruct(value) {
		return result, toolbox.ProcessStruct(value, func(fieldType reflect.StructField, field reflect.Value) error {
			result.SetValue(fieldType.Name, field.Interface())
			return nil
		})
	}
	if toolbox.IsMap(value) {
		toolbox.ProcessMap(value, func(key, value interface{}) bool {
			result.SetValue(toolbox.AsString(key), value)
			return true
		})
		return result, nil
	}
	return nil, errors.Errorf("unsupported object source: %T", value)
}

//NewMap creates a map of string and object
func (p *Provider) NewMap(index Index) *Map {
	return &Map{_map: map[string][]interface{}{}, _proto: p.Proto, index: index}
}

//NewMultimap creates a multimap of string and slice
func (p *Provider) NewMultimap(index Index) *Multimap {
	return &Multimap{_map: map[string][][]interface{}{}, _proto: p.Proto, index: index}
}

//NewProvider creates provider
func NewProvider(fields ...*Field) *Provider {
	return &Provider{Proto: newProto(fields...)}
}
