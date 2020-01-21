package generic

import "github.com/viant/toolbox"

//Provider provides shares _proto data across all dynamic types
type Provider struct {
	Proto
}

//NewSlice creates a slice
func (p *Provider) NewSlice(items ...interface{}) *Slice {
	slice := &Slice{_data: [][]interface{}{}, _proto: &p.Proto}
	for _, items := range items {
		slice.Add(toolbox.AsMap(items))
	}
	return slice
}

//NewObject creates an object
func (p *Provider) NewObject() *Object {
	return &Object{_data: []interface{}{}, _proto: &p.Proto}
}

//NewMap creates a map of string and object
func (p *Provider) NewMap(index Index) *Map {
	return &Map{_map: map[string][]interface{}{}, _proto: &p.Proto, index: index}
}

//NewMultimap creates a multimap of string and slice
func (p *Provider) NewMultimap(index Index) *Multimap {
	return &Multimap{_map: map[string][][]interface{}{}, _proto: &p.Proto, index: index}
}

//NewProvider creates provider
func NewProvider() *Provider {
	return &Provider{Proto: *newProto()}
}
