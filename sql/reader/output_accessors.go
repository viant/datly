package reader

import (
	"fmt"
	"reflect"
	"slices"

	xshape "github.com/viant/x/shape"
)

// outputAccessors belongs to one prepared output container. It points at the
// existing relation plans; it is not an alternate view graph or field mapper.
type outputAccessors struct {
	fields  map[*RelationPlan]*xshape.Accessor
	metrics *xshape.Accessor
	status  *xshape.Accessor
	success any
}

func (p *Plan) compileOutputAccessors(outputType reflect.Type) (*outputAccessors, error) {
	result := &outputAccessors{fields: map[*RelationPlan]*xshape.Accessor{}}
	if p == nil || p.Root == nil || p.DirectOutput || outputType == nil {
		return result, nil
	}
	hasOutputs := false
	for _, relation := range p.Root.Relations {
		if relation != nil && relation.Relation != nil && relation.Relation.IsOutput() {
			hasOutputs = true
			break
		}
	}
	if !hasOutputs {
		return result, nil
	}
	owner := xshape.Linked(outputType)
	type slot struct {
		name  string
		index []int
	}
	var occupied []slot
	if p.OutputViewField != "" {
		field, err := owner.Accessor(p.OutputViewField)
		if err != nil {
			return nil, err
		}
		index, err := field.FieldIndex()
		if err != nil {
			return nil, err
		}
		occupied = append(occupied, slot{p.OutputViewField, index})
	}
	for _, relation := range p.Root.Relations {
		if relation == nil || relation.Relation == nil || !relation.Relation.IsOutput() {
			continue
		}
		field, err := owner.Accessor(relation.Relation.Holder)
		if err != nil {
			return nil, fmt.Errorf("output holder %s: %w", relation.Relation.Holder, err)
		}
		model := field.Type()
		if model.Kind() == reflect.Pointer {
			model = model.Elem()
		}
		if model.Kind() != reflect.Struct {
			return nil, fmt.Errorf("output holder %s must be a struct or pointer to struct", relation.Relation.Holder)
		}
		index, err := field.FieldIndex()
		if err != nil {
			return nil, err
		}
		for _, prior := range occupied {
			length := min(len(index), len(prior.index))
			if slices.Equal(index[:length], prior.index[:length]) {
				return nil, fmt.Errorf("output holder %s overlaps holder %s", relation.Relation.Holder, prior.name)
			}
		}
		occupied = append(occupied, slot{relation.Relation.Holder, index})
		result.fields[relation] = field
	}
	return result, nil
}
