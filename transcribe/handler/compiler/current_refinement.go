package compiler

import (
	"fmt"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	xshape "github.com/viant/x/shape"
)

// RefineCurrentField incorporates final canonical Go type identities into one
// declared projection. It does not infer whether a runtime query loaded it.
func (c *Compiler) RefineCurrentField(field plan.CurrentField, currentType, entityType string) (plan.CurrentField, error) {
	current, err := (xshape.Resolver{}).Canonical(currentType)
	if err != nil {
		return plan.CurrentField{}, err
	}
	entity, err := (xshape.Resolver{}).Canonical(entityType)
	if err != nil {
		return plan.CurrentField{}, err
	}
	contract := xshape.Contract{}
	for _, candidate := range []struct {
		from, to   string
		conversion plan.LinkConversion
	}{
		{current, entity, plan.LinkDirect},
		{"*" + current, entity, plan.LinkAddress},
		{current, "*" + entity, plan.LinkDereference},
	} {
		equivalent, err := contract.Equivalent(candidate.from, candidate.to)
		if err != nil {
			return plan.CurrentField{}, err
		}
		if equivalent {
			field.Current.Type = spec.TypeRef{Name: current}
			field.Entity.Type = spec.TypeRef{Name: entity}
			field.Conversion = candidate.conversion
			return field, nil
		}
	}
	return plan.CurrentField{}, fmt.Errorf("current field %s type %s cannot project into entity field %s type %s", field.Current.Field, current, field.Entity.Field, entity)
}
