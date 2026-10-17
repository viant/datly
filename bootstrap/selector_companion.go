package bootstrap

import (
	"fmt"

	"github.com/viant/datly/internal/selectorcompanion"
	selectorModel "github.com/viant/datly/internal/selectorcompanion/model"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

func (s *RouteSource) selectorAssociation(types *typecatalog.Resolver) (*selectorModel.Association, error) {
	var holder *x.Type
	var err error
	var lookup xshape.Lookup
	if types != nil {
		lookup = types.Descriptor
	}
	if s.linkedHolderType != nil {
		holder = x.NewType(s.linkedHolderType)
	} else if lookup != nil {
		holder, err = lookup(s.PackagePath + "." + s.HolderType)
		if err != nil {
			return nil, err
		}
	}
	if holder == nil {
		return nil, nil
	}
	companion, err := selectorcompanion.Resolve(holder, s.FieldName, lookup)
	if err != nil {
		return nil, fmt.Errorf("component %s selector metadata: %w", s.componentName(), err)
	}
	if companion == nil {
		return nil, nil
	}
	return selectorModel.New(holder, s.FieldName, companion.Descriptor)
}

// Revalidate the immutable association against the actual selected contracts.
// Merely supplying an association never authorizes missing transport fields.
func checkedSelectorContract(component *spec.Component, input, output *x.Type, types *typecatalog.Resolver) (*selectorcompanion.Contract, error) {
	var lookup xshape.Lookup
	if types != nil {
		lookup = types.Descriptor
	}
	return selectorcompanion.CheckContracts(component, input, output, lookup)
}
