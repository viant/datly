package generate

import (
	"fmt"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

// A linked package type is outside generated source ownership. CAST may agree
// with it, but cannot change its compiled fields or project a fictitious shape.
func (r *planResolver) validateLinkedViewCasts(view *spec.View, descriptor *x.Type) error {
	validator := &linkedViewCastValidator{plan: r.plan, shape: xshape.New(descriptor, r.types.Descriptor), visiting: map[*spec.View]bool{}}
	return validator.validate(view, "")
}

type linkedViewCastValidator struct {
	plan     *Plan
	shape    *xshape.Type
	visiting map[*spec.View]bool
}

// View relations supply authored holder paths; x/shape owns all traversal of
// the actual linked/synthetic type and its declaring package/import identity.
func (v *linkedViewCastValidator) validate(view *spec.View, prefix string) error {
	if view == nil || v.visiting[view] {
		return nil
	}
	v.visiting[view] = true
	defer delete(v.visiting, view)
	fields := resolveScalarViewFields(v.plan, view, false)
	imports := map[string]string{}
	for _, imported := range v.plan.Imports {
		imports[imported.Alias] = imported.Package
	}
	contract := xshape.Contract{Packages: xshape.Packages{Imports: imports}}
	for _, wanted := range fields {
		if !wanted.ExplicitType {
			continue
		}
		path := prefix + wanted.Name
		actual, err := v.shape.ResolveField(path)
		if err != nil {
			return fmt.Errorf("CAST field %s on uneditable linked type %s: %w", path, v.shape.Descriptor().Key(), err)
		}
		equivalent, err := contract.Equivalent(wanted.Type, actual.Identity)
		if err != nil {
			return err
		}
		if !equivalent {
			return fmt.Errorf("CAST cannot change uneditable linked field %s.%s: declared %s differs from linked field %s", v.shape.Descriptor().Key(), path, wanted.Type, actual.Identity)
		}
	}
	for _, relation := range view.Relations {
		if relation == nil {
			continue
		}
		holder := typecatalog.FieldName(relation.Holder)
		if holder == "" {
			holder = typecatalog.FieldName(relation.Name)
		}
		if err := v.validate(relation.View, prefix+holder+"."); err != nil {
			return err
		}
	}
	return nil
}
