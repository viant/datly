package generate

import (
	"strings"

	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
)

// An auxiliary canonical Input lifecycle owns its SQL-derived public body;
// independent reads may project persisted fields into that row without owning
// its application-only fields or Has type. Physical writers keep their separate
// admission rules. Final mutation/lifecycle validation still owns hook linking.
func (r *planResolver) auxiliaryBodyRoot() *spec.View {
	c := r.input.Component
	if c == nil || c.RootView == nil || r.input.ExternalHandler != nil || r.input.VeltyHandler != nil || r.input.GoHandler != nil || r.input.ContractHandler != nil || r.input.Views[RootViewPath] != nil {
		return nil
	}
	root := c.RootView
	if root.InMemory || root.Source == nil || len(root.Relations) != 0 || root.SelfReference != nil || strings.TrimSpace(root.EntityHooks) == "" || strings.TrimSpace(root.TypeName) == "" {
		return nil
	}
	if !root.Auxiliary && (r.requireConcreteHelpers || !pendingAuxiliarySource(root)) {
		return nil
	}
	if r.requireConcreteHelpers && !r.input.nativeMutationTargetSelected {
		return nil
	}
	canonical, err := r.plan.CanonicalType(r.plan.Package, root.TypeName)
	if err != nil {
		return nil
	}
	planned, err := r.plan.CanonicalType(r.plan.Package, r.plan.RootViewType)
	if err != nil || canonical != planned {
		return nil
	}
	matched := false
	for _, param := range preferDefinedParams(c.Parameters) {
		if param == nil {
			continue
		}
		kind := strings.ToLower(strings.TrimSpace(param.Source.Kind))
		if kind != "body" && !(kind == "output" && param.Source.Name == "body") {
			continue
		}
		expression := strings.TrimSpace(param.TypeExpr)
		if expression == "" || expression == "?" {
			continue
		}
		actual, err := r.plan.CanonicalType(r.plan.Package, expression)
		if err != nil {
			return nil
		}
		named, err := (xshape.Resolver{}).Named(actual)
		if err != nil || named != canonical {
			return nil
		}
		matched = true
	}
	if !matched {
		return nil
	}
	return root
}
