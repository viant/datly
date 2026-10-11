package generate

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

// Representation references have no runtime binding edge. Retain them with
// the existing TypeFor anchor machinery and emit canonical package identities.
func (r *planResolver) resolveWireTypes() error {
	for _, param := range spec.EffectiveParameters(r.input.Component.Parameters) {
		if param == nil {
			continue
		}
		var descriptor *x.Type
		seen := map[string]bool{}
		for _, expression := range []string{strings.TrimSpace(param.WireTypeExpr), strings.TrimSpace(reflect.StructTag(param.Tag).Get("wireType"))} {
			if expression == "" || seen[expression] {
				continue
			}
			seen[expression] = true
			named, err := (xshape.Resolver{}).Named(expression)
			if err != nil || named != expression {
				return fmt.Errorf("parameter %s wireType %q requires a named non-pointer representation", param.Name, expression)
			}
			if r.lookup() == nil {
				return fmt.Errorf("parameter %s wireType %q requires type authority", param.Name, expression)
			}
			resolved, err := r.lookup()(expression)
			if err != nil {
				return fmt.Errorf("parameter %s wireType %q: %w", param.Name, expression, err)
			}
			if resolved == nil || resolved.Name == "" || resolved.PkgPath == "" || strings.HasPrefix(expression, "*") || resolved.Type != nil && resolved.Type.Kind() == reflect.Pointer {
				return fmt.Errorf("parameter %s wireType %q requires a named non-pointer representation", param.Name, expression)
			}
			if descriptor != nil && descriptor.Key() != resolved.Key() {
				return fmt.Errorf("parameter %s has conflicting wireType declarations", param.Name)
			}
			descriptor = resolved
		}
		if descriptor == nil {
			continue
		}
		param.WireTypeExpr = descriptor.PkgPath + "." + descriptor.Name
		r.plan.WireTypes = append(r.plan.WireTypes, linkedNamedTypeExpression(r.plan, r.input.TargetPackage, descriptor))
		for index := range r.plan.Input.Fields {
			if r.plan.Input.Fields[index].Name == generatedParameterName(param) {
				r.plan.Input.Fields[index].Tag = replaceStructTag(r.plan.Input.Fields[index].Tag, "wireType", param.WireTypeExpr)
			}
		}
	}
	if h := r.input.ExternalHandler; h != nil {
		for _, expression := range h.WireTypes {
			if r.lookup() == nil {
				return fmt.Errorf("external handler wireType %q requires type authority", expression)
			}
			descriptor, err := r.lookup()(expression)
			if err != nil {
				return err
			}
			if descriptor == nil || descriptor.Name == "" || descriptor.PkgPath == "" {
				return fmt.Errorf("external handler wireType %q requires named representation authority", expression)
			}
			anchor := linkedNamedTypeExpression(r.plan, r.input.TargetPackage, descriptor)
			found := false
			for _, existing := range r.plan.WireTypes {
				found = found || existing == anchor
			}
			if !found {
				r.plan.WireTypes = append(r.plan.WireTypes, anchor)
			}
		}
	}
	return nil
}
