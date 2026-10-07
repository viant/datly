package transcribe

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	xshape "github.com/viant/x/shape"
)

// A qualified output row type explicitly selects an existing Go row contract.
// Its reachable row graph is linked authority, rather than a new generated shape.
// Unqualified names remain declarations of generated rows.
func linkDeclaredOutputView(result *Result, authored *spec.Component) error {
	if result == nil || result.Component == nil || result.Component.RootView == nil || result.TypeResolver == nil || authored == nil {
		return nil
	}
	for _, param := range spec.EffectiveParameters(authored.Parameters) {
		if param == nil || !strings.EqualFold(param.Source.Kind, "output") || (!strings.EqualFold(param.Source.Name, "view") && !param.IsDerivedOutput()) {
			continue
		}
		expression := strings.TrimSpace(param.OutputTypeExpr)
		if expression == "" {
			expression = strings.TrimSpace(param.TypeExpr)
		}
		if !strings.Contains(expression, ".") {
			continue
		}
		descriptor, err := result.TypeResolver.Descriptor(expression)
		if err != nil {
			return fmt.Errorf("resolve output/view row type %q: %w", expression, err)
		}
		if descriptor == nil || descriptor.Name == "" || descriptor.PkgPath == "" {
			return fmt.Errorf("output/view row type %q requires existing named Go type authority", expression)
		}
		if _, err := xshape.New(descriptor, result.TypeResolver.Descriptor).Fields(); err != nil {
			return fmt.Errorf("output/view row type %q must be a Go struct: %w", expression, err)
		}
		resolved, err := result.TypeResolver.ResolveShape(expression)
		if err != nil {
			return err
		}
		// Persist canonical type identity in binding metadata. The authored alias
		// need not appear in emitted Go when the row belongs to this package.
		for _, compiled := range result.Component.Parameters {
			if compiled != nil && compiled.Identity() == param.Identity() {
				if param.OutputTypeExpr != "" {
					compiled.OutputTypeExpr = resolved.Identity
				} else {
					compiled.TypeExpr = resolved.Identity
				}
			}
		}
		identity := gen.RootViewPath
		if param.IsDerivedOutput() {
			var view *spec.View
			for _, relation := range result.Component.RootView.Relations {
				if relation != nil && strings.EqualFold(relation.Name, param.Name) {
					view = relation.View
				}
			}
			if view == nil {
				return fmt.Errorf("derived output %q has no canonical view", param.Name)
			}
			identity, err = view.Identity()
			if err != nil {
				return err
			}
		}
		if result.Views == nil {
			result.Views = gen.ViewReferences{}
		}
		if prior := result.Views[identity]; prior != nil && prior.DescriptorKey != descriptor.Key() {
			return fmt.Errorf("output/view has conflicting row type authorities")
		}
		result.Views[identity] = &gen.ViewReference{DescriptorKey: descriptor.Key()}
	}
	return nil
}
