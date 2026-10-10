package transcribe

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
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

// Qualified output_type explicitly selects an existing package contract, just
// as a qualified output/view selects existing rows. Bare names remain generated.
func linkDeclaredOutputContract(result *Result, authored *spec.Component) error {
	if result == nil || result.TypeResolver == nil || authored == nil || authored.Settings == nil || result.Source != nil && result.Source.PackageComponent != nil {
		return nil
	}
	expression := strings.TrimSpace(authored.Settings.OutputType)
	if !strings.Contains(expression, ".") {
		return nil
	}
	descriptor, err := result.TypeResolver.Descriptor(expression)
	if err != nil {
		return fmt.Errorf("resolve output contract %q: %w", expression, err)
	}
	// An unresolved qualified name may still declare a generated contract in
	// another destination package. Preserve that existing generation path.
	if descriptor == nil {
		return nil
	}
	if descriptor.Name == "" || descriptor.PkgPath == "" {
		return fmt.Errorf("output contract %q requires existing named Go type authority", expression)
	}
	shape := xshape.New(descriptor, result.TypeResolver.Descriptor)
	fields, err := shape.Fields()
	if err != nil {
		return fmt.Errorf("output contract %q must be a Go struct: %w", expression, err)
	}
	for _, param := range spec.EffectiveParameters(result.Component.Parameters) {
		if param == nil || (!strings.EqualFold(param.Source.Kind, "output") && !param.EmitOutput) {
			continue
		}
		field, err := shape.ResolveField(typecatalog.ExportedFieldName(param.Name))
		if err != nil {
			return fmt.Errorf("output contract %q field %s: %w", expression, param.Name, err)
		}
		if strings.EqualFold(param.Source.Kind, "output") {
			var declaredField xshape.Field
			for _, candidate := range fields {
				if candidate.Name == typecatalog.ExportedFieldName(param.Name) {
					declaredField = candidate
					break
				}
			}
			metadata, err := dtag.ParseField(declaredField.StructField())
			if err != nil {
				return fmt.Errorf("output contract %q field %s: %w", expression, param.Name, err)
			}
			if metadata.Binding == nil || !strings.EqualFold(metadata.Binding.Location.Kind, param.Source.Kind) || !strings.EqualFold(metadata.Binding.Location.In, param.Source.Name) {
				return fmt.Errorf("output contract %q field %s has incompatible binding metadata", expression, param.Name)
			}
		}
		wanted := strings.TrimSpace(param.OutputTypeExpr)
		if wanted == "" {
			wanted = strings.TrimSpace(param.TypeExpr)
		}
		if wanted != "" && wanted != "?" {
			canonical, err := result.TypeResolver.CanonicalDeclaration(wanted, descriptor.PkgPath)
			if err != nil {
				return fmt.Errorf("output contract %q field %s: %w", expression, param.Name, err)
			}
			if canonical != field.Identity {
				return fmt.Errorf("output contract %q field %s: declared %s differs from linked field %s", expression, param.Name, canonical, field.Identity)
			}
		}
	}
	if prior := result.Contracts.Output; prior != nil && prior.DescriptorKey != descriptor.Key() {
		return fmt.Errorf("output contract has conflicting package type authorities")
	}
	result.Contracts.Output = &gen.ContractReference{Expression: expression, DescriptorKey: descriptor.Key()}
	return nil
}
