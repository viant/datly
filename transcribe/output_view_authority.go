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
// An exact explicit local SQL declaration may retain generated ownership;
// qualification alone still selects existing Go authority.
func linkDeclaredOutputView(result *Result, authored *spec.Component, generatedRows *generatedRowAuthority) error {
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
		view := result.Component.RootView
		identity := gen.RootViewPath
		if param.IsDerivedOutput() {
			view = nil
			for _, relation := range result.Component.RootView.Relations {
				if relation != nil && relation.Kind == spec.RelationKindDerived && strings.EqualFold(relation.Name, param.Name) {
					if view != nil {
						return fmt.Errorf("derived output %q has ambiguous canonical views", param.Name)
					}
					view = relation.View
				}
			}
			if view == nil {
				return fmt.Errorf("derived output %q has no canonical view", param.Name)
			}
			var err error
			identity, err = view.Identity()
			if err != nil {
				return err
			}
		}
		if result.Views[identity] == nil {
			generated, err := generatedOutputRow(result, generatedRows, view, expression)
			if err != nil {
				return err
			}
			if generated {
				canonical, err := generatedRows.plan.CanonicalType(generatedRows.plan.Package, expression)
				if err != nil {
					return err
				}
				// Native derived outputs have one row, held as T or *T. Check
				// canonical wrappers so equivalent whitespace cannot change arity.
				if param.IsDerivedOutput() {
					row, err := generatedRows.plan.CanonicalType(generatedRows.plan.Package, view.TypeName)
					if err != nil {
						return err
					}
					if canonical != row && canonical != "*"+row {
						return fmt.Errorf("SQL-owned derived output %q requires a single row or single pointer", param.Name)
					}
				}
				for _, compiled := range result.Component.Parameters {
					if compiled != nil && compiled.Identity() == param.Identity() {
						if param.OutputTypeExpr != "" {
							compiled.OutputTypeExpr = canonical
						} else {
							compiled.TypeExpr = canonical
						}
					}
				}
				if result.generatedOutputRows == nil {
					result.generatedOutputRows = map[*spec.View]bool{}
				}
				result.generatedOutputRows[view] = true
				continue
			}
		}
		resolved, err := result.TypeResolver.ResolveShape(expression)
		if err != nil {
			return fmt.Errorf("resolve output/view row type %q: %w", expression, err)
		}
		if resolved == nil || resolved.Descriptor == nil || resolved.Descriptor.Name == "" || resolved.Descriptor.PkgPath == "" {
			return fmt.Errorf("output/view row type %q requires existing named Go type authority", expression)
		}
		descriptor := resolved.Descriptor
		if _, err := xshape.New(descriptor, result.TypeResolver.Descriptor).Fields(); err != nil {
			return fmt.Errorf("output/view row type %q must be a Go struct: %w", expression, err)
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
		if result.Views == nil {
			result.Views = gen.ViewReferences{}
		}
		if prior := result.Views[identity]; prior != nil {
			if prior.DescriptorKey != descriptor.Key() {
				return fmt.Errorf("output/view has conflicting row type authorities")
			}
			// Retain existing linked/borrowed authority and its receipts.
			continue
		}
		result.Views[identity] = &gen.ViewReference{DescriptorKey: descriptor.Key()}
	}
	return nil
}

func generatedOutputRow(result *Result, authority *generatedRowAuthority, view *spec.View, expression string) (bool, error) {
	if authority == nil || authority.plan == nil || view == nil {
		return false, nil
	}
	canonical, err := authority.plan.CanonicalType(authority.plan.Package, expression)
	if err != nil {
		return false, err
	}
	base := canonical
	for strings.HasPrefix(base, "[]") || strings.HasPrefix(base, "*") {
		if strings.HasPrefix(base, "[]") {
			base = base[2:]
		} else {
			base = base[1:]
		}
	}
	owners := authority.owners[base]
	if len(owners) != 1 {
		return false, nil
	}
	ownerIdentity, err := owners[0].Identity()
	if err != nil {
		return false, err
	}
	viewIdentity, err := view.Identity()
	if err != nil {
		return false, err
	}
	if ownerIdentity != viewIdentity {
		return false, nil
	}
	resolved, err := result.TypeResolver.ResolveWithProvenance(expression)
	if err != nil {
		return false, err
	}
	if resolved == nil {
		return true, nil
	}
	return resolved.Origin == typecatalog.TypeOriginGenerated, nil
}

// Explicit qualified related-row declarations can reuse an existing row inside
// a generated parent. An unresolved declaration retains generated ownership.
func linkDeclaredRelatedViews(result *Result, generatedRows *generatedRowAuthority) error {
	if result == nil || result.Component == nil || result.TypeResolver == nil || result.Views[gen.RootViewPath] != nil {
		return nil
	}
	visited := map[*spec.View]bool{}
	var visit func(*spec.View) error
	visit = func(view *spec.View) error {
		if view == nil || visited[view] {
			return nil
		}
		visited[view] = true
		for _, relation := range view.Relations {
			if relation == nil || relation.View == nil {
				continue
			}
			child := relation.View
			if result.generatedOutputRows[child] {
				identity, err := child.Identity()
				if err != nil {
					return err
				}
				if result.Views[identity] == nil {
					if err := visit(child); err != nil {
						return err
					}
					continue
				}
			}
			expression := strings.TrimSpace(child.TypeName)
			if strings.Contains(expression, ".") {
				descriptor, err := result.TypeResolver.Descriptor(expression)
				if err != nil {
					return fmt.Errorf("resolve related row type %q: %w", expression, err)
				}
				if descriptor != nil {
					identity, err := child.Identity()
					if err != nil {
						return err
					}
					if result.Views[identity] == nil {
						// Reuse exact local SQL ownership and selected generated provenance.
						// Qualified names alone never authorize overwriting Go authority.
						resolved, err := result.TypeResolver.ResolveWithProvenance(expression)
						if err != nil {
							return err
						}
						if resolved != nil && resolved.Origin == typecatalog.TypeOriginGenerated {
							generated, err := generatedOutputRow(result, generatedRows, child, expression)
							if err != nil {
								return err
							}
							if generated {
								if err := visit(child); err != nil {
									return err
								}
								continue
							}
						}
					}
					if descriptor.Name == "" || descriptor.PkgPath == "" {
						return fmt.Errorf("related row type %q requires existing named Go type authority", expression)
					}
					if _, err := xshape.New(descriptor, result.TypeResolver.Descriptor).Fields(); err != nil {
						return fmt.Errorf("related row type %q must be a Go struct: %w", expression, err)
					}
					if result.Views == nil {
						result.Views = gen.ViewReferences{}
					}
					if prior := result.Views[identity]; prior != nil {
						if prior.DescriptorKey != descriptor.Key() {
							return fmt.Errorf("related view %q has conflicting row type authorities", identity)
						}
						// Preserve selected linked/borrowed authority and its receipt.
						continue
					}
					result.Views[identity] = &gen.ViewReference{DescriptorKey: descriptor.Key()}
					continue
				}
			}
			if err := visit(child); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(result.Component.RootView)
}

// Qualified output_type explicitly selects an existing package contract, just
// as a qualified output/view selects existing rows. Bare names remain generated.
func linkDeclaredOutputContract(result *Result, authored *spec.Component) error {
	if result == nil || result.TypeResolver == nil || authored == nil || authored.Settings == nil || result.Source != nil && result.Source.PackageComponent != nil {
		return nil
	}
	return linkExplicitOutputContract(result, authored.Settings.OutputType)
}

// linkExplicitOutputContract checks only an expression retained from the current
// authored DQL. Package defaults and implicit role linkage remain separate.
func linkExplicitOutputContract(result *Result, expression string) error {
	if result == nil || result.TypeResolver == nil {
		return nil
	}
	expression = strings.TrimSpace(expression)
	if !strings.Contains(expression, ".") {
		return nil
	}
	resolved, err := result.TypeResolver.ResolveWithProvenance(expression)
	if err != nil {
		return fmt.Errorf("resolve output contract %q: %w", expression, err)
	}
	// Previously generated contracts remain generation-owned. Artifact destination
	// and publication checks still determine whether this component may write them.
	if resolved != nil && resolved.Origin == typecatalog.TypeOriginGenerated {
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
	if result.Source != nil && result.Source.Types != nil {
		origin, _, err := result.Source.Types.Origin(typecatalog.PackageAuthority, descriptor.Key())
		if err != nil {
			return err
		}
		if origin == typecatalog.TypeOriginGenerated {
			return nil
		}
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
