package transcribe

import (
	"strings"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/internal/link"
)

// retainSelectorBusinessInput separates planned side storage from the admitted
// public input before applying the existing exact contract comparison.
func (a *packageAuthority) retainSelectorBusinessInput(result *Result, contracts *link.Contracts) error {
	if result.Contracts.Input != nil || strings.TrimSpace(a.source.Text) == "" {
		return nil
	}
	hasSelectors := false
	for _, component := range []*spec.Component{contracts.PackageComponent, contracts.CompiledComponent} {
		if component == nil {
			continue
		}
		for _, parameter := range spec.EffectiveParameters(component.Parameters) {
			if parameter != nil && parameter.QuerySelector != nil {
				hasSelectors = true
				break
			}
		}
	}
	if !hasSelectors {
		return nil
	}
	reference := link.Reference(a.route.InputType, a.input)
	if reference == nil {
		return nil
	}
	withoutSide := func(component *spec.Component) (*spec.Component, error) {
		if component == nil {
			return nil, nil
		}
		target := component.Key.Scope
		if component.TypeContext != nil && component.TypeContext.PackagePath != "" {
			target = component.TypeContext.PackagePath
		}
		side, err := gen.New(gen.Input{Component: component.Clone(), Declarations: result.Declarations, TypeResolver: result.TypeResolver, TargetPackage: target, Contracts: gen.ContractReferences{Input: reference}, Views: result.Views, ViewBindings: result.ViewBindings, Resources: result.Source.Resources}).SelectorSideParameters()
		if err != nil {
			return nil, err
		}
		selected := make(map[string]bool, len(side))
		for _, parameter := range side {
			selected[parameter.Identity()] = true
		}
		copy := component.Clone()
		params := copy.Parameters[:0]
		for _, parameter := range copy.Parameters {
			if parameter == nil || !selected[parameter.Identity()] {
				params = append(params, parameter)
			}
		}
		copy.Parameters = params
		return copy, nil
	}
	oldInput, err := withoutSide(contracts.PackageComponent)
	if err != nil {
		return err
	}
	newInput, err := withoutSide(contracts.CompiledComponent)
	if err != nil {
		return err
	}
	ordinary := &link.Contracts{PackageComponent: oldInput, CompiledComponent: newInput, Lookup: result.TypeResolver.Descriptor}
	equal, err := ordinary.EqualParamsChecked(false)
	if err != nil {
		return err
	}
	if equal {
		result.Contracts.Input = reference
	}
	return nil
}
