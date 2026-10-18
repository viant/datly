package link

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/typeidentity"
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

// Contracts identifies package contracts whose metadata survives a DQL overlay.
type Contracts struct {
	Lookup            xshape.Lookup
	PackageComponent  *spec.Component
	CompiledComponent *spec.Component
	Source            *bootstrap.RouteSource
	Input             *x.Type
	Output            *x.Type
}

// References propagates current type-authority failures during authoring.
func (l *Contracts) References() (gen.ContractReferences, error) {
	var result gen.ContractReferences
	input, err := l.EqualParamsChecked(false)
	if err != nil {
		return result, err
	}
	if input {
		result.Input = Reference(l.Source.InputType, l.Input)
	}
	output, err := l.EqualParamsChecked(true)
	if err != nil {
		return result, err
	}
	if output {
		result.Output = Reference(l.Source.OutputType, l.Output)
	}
	return result, nil
}

// PrepareGeneratedTypes removes package-owned type expressions only
// when DQL changed that role. Generation then uses its local component-derived
// name instead of attempting to redeclare an imported package contract.
func (l *Contracts) PrepareGeneratedTypes(component *spec.Component, contracts gen.ContractReferences, authoredInput, authoredOutput string) {
	if component == nil || component.Settings == nil || l == nil || l.Source == nil {
		return
	}
	if contracts.Input == nil && authoredInput == "" && l.sameTypeExpression(component.Settings.InputType, l.Source.InputType) {
		component.Settings.InputType = ""
	}
	if contracts.Output == nil && authoredOutput == "" && l.sameTypeExpression(component.Settings.OutputType, l.Source.OutputType) {
		component.Settings.OutputType = ""
	}
}

func (l *Contracts) sameTypeExpression(left, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	return left != "" && left == right
}

// Reference preserves the exact descriptor identity of a linked contract.
func Reference(expression string, descriptor *x.Type) *gen.ContractReference {
	if descriptor == nil {
		return nil
	}
	return &gen.ContractReference{Expression: strings.TrimSpace(expression), DescriptorKey: descriptor.Key()}
}

func (l *Contracts) EqualParams(output bool) bool {
	equal, err := l.EqualParamsChecked(output)
	return err == nil && equal
}

// EqualParamsChecked compares detached contracts under the selected authority.
func (l *Contracts) EqualParamsChecked(output bool) (bool, error) {
	if output && !l.equalViews() {
		return false, nil
	}
	packageParams, err := l.params(l.PackageComponent, output)
	if err != nil {
		return false, err
	}
	compiledParams, err := l.params(l.CompiledComponent, output)
	if err != nil {
		return false, err
	}
	packageContract, err := json.Marshal(packageParams)
	if err != nil {
		return false, err
	}
	compiledContract, err := json.Marshal(compiledParams)
	if err != nil {
		return false, err
	}
	return bytes.Equal(packageContract, compiledContract), nil
}

func (l *Contracts) equalViews() bool {
	var packageView, compiledView *spec.View
	var err error
	if l.PackageComponent != nil {
		packageView, err = l.normalizedView(l.PackageComponent)
		if err != nil {
			return false
		}
	}
	if l.CompiledComponent != nil {
		compiledView, err = l.normalizedView(l.CompiledComponent)
		if err != nil {
			return false
		}
	}
	packageJSON, err := json.Marshal(packageView)
	if err != nil {
		return false
	}
	compiledJSON, err := json.Marshal(compiledView)
	return err == nil && bytes.Equal(packageJSON, compiledJSON)
}

func (l *Contracts) normalizedView(component *spec.Component) (*spec.View, error) {
	if component == nil {
		return nil, nil
	}
	// Binding-inferred policy is not an authored output-contract change. Compare
	// both contracts after the same resolution, without mutating package metadata.
	component = component.Clone()
	if err := bootstrap.ResolveQuerySelectorViews(component, false); err != nil {
		return nil, err
	}
	inheritedDest := ""
	if component.Settings != nil && component.Settings.Generation != nil {
		inheritedDest = strings.TrimSpace(component.Settings.Generation.ViewFile)
	}
	return NormalizeOwnershipView(component.RootView, inheritedDest, true), nil
}

// NormalizeOwnershipView clones a view and removes representational differences
// before comparing package ownership, without changing authored metadata.
func NormalizeOwnershipView(source *spec.View, inheritedDest string, omitNamespace bool) *spec.View {
	view := source.Clone()
	visited := map[*spec.View]bool{}
	var normalize func(*spec.View, string)
	normalize = func(current *spec.View, inherited string) {
		if current == nil || visited[current] {
			return
		}
		visited[current] = true
		if omitNamespace {
			current.Namespace = ""
		}
		effectiveDest := strings.TrimSpace(current.Dest)
		if effectiveDest == "" {
			effectiveDest = inherited
			current.Dest = effectiveDest
		}
		if current.Source != nil {
			for _, embed := range current.Source.Embeds {
				if embed != nil {
					embed.Raw = ""
				}
			}
		}
		for _, relation := range current.Relations {
			if relation != nil {
				normalize(relation.View, effectiveDest)
			}
		}
	}
	normalize(view, inheritedDest)
	return view
}

func (l *Contracts) params(component *spec.Component, output bool) ([]*spec.Parameter, error) {
	if component == nil {
		return nil, nil
	}
	var result []*spec.Parameter
	for _, param := range spec.EffectiveParameters(component.Parameters) {
		if param == nil || l.isOutputParam(param) != output {
			continue
		}
		copy := *param
		copy.Declaration = ""
		copy.Raw = ""
		// Compare the same Go destination identity serialized by reader metadata;
		// reflection erases aliases but cannot change a named type or wrapper.
		if l.Lookup != nil {
			if err := typeidentity.NormalizeDestination(&copy, component.Key.Scope, component.TypeContext, l.Lookup); err != nil {
				return nil, err
			}
		}
		result = append(result, &copy)
	}
	return result, nil
}

func (l *Contracts) isOutputParam(param *spec.Parameter) bool {
	return param != nil && (param.EmitOutput || strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output"))
}
