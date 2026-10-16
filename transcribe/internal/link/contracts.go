package link

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/x"
)

// Contracts identifies package contracts whose metadata survives a DQL overlay.
type Contracts struct {
	PackageComponent  *spec.Component
	CompiledComponent *spec.Component
	Source            *bootstrap.RouteSource
	Input             *x.Type
	Output            *x.Type
}

func (l *Contracts) References() gen.ContractReferences {
	var result gen.ContractReferences
	if l.EqualParams(false) {
		result.Input = Reference(l.Source.InputType, l.Input)
	}
	if l.EqualParams(true) {
		result.Output = Reference(l.Source.OutputType, l.Output)
	}
	return result
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
	if output && !l.equalViews() {
		return false
	}
	packageContract, err := json.Marshal(l.params(l.PackageComponent, output))
	if err != nil {
		return false
	}
	compiledContract, err := json.Marshal(l.params(l.CompiledComponent, output))
	return err == nil && bytes.Equal(packageContract, compiledContract)
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

func (l *Contracts) params(component *spec.Component, output bool) []*spec.Parameter {
	if component == nil {
		return nil
	}
	var result []*spec.Parameter
	for _, param := range spec.EffectiveParameters(component.Parameters) {
		if param == nil || l.isOutputParam(param) != output {
			continue
		}
		copy := *param
		copy.Declaration = ""
		copy.Raw = ""
		result = append(result, &copy)
	}
	return result
}

func (l *Contracts) isOutputParam(param *spec.Parameter) bool {
	return param != nil && (param.EmitOutput || strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output"))
}
