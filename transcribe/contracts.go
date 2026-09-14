package transcribe

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/x"
)

type contractLinker struct {
	packageComponent  *spec.Component
	compiledComponent *spec.Component
	source            *bootstrap.RouteSource
	input             *x.Type
	output            *x.Type
}

func (l *contractLinker) references() gen.ContractReferences {
	var result gen.ContractReferences
	if l.equalParams(false) {
		result.Input = l.reference(l.source.InputType, l.input)
	}
	if l.equalParams(true) {
		result.Output = l.reference(l.source.OutputType, l.output)
	}
	return result
}

// prepareGeneratedContractTypes removes package-owned type expressions only
// when DQL changed that role. Generation then uses its local component-derived
// name instead of attempting to redeclare an imported package contract.
func (l *contractLinker) prepareGeneratedTypes(component *spec.Component, contracts gen.ContractReferences, authored ContractTypeOverrides) {
	if component == nil || component.Settings == nil || l == nil || l.source == nil {
		return
	}
	if contracts.Input == nil && authored.Input == "" && l.sameTypeExpression(component.Settings.InputType, l.source.InputType) {
		component.Settings.InputType = ""
	}
	if contracts.Output == nil && authored.Output == "" && l.sameTypeExpression(component.Settings.OutputType, l.source.OutputType) {
		component.Settings.OutputType = ""
	}
}

func (l *contractLinker) sameTypeExpression(left, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	return left != "" && left == right
}

func (l *contractLinker) reference(expression string, descriptor *x.Type) *gen.ContractReference {
	if descriptor == nil {
		return nil
	}
	return &gen.ContractReference{Expression: strings.TrimSpace(expression), DescriptorKey: descriptor.Key()}
}

func (l *contractLinker) equalParams(output bool) bool {
	if output && !l.equalViews() {
		return false
	}
	packageContract, err := json.Marshal(l.params(l.packageComponent, output))
	if err != nil {
		return false
	}
	compiledContract, err := json.Marshal(l.params(l.compiledComponent, output))
	return err == nil && bytes.Equal(packageContract, compiledContract)
}

func (l *contractLinker) equalViews() bool {
	var packageView, compiledView *spec.View
	if l.packageComponent != nil {
		packageView = l.normalizedView(l.packageComponent)
	}
	if l.compiledComponent != nil {
		compiledView = l.normalizedView(l.compiledComponent)
	}
	packageJSON, err := json.Marshal(packageView)
	if err != nil {
		return false
	}
	compiledJSON, err := json.Marshal(compiledView)
	return err == nil && bytes.Equal(packageJSON, compiledJSON)
}

func (l *contractLinker) normalizedView(component *spec.Component) *spec.View {
	if component == nil {
		return nil
	}
	inheritedDest := ""
	if component.Settings != nil && component.Settings.Generation != nil {
		inheritedDest = strings.TrimSpace(component.Settings.Generation.ViewFile)
	}
	return l.normalizedOwnershipView(component.RootView, inheritedDest, true)
}

func (l *contractLinker) normalizedOwnershipView(source *spec.View, inheritedDest string, omitNamespace bool) *spec.View {
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

func (l *contractLinker) params(component *spec.Component, output bool) []*spec.Parameter {
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

func (l *contractLinker) isOutputParam(param *spec.Parameter) bool {
	return param != nil && (param.EmitOutput || strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output"))
}
