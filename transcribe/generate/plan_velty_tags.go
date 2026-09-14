package generate

import (
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/tagly/tags"
)

func applyGeneratedInputVeltyAliases(plan *Plan, component *spec.Component) {
	if plan == nil || component == nil || plan.Input.Ownership != ContractGenerated {
		return
	}
	indexes := make(map[string]int, len(plan.Input.Fields))
	for index := range plan.Input.Fields {
		indexes[plan.Input.Fields[index].Name] = index
	}
	for _, param := range preferDefinedParams(component.Parameters) {
		if param == nil || param.EmitOutput || strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output") {
			continue
		}
		fieldName := generatedParameterName(param)
		logicalName := param.Name
		if param.QuerySelector != nil {
			logicalName = fieldName
		}
		index, ok := indexes[fieldName]
		if !ok {
			continue
		}
		plan.Input.Fields[index].Tag = withVeltyNames(plan.Input.Fields[index].Tag, fieldName, logicalName, true)
	}
}

func withVeltyNames(raw string, first string, second string, enabled bool) string {
	if !enabled || tags.NewTags(strings.TrimSpace(raw)).Lookup("velty") != nil {
		return raw
	}
	seen := map[string]bool{}
	names := make([]string, 0, 2)
	for _, candidate := range []string{first, second} {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" || seen[candidate] {
			continue
		}
		seen[candidate] = true
		names = append(names, candidate)
	}
	if len(names) == 0 {
		return raw
	}
	return appendStructTag(raw, "velty", "names="+strings.Join(names, "|"))
}
