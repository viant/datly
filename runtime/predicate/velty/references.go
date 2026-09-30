package velty

import (
	"sort"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

// References returns canonical handler type dependencies without changing the
// caller's metadata or requiring executable implementations yet.
func References(component *spec.Component, context *typecatalog.ResolutionContext) ([]string, error) {
	if component == nil {
		return nil, nil
	}
	component = (&spec.Component{Parameters: component.Parameters}).Clone()
	if err := (DefinitionCompiler{Context: context}).Compile(component); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	registry := predicateRegistry()
	for _, parameter := range component.Parameters {
		if parameter == nil {
			continue
		}
		for _, definition := range parameter.Predicates {
			if definition != nil && registry[strings.ToLower(strings.TrimSpace(definition.Name))].handler {
				seen[definition.Args[0]] = true
			}
		}
	}
	result := make([]string, 0, len(seen))
	for name := range seen {
		result = append(result, name)
	}
	sort.Strings(result)
	return result, nil
}
