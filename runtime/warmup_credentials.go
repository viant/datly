package runtime

import (
	"fmt"
	"strings"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

// CredentialHeaders selects only canonical, verified JWT inputs for transport
// retention. Other input values must come from server-authored warmup cases.
func (w *Warmup) CredentialHeaders() ([]string, error) {
	if _, ok := w.registered.Reader.(dexec.QueryPreparer); !ok {
		return nil, fmt.Errorf("HTTP warmup requires canonical query preparation")
	}
	rootContract, _ := w.registered.Input.ForRoute(w.target.Route)
	entries := make([]*registry.RegisteredComponent, 0, len(w.runtime.registered))
	for _, entry := range w.runtime.registered {
		entries = append(entries, entry)
	}
	catalog, err := registry.NewInputCatalog(entries)
	if err != nil {
		return nil, err
	}
	fields, err := catalog.Fields(w.target.Route)
	if err != nil {
		return nil, err
	}
	var result []string
	for _, field := range fields {
		if !field.VerifiesJWT() {
			continue
		}
		binding := field.Binding()
		if binding.Location.Kind != "header" || binding.When != "" || binding.Scope != "" || binding.With != "" {
			return nil, fmt.Errorf("HTTP warmup requires unconditional header JWT binding: %s", field.Path())
		}
		for _, root := range rootContract.Fields() {
			source := root.Binding()
			if source.Location.Kind != "header" || !strings.EqualFold(source.Location.In, binding.Location.In) {
				continue
			}
			name := root.Path()
			if parameter, ok := source.Extension.(*spec.Parameter); ok && parameter != nil && parameter.Name != "" {
				name = parameter.Name
			}
			if w.settings.IndexParameter == name {
				return nil, fmt.Errorf("warmup index must not remove JWT input %s", name)
			}
			for _, set := range w.settings.Cases {
				if set != nil {
					for _, parameter := range set.Set {
						if parameter != nil && parameter.Name == name {
							return nil, fmt.Errorf("warmup cases must not override caller JWT input %s", name)
						}
					}
				}
			}
		}

		result = append(result, binding.Location.In)
	}
	return result, nil
}
