package registry

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
)

// InputCatalog projects the transitive transport contract of exact registered
// routes. It contains immutable metadata, never providers or binding execution.
// Publication filtering is deliberately outside this owner: private component
// inputs may be required to invoke a public parent.
type InputCatalog struct {
	routes map[string][]componentInputRoute
}

type componentInputRoute struct {
	component spec.Key
	contract  *RouteInputContract
}

func NewInputCatalog(entries []*RegisteredComponent) (*InputCatalog, error) {
	result := &InputCatalog{routes: map[string][]componentInputRoute{}}
	seen := map[spec.Key]bool{}
	for _, entry := range entries {
		if entry == nil || entry.Component == nil || entry.Input == nil {
			return nil, fmt.Errorf("input catalog requires compiled registrations")
		}
		if seen[entry.Component.Key] {
			return nil, fmt.Errorf("duplicate component %s", entry.Component.Key.String())
		}
		seen[entry.Component.Key] = true
		for _, endpoint := range entry.Component.Routes {
			if endpoint == nil {
				return nil, fmt.Errorf("input catalog has a nil route")
			}
			if _, err := route.CompilePathTemplate(endpoint.Path); err != nil {
				return nil, err
			}
			ref := spec.RouteRef{Method: endpoint.Method, Path: endpoint.Path}
			contract, ok := entry.Input.ForRoute(ref)
			if !ok {
				return nil, fmt.Errorf("route-effective input contract is missing for %s", ref.String())
			}
			key := ref.String()
			for _, existing := range result.routes[key] {
				if existing.component == entry.Component.Key {
					return nil, fmt.Errorf("duplicate input contract for %s at %s", entry.Component.Key.String(), key)
				}
			}
			result.routes[key] = append(result.routes[key], componentInputRoute{component: entry.Component.Key, contract: contract})
		}
	}
	return result, nil
}

// Fields returns detached effective HTTP inputs, including every statically
// declared component dependency. All component fields execute binding regardless
// of their own Required flag; child requiredness therefore remains authoritative.
func (c *InputCatalog) Fields(ref spec.RouteRef) ([]InputField, error) {
	return c.fields(ref, nil)
}

// FieldsFor selects an exact component contract even when other MCP components
// have the same HTTP route. Route-only dependencies must still be unambiguous.
func (c *InputCatalog) FieldsFor(component spec.Key, ref spec.RouteRef) ([]InputField, error) {
	return c.fields(ref, &component)
}

func (c *InputCatalog) fields(ref spec.RouteRef, component *spec.Key) ([]InputField, error) {
	projection := inputProjection{root: ref, catalog: c, active: map[string]bool{}, visited: map[string]bool{}, locations: map[string]int{}}
	if err := projection.visit(ref, component); err != nil {
		return nil, err
	}
	return projection.fields, nil
}

type inputProjection struct {
	root            spec.RouteRef
	catalog         *InputCatalog
	active, visited map[string]bool
	locations       map[string]int
	fields          []InputField
}

func (p *inputProjection) visit(ref spec.RouteRef, component *spec.Key) error {
	var entry *RouteInputContract
	for _, candidate := range p.catalog.routes[ref.String()] {
		if component != nil && candidate.component != *component {
			continue
		}
		if entry != nil {
			return fmt.Errorf("ambiguous component dependency route contract: %s", ref.String())
		}
		entry = candidate.contract
	}
	if entry == nil {
		return fmt.Errorf("component dependency route contract not found: %s", ref.String())
	}
	if p.active[ref.String()] {
		return fmt.Errorf("component input dependency cycle at %s", ref.String())
	}
	if p.visited[ref.String()] {
		return nil
	}
	p.active[ref.String()] = true
	defer delete(p.active, ref.String())
	for _, field := range entry.Fields() {
		if target, ok := field.Dependency(); ok {
			binding := field.Binding()
			if binding.When != "" || binding.Scope != "" || binding.With != "" {
				return fmt.Errorf("conditional component dependency %s requires a transport projection", field.Path())
			}
			if err := p.visit(target, nil); err != nil {
				return fmt.Errorf("%s input %s: %w", ref.String(), field.Path(), err)
			}
			continue
		}
		switch field.binding.Location.Kind {
		case "query", "path", "header", "cookie", "body", "form":
		default:
			continue
		}
		if err := p.add(field); err != nil {
			return fmt.Errorf("%s: %w", ref.String(), err)
		}
	}
	p.visited[ref.String()] = true
	return nil
}

func (p *inputProjection) add(field InputField) error {
	binding := field.binding
	name := binding.Location.In
	if name == "" && binding.Location.Kind != "body" {
		name = binding.Name
	}
	key := binding.Location.Kind + ":" + name
	if binding.Location.Kind == "header" {
		key = strings.ToLower(key)
	}
	if index, ok := p.locations[key]; ok {
		previous := &p.fields[index]
		prior := previous.binding
		// Different consumers may bind the same request value. Merge identical wire
		// contracts and requiredness; incompatible constraints must not be discarded.
		if previous.SourceType() != field.SourceType() || prior.When != binding.When || prior.Scope != binding.Scope || prior.With != binding.With ||
			!reflect.DeepEqual(prior.MinAllowedRecords, binding.MinAllowedRecords) || !reflect.DeepEqual(prior.MaxAllowedRecords, binding.MaxAllowedRecords) ||
			!reflect.DeepEqual(prior.ExpectedReturned, binding.ExpectedReturned) || !reflect.DeepEqual(prior.DefaultValue, binding.DefaultValue) {
			return fmt.Errorf("conflicting transitive transport input %s", key)
		}
		if field.origin.String() == p.root.String() && previous.origin.String() != p.root.String() {
			required := previous.binding.Required
			verified := previous.verifiedJWT
			*previous = field
			if required != nil && *required {
				value := true
				previous.binding.Required = &value
			}
			previous.verifiedJWT = previous.verifiedJWT || verified
		}
		if binding.Required != nil && *binding.Required {
			value := true
			previous.binding.Required = &value
		}
		previous.verifiedJWT = previous.verifiedJWT || field.verifiedJWT
		return nil
	}
	p.locations[key] = len(p.fields)
	p.fields = append(p.fields, field)
	return nil
}
