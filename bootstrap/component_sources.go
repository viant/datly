package bootstrap

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

// PackageComponentSource groups the ordered route holders that declare one
// package-authoritative component contract.
type PackageComponentSource struct {
	Routes     []*RouteSource
	InputType  *x.Type
	OutputType *x.Type
	component  *spec.Component
}

// ComponentName returns the canonical component name shared by this source's
// route holders.
func (s *PackageComponentSource) ComponentName() string {
	if s == nil || len(s.Routes) == 0 {
		return ""
	}
	return s.Routes[0].componentName()
}

// GroupPackageComponentSources resolves exact contract identities and groups
// route holders without relying on field names, file order heuristics, or
// dynamic registration.
func GroupPackageComponentSources(routes []*RouteSource, types *typecatalog.Resolver) ([]*PackageComponentSource, error) {
	if types == nil {
		return nil, fmt.Errorf("package type resolver is required")
	}
	groups := map[string]*PackageComponentSource{}
	var order []string
	for _, route := range routes {
		if route == nil {
			return nil, fmt.Errorf("package route source is required")
		}
		component, err := route.canonicalComponent()
		if err != nil {
			return nil, err
		}
		input, err := types.ResolveShape(route.InputType)
		if err != nil {
			return nil, fmt.Errorf("resolve package component %s input: %w", component.Key.String(), err)
		}
		output, err := types.ResolveShape(route.OutputType)
		if err != nil {
			return nil, fmt.Errorf("resolve package component %s output: %w", component.Key.String(), err)
		}
		if input == nil || input.Descriptor == nil || output == nil || output.Descriptor == nil {
			return nil, fmt.Errorf("package component %s contract descriptors are required", component.Key.String())
		}
		key := component.Key.String() + "\x00" + input.Identity + "\x00" + output.Identity
		group := groups[key]
		if group == nil {
			group = &PackageComponentSource{
				InputType: input.Descriptor, OutputType: output.Descriptor, component: component,
			}
			groups[key] = group
			order = append(order, key)
		}
		group.Routes = append(group.Routes, route)
	}
	result := make([]*PackageComponentSource, 0, len(order))
	for _, key := range order {
		result = append(result, groups[key])
	}
	sort.SliceStable(result, func(i, j int) bool {
		left, right := result[i].Routes[0], result[j].Routes[0]
		if left.PackagePath != right.PackagePath {
			return left.PackagePath < right.PackagePath
		}
		return result[i].ComponentName() < result[j].ComponentName()
	})
	return result, nil
}

// ResolveDescriptors reconstructs one canonical component from all grouped
// route holders. Component-wide metadata must agree exactly across routes.
func (s *PackageComponentSource) ResolveDescriptors(types *typecatalog.Resolver) (*spec.Component, error) {
	if s == nil || len(s.Routes) == 0 {
		return nil, fmt.Errorf("package component routes are required")
	}
	authority := s.component
	if authority == nil {
		var err error
		authority, err = s.Routes[0].canonicalComponent()
		if err != nil {
			return nil, err
		}
		s.component = authority
	}
	// Assemble every authored holder before route expansion. An emitted
	// alternate must already be visible when WithURI checks existing routes.
	combined := authority.Clone()
	for _, route := range s.Routes[1:] {
		component, err := route.canonicalComponent()
		if err != nil {
			return nil, err
		}
		if !s.matchesMetadata(component) {
			return nil, fmt.Errorf("package component %s has conflicting component metadata across route holders", authority.Key.String())
		}
		combined.Routes = append(combined.Routes, component.Routes...)
	}
	return (ContractResolver{
		Component: combined, InputType: s.InputType, OutputType: s.OutputType, Types: types,
	}).Resolve()
}

func (s *PackageComponentSource) matchesMetadata(candidate *spec.Component) bool {
	if s == nil || s.component == nil || candidate == nil {
		return s != nil && s.component == candidate
	}
	authority := s.component.Clone()
	actual := candidate.Clone()
	authority.Routes = nil
	actual.Routes = nil
	return reflect.DeepEqual(authority, actual)
}
