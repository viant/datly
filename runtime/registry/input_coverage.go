package registry

import (
	"fmt"
	"reflect"
	"strings"

	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/tagly/tags"
)

func (c *InputCatalog) resolve(ref spec.RouteRef, component *spec.Key) (*componentInputRoute, error) {
	var result *componentInputRoute
	for _, candidate := range c.routes[ref.String()] {
		if component != nil && candidate.component != *component {
			continue
		}
		if result != nil {
			return nil, fmt.Errorf("ambiguous component dependency route contract: %s", ref.String())
		}
		value := candidate
		result = &value
	}
	if result == nil {
		return nil, fmt.Errorf("component dependency route contract not found: %s", ref.String())
	}
	return result, nil
}

// The second phase validates the entire dependency graph against the completed,
// unchanged default projection. Covered branches cannot supply each other.
type scopedTransportCoverage struct {
	projection      *inputProjection
	active, visited map[string]bool
}

func (c *scopedTransportCoverage) visit(ref spec.RouteRef, component *spec.Key, covered bool) error {
	entry, err := c.projection.catalog.resolve(ref, component)
	if err != nil {
		return err
	}
	identity := entry.component.String() + "|" + ref.String()
	if c.active[identity] {
		return fmt.Errorf("component input dependency cycle at %s", ref.String())
	}
	visitKey := fmt.Sprintf("%s|covered=%t", identity, covered)
	if c.visited[visitKey] {
		return nil
	}
	c.active[identity] = true
	defer delete(c.active, identity)
	for _, field := range entry.contract.Fields() {
		binding := field.binding
		if target, ok := field.Dependency(); ok {
			if binding.When != "" || binding.With != "" || ((covered || binding.Scope != "") && (binding.ResolutionGroup != nil || binding.Async)) {
				return fmt.Errorf("conditional component dependency %s requires a transport projection", field.Path())
			}
			if err = c.visit(target, nil, covered || binding.Scope != ""); err != nil {
				return fmt.Errorf("%s input %s: %w", ref.String(), field.Path(), err)
			}
			continue
		}
		if !requestTransport(field) {
			continue
		}
		if binding.Scope != "" {
			return fmt.Errorf("scoped transport input %s requires a transport projection", field.Path())
		}
		if !covered {
			continue
		}
		if binding.When != "" || binding.With != "" || binding.ResolutionGroup != nil || binding.Async {
			return fmt.Errorf("conditional transport input %s requires a transport projection", field.Path())
		}
		key := transportCoverageKey(field)
		index, exists := c.projection.locations[key]
		if !exists {
			return fmt.Errorf("scoped transport input %s adds uncovered %s", field.Path(), key)
		}
		authority := c.projection.fields[index]
		if authority.binding.When != "" || authority.binding.Scope != "" || authority.binding.With != "" || authority.binding.ResolutionGroup != nil || authority.binding.Async {
			return fmt.Errorf("scoped transport input %s has no unconditional authority for %s", field.Path(), key)
		}
		if err = compatibleTransportCoverage(authority, field); err != nil {
			return fmt.Errorf("scoped transport input %s conflicts with %s: %w", field.Path(), key, err)
		}
	}
	c.visited[visitKey] = true
	return nil
}
func requestTransport(field InputField) bool {
	switch field.binding.Location.Kind {
	case "query", "path", "header", "cookie", "body", "form":
		return true
	}
	return false
}
func transportCoverageKey(field InputField) string {
	b := field.binding
	name := b.Location.In
	if name == "" && b.Location.Kind != "body" {
		name = b.Name
	}
	if b.Location.Kind == "header" {
		name = strings.ToLower(name)
	}
	return b.Location.Kind + ":" + name
}
func transportRequired(field InputField) bool {
	return field.binding.Location.Kind == "path" || field.binding.Required != nil && *field.binding.Required
}

// The official compiler adapter is private and imports registry. Inspecting its
// concrete type avoids an import cycle or a new marker ABI; no codec/transformer
// method (including Codec/WireSourceType) runs during this proof.
func officialCoverageTransformer(field InputField) bool {
	value := reflect.ValueOf(field.binding.Transformer)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() {
		return false
	}
	typ := value.Type().Elem()
	return typ.PkgPath() == "github.com/viant/datly/runtime/handler/compiler" && typ.Name() == "codecTransformer"
}
func coverageSourceType(field InputField) reflect.Type {
	if field.binding.SourceType != nil {
		return field.binding.SourceType
	}
	return field.destinationType
}
func compatibleTransportCoverage(authority, child InputField) error {
	// Reject unknown transformers BEFORE any accessor that might execute their methods.
	transformed := authority.binding.Transformer != nil || child.binding.Transformer != nil
	if transformed {
		if !officialCoverageTransformer(authority) || !officialCoverageTransformer(child) || !authority.verifiedJWT || !child.verifiedJWT {
			return fmt.Errorf("unsupported transformer authority")
		}
		if coverageSourceType(authority) != reflect.TypeFor[string]() || coverageSourceType(child) != reflect.TypeFor[string]() || authority.destinationType != reflect.TypeFor[*jwt.Claims]() || child.destinationType != reflect.TypeFor[*jwt.Claims]() {
			return fmt.Errorf("unsupported JWT representation")
		}
	} else if authority.verifiedJWT || child.verifiedJWT {
		return fmt.Errorf("unconstructed verification authority")
	}
	if coverageSourceType(authority) != coverageSourceType(child) || authority.destinationType != child.destinationType || authority.wireType != child.wireType || authority.anonymous != child.anonymous || !reflect.DeepEqual(authority.wireSchema, child.wireSchema) || !reflect.DeepEqual(authority.wireSchemas, child.wireSchemas) {
		return fmt.Errorf("different source, destination or wire contract")
	}
	if transportRequired(child) && !transportRequired(authority) {
		return fmt.Errorf("required child lacks required default authority")
	}
	if child.verifiedJWT && !authority.verifiedJWT {
		return fmt.Errorf("child verification lacks default authority")
	}
	a, b := authority.binding, child.binding
	// Owner storage and marker paths are not transport names; requiredness is directional.
	a.Path, b.Path = "", ""
	a.MarkerField, b.MarkerField = "", ""
	a.Required, b.Required = nil, nil
	a.Location, b.Location = bindstate.Location{}, bindstate.Location{}
	a.Transformer, b.Transformer = nil, nil
	a.Extension, b.Extension = nil, nil
	if !reflect.DeepEqual(a, b) {
		return fmt.Errorf("different binding constraints")
	}
	if err := coverageParameter(authority, child, transformed); err != nil {
		return err
	}
	leftTags, err := coverageTags(authority)
	if err != nil {
		return err
	}
	rightTags, err := coverageTags(child)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(leftTags, rightTags) {
		return fmt.Errorf("different public field tags")
	}
	af, err := authority.StructField()
	if err != nil {
		return err
	}
	bf, err := child.StructField()
	if err != nil {
		return err
	}
	if af.Anonymous != bf.Anonymous {
		return fmt.Errorf("different embedded field projection")
	}
	// Descriptions and examples belong to the selected field's owner. They do
	// not change transport compatibility; projection retains that owner's docs.

	ah, e := authority.Internal()
	if e != nil {
		return e
	}
	ch, e := child.Internal()
	if e != nil {
		return e
	}
	if ah != ch {
		return fmt.Errorf("different public visibility")
	}
	return nil
}

func coverageParameter(authority, child InputField, transformed bool) error {
	a, ao := authority.binding.Extension.(*spec.Parameter)
	b, bo := child.binding.Extension.(*spec.Parameter)
	if authority.binding.Extension == nil && child.binding.Extension == nil {
		if transformed {
			return fmt.Errorf("missing JWT codec contract")
		}
		return nil
	}
	if !ao || !bo || a == nil || b == nil {
		return fmt.Errorf("unsupported parameter extension")
	}
	if transformed {
		if a.Codec == nil || b.Codec == nil || a.Codec.Body != "JwtClaim" || b.Codec.Body != "JwtClaim" || len(a.Codec.Args) != 0 || len(b.Codec.Args) != 0 {
			return fmt.Errorf("unsupported JWT codec contract")
		}
	} else if a.Codec != nil || b.Codec != nil {
		return fmt.Errorf("unconstructed codec contract")
	}
	left, right := *a, *b
	left.Description, right.Description = "", ""
	left.Example, right.Example = "", ""
	left.Required, right.Required = nil, nil
	left.Raw, right.Raw = "", ""
	left.Tag, right.Tag = "", ""
	left.Source, right.Source = spec.BindSource{}, spec.BindSource{}
	if !reflect.DeepEqual(left, right) {
		return fmt.Errorf("different public parameter metadata")
	}

	return nil
}
func coverageTags(field InputField) (map[string]string, error) {
	f, e := field.StructField()
	if e != nil {
		return nil, e
	}
	result := map[string]string{}
	appendTag := func(tag string) error {
		items, e := tags.Parse(tag)
		if e != nil {
			return e
		}
		for _, item := range items {
			if item.Name == "bind" || item.Name == "parameter" {
				continue
			}
			value := string(item.Values)
			if previous, ok := result[item.Name]; ok && previous != value {
				return fmt.Errorf("conflicting retained field tags")
			}
			result[item.Name] = value
		}
		return nil
	}
	if e = appendTag(string(f.Tag)); e != nil {
		return nil, e
	}
	if p, ok := field.binding.Extension.(*spec.Parameter); ok && p != nil {
		if e = appendTag(p.Tag); e != nil {
			return nil, e
		}
	}
	return result, nil
}
