package generate

import (
	"fmt"
	"path"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/transcribe/internal/discoverytype"
	xshape "github.com/viant/x/shape"
)

// RuntimeInputType materializes the generated input contract used before the
// generated package itself exists. Linked contracts should use their compiled
// reflect.Type directly; this method is the generated-contract counterpart.
func (g *Generator) RuntimeInputType() (reflect.Type, error) {
	return g.runtimeInputType(false)
}

// ColumnDiscoveryInputType permits temporary structural representations of
// descriptor-only linked rows during column discovery. It does not establish
// compiled named identity and must not be used for component execution.
func (g *Generator) ColumnDiscoveryInputType() (reflect.Type, error) {
	return g.runtimeInputType(true)
}

func (g *Generator) runtimeInputType(discoveryOnly bool) (reflect.Type, error) {
	typ, _, err := g.runtimeInputPlan(discoveryOnly)
	return typ, err
}

// ColumnDiscoveryInput returns the exact generated side-parameter projection
// alongside the provisional business input. No holder method must exist yet.
func (g *Generator) ColumnDiscoveryInput() (reflect.Type, []*spec.Parameter, error) {
	typ, plan, err := g.runtimeInputPlan(true)
	if err != nil {
		return nil, nil, err
	}
	side, err := selectorParameters(g.input.Component, plan.QuerySelectors)
	return typ, side, err
}

func selectorParameters(component *spec.Component, fields []Field) ([]*spec.Parameter, error) {
	selected := map[string]bool{}
	for _, field := range fields {
		m, err := tag.ParseField(reflect.StructField{Name: field.Name, Tag: reflect.StructTag(field.Tag)})
		if err != nil {
			return nil, err
		}
		selected[(&spec.Parameter{Name: m.Binding.Name, Source: spec.BindSource{Kind: m.Binding.Location.Kind, Name: m.Binding.Location.In}}).Identity()] = true
	}
	var side []*spec.Parameter
	for _, parameter := range spec.EffectiveParameters(component.Parameters) {
		if parameter != nil && selected[parameter.Identity()] {
			side = append(side, parameter)
		}
	}
	return side, nil
}

func (g *Generator) runtimeInputPlan(discoveryOnly bool) (reflect.Type, *Plan, error) {
	if g == nil {
		return nil, nil, fmt.Errorf("generator is required")
	}
	if g.initErr != nil {
		return nil, nil, g.initErr
	}
	if g.input.Component == nil {
		return nil, nil, fmt.Errorf("generation component is required")
	}
	staged := *g
	staged.input.columnDiscovery = discoveryOnly
	plan, err := staged.plan(false)
	if err != nil {
		return nil, nil, err
	}
	plan.omitUnresolvedHelpers()
	if !discoveryOnly {
		// Preserve the existing transient, methodless Go input API. Persisted
		// DQL generation uses the holder companion instead of these fields.
		plan.Input.Fields = append(plan.Input.Fields, plan.QuerySelectors...)
	}
	materializer := newRuntimeInputMaterializer(plan, g.resolver)
	materializer.discoveryOnly = discoveryOnly
	typ, err := materializer.inputType()
	return typ, plan, err
}

// RuntimeOutputType materializes the generated output contract used before a
// generated package exists. It mirrors RuntimeInputType for dynamic readers.
func (g *Generator) RuntimeOutputType() (reflect.Type, error) {
	if g == nil {
		return nil, fmt.Errorf("generator is required")
	}
	if g.initErr != nil {
		return nil, g.initErr
	}
	if g.input.Component == nil {
		return nil, fmt.Errorf("generation component is required")
	}
	plan, err := g.plan(false)
	if err != nil {
		return nil, err
	}
	plan.omitUnresolvedHelpers()
	return newRuntimeInputMaterializer(plan, g.resolver).outputType()
}

func (plan *Plan) omitUnresolvedHelpers() {
	if plan == nil || len(plan.HelperTypes) == 0 {
		return
	}
	unresolved := map[string]bool{}
	concrete := plan.HelperTypes[:0]
	for _, helper := range plan.HelperTypes {
		probe := &Plan{HelperTypes: []HelperType{helper}}
		if probe.validateConcreteHelperFields() != nil {
			unresolved[helper.Name] = true
			continue
		}
		concrete = append(concrete, helper)
	}
	plan.HelperTypes = concrete
	if len(unresolved) == 0 {
		return
	}
	fields := plan.Input.Fields[:0]
	for _, field := range plan.Input.Fields {
		if unresolved[helperTypeName(unwrapQualifiedTypeName(field.Type))] {
			continue
		}
		fields = append(fields, field)
	}
	plan.Input.Fields = fields
}

type runtimeInputMaterializer struct {
	discoveryOnly bool
	plan          *Plan
	resolver      typeResolver
	imports       map[string]string
	helpers       map[string]HelperType
	views         map[string]ViewPlan
	placeholders  map[string]bool
	types         map[string]reflect.Type
	building      map[string]bool
}

func newRuntimeInputMaterializer(plan *Plan, resolver typeResolver) *runtimeInputMaterializer {
	result := &runtimeInputMaterializer{
		plan: plan, resolver: resolver, imports: map[string]string{}, helpers: map[string]HelperType{}, views: map[string]ViewPlan{},
		placeholders: map[string]bool{}, types: map[string]reflect.Type{}, building: map[string]bool{},
	}
	if plan == nil {
		return result
	}
	for _, item := range plan.Imports {
		alias := strings.TrimSpace(item.Alias)
		if alias == "" {
			alias = path.Base(strings.TrimSpace(item.Package))
		}
		if alias != "" {
			result.imports[alias] = strings.TrimSpace(item.Package)
		}
	}
	for _, helper := range plan.HelperTypes {
		result.helpers[helper.Name] = helper
	}
	for _, view := range plan.Views {
		if view.Ownership != ViewGenerated {
			continue
		}
		result.views[view.Name] = view
		result.views[view.Type] = view
		if plan.Package != "" {
			result.views[plan.Package+"."+view.Name] = view
		}
		if len(view.SetMarkerFields) != 0 {
			marker := HelperType{Name: view.Name + "Has"}
			for _, name := range view.SetMarkerFields {
				marker.Fields = append(marker.Fields, Field{Name: name, Type: "bool"})
			}
			result.helpers[marker.Name] = marker
		}
	}
	for _, name := range plan.referencedPlaceholderTypes() {
		result.placeholders[name] = true
	}
	return result
}

func (m *runtimeInputMaterializer) inputType() (reflect.Type, error) {
	if m == nil || m.plan == nil {
		return nil, fmt.Errorf("input contract plan is required")
	}
	fields := runtimeFields(m.plan.Input.Fields)
	if marker, ok := hasMarkerField(m.plan.Input.Type, m.plan.Input.Fields); ok {
		markerFields := make([]xshape.RuntimeField, 0, len(m.plan.Input.Fields))
		for _, field := range m.plan.Input.Fields {
			if shouldSkipHasMirror(field) {
				continue
			}
			markerFields = append(markerFields, xshape.RuntimeField{Name: field.Name, TypeExpr: "bool"})
		}
		markerType, err := m.runtime().Struct(markerFields)
		if err != nil {
			return nil, fmt.Errorf("materialize input marker: %w", err)
		}
		fields = append(fields, xshape.RuntimeField{
			Name: marker.Name, Type: (xshape.Runtime{}).Pointer(markerType), Tag: reflect.StructTag(marker.Tag),
		})
	}
	result, err := m.runtime().Struct(fields)
	if err != nil {
		return nil, fmt.Errorf("materialize input contract: %w", err)
	}
	return result, nil
}

func (m *runtimeInputMaterializer) outputType() (reflect.Type, error) {
	if m == nil || m.plan == nil {
		return nil, fmt.Errorf("output contract plan is required")
	}
	result, err := m.runtime().Struct(runtimeFields(m.plan.Output.Fields))
	if err != nil {
		return nil, fmt.Errorf("materialize output contract: %w", err)
	}
	return result, nil
}

func runtimeFields(source []Field) []xshape.RuntimeField {
	result := make([]xshape.RuntimeField, 0, len(source))
	for _, field := range source {
		if field.Implementation {
			continue
		}
		result = append(result, xshape.RuntimeField{
			Name: field.Name, TypeExpr: field.Type, Tag: reflect.StructTag(field.Tag), Anonymous: field.Anonymous,
		})
	}
	return result
}

func (m *runtimeInputMaterializer) runtime() xshape.Runtime {
	return xshape.Runtime{Imports: m.imports, Lookup: m.identifierType}
}

func (m *runtimeInputMaterializer) identifierType(name string) (reflect.Type, error) {
	if result := m.types[name]; result != nil {
		return result, nil
	}
	if helper, ok := m.helpers[name]; ok {
		if m.building[name] {
			return nil, fmt.Errorf("generated helper type %q is recursive", name)
		}
		m.building[name] = true
		result, err := m.runtime().Struct(runtimeFields(helper.Fields))
		delete(m.building, name)
		if err != nil {
			return nil, err
		}
		m.types[name] = result
		return result, nil
	}
	if view, ok := m.views[name]; ok {
		identity := view.Type
		if identity == "" {
			identity = view.Name
		}
		if m.building[identity] {
			return m.runtime().Struct(nil)
		}
		m.building[identity] = true
		result, err := m.runtime().Struct(runtimeFields(view.Fields))
		delete(m.building, identity)
		if err != nil {
			return nil, err
		}
		m.types[name] = result
		m.types[view.Name] = result
		m.types[view.Type] = result
		return result, nil
	}
	if m.placeholders[name] {
		result, err := m.runtime().Struct(nil)
		if err != nil {
			return nil, err
		}
		m.types[name] = result
		return result, nil
	}
	return m.descriptorType(name)
}

func (m *runtimeInputMaterializer) descriptorType(name string) (reflect.Type, error) {
	if standard := standardRuntimeType(name); standard != nil {
		return standard, nil
	}
	if m.resolver == nil {
		return nil, fmt.Errorf("named type %q requires type authority", name)
	}
	key := name
	if m.plan != nil && m.plan.knownLocalTypes[name] != "" {
		key = m.plan.knownLocalTypes[name]
	}
	descriptor, err := m.resolver.Descriptor(key)
	if err != nil {
		return nil, err
	}
	if descriptor == nil {
		return nil, fmt.Errorf("named type %q was not found", name)
	}
	if descriptor.Type != nil {
		return descriptor.Type, nil
	}
	if !m.discoveryOnly {
		return nil, fmt.Errorf("named type %q has no compiled runtime type", name)
	}
	key = descriptor.Key()
	if cached := m.types[key]; cached != nil {
		return cached, nil
	}
	if m.building[key] {
		return nil, fmt.Errorf("descriptor-only discovery type %q is recursive", key)
	}
	m.building[key] = true
	defer delete(m.building, key)
	result, err := discoverytype.Materialize(descriptor, m.resolver.Descriptor, m.runtime())
	if err != nil {
		return nil, err
	}
	m.types[key] = result
	return result, nil
}

func standardRuntimeType(name string) reflect.Type { return discoverytype.StandardType(name) }

// SelectorSideParameters plans only side storage against an admitted Input.
// Full generation remains responsible for output and view graph validation.
func (g *Generator) SelectorSideParameters() ([]*spec.Parameter, error) {
	if g == nil {
		return nil, fmt.Errorf("generator is required")
	}
	if g.initErr != nil {
		return nil, g.initErr
	}
	if g.input.Component == nil {
		return nil, fmt.Errorf("generation component is required")
	}
	if !ordinarySelectorReader(g.input.Component) {
		return nil, nil
	}
	fields, err := resolveSelectorFields(g.input.Component, g.input.Declarations)
	if err != nil {
		return nil, err
	}
	plan := &Plan{Package: g.input.TargetPackage, QuerySelectors: fields}
	resolver := &planResolver{input: g.input, types: g.resolver, plan: plan}
	if g.input.Contracts.Input != nil {
		if err = resolver.resolveContract("input", &plan.Input, g.input.Contracts.Input); err != nil {
			return nil, err
		}
	}
	if plan.Input.Ownership == ContractLinked {
		plan.QuerySelectors, err = resolver.linkedSelectorFields()
		if err != nil {
			return nil, err
		}
	}
	return selectorParameters(g.input.Component, plan.QuerySelectors)
}
