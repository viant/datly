package generate

import (
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"strings"
	"time"

	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
	xresponse "github.com/viant/xdatly/response"
)

// RuntimeInputType materializes the generated input contract used before the
// generated package itself exists. Linked contracts should use their compiled
// reflect.Type directly; this method is the generated-contract counterpart.
func (g *Generator) RuntimeInputType() (reflect.Type, error) {
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
	return newRuntimeInputMaterializer(plan, g.resolver).inputType()
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
	plan         *Plan
	resolver     typeResolver
	imports      map[string]string
	helpers      map[string]HelperType
	views        map[string]ViewPlan
	placeholders map[string]bool
	types        map[string]reflect.Type
	building     map[string]bool
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

func runtimeFields(source []Field) []xshape.RuntimeField {
	result := make([]xshape.RuntimeField, 0, len(source))
	for _, field := range source {
		if field.Implementation {
			continue
		}
		result = append(result, xshape.RuntimeField{
			Name: field.Name, TypeExpr: field.Type, Tag: reflect.StructTag(field.Tag),
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
	descriptor, err := m.resolver.Descriptor(name)
	if err != nil {
		return nil, err
	}
	if descriptor == nil {
		return nil, fmt.Errorf("named type %q was not found", name)
	}
	if descriptor.Type == nil {
		return nil, fmt.Errorf("named type %q has no compiled runtime type", name)
	}
	return descriptor.Type, nil
}

func standardRuntimeType(name string) reflect.Type {
	switch strings.TrimSpace(name) {
	case "encoding/json.RawMessage":
		return reflect.TypeFor[json.RawMessage]()
	case "time.Time":
		return reflect.TypeFor[time.Time]()
	case "github.com/viant/xdatly/response.Status", "response.Status":
		return reflect.TypeFor[xresponse.Status]()
	case "github.com/viant/xdatly/handler.Violation", "handler.Violation", "xhandler.Violation":
		return reflect.TypeFor[xhandler.Violation]()
	}
	return nil
}
