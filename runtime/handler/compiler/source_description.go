package compiler

import (
	"fmt"
	"github.com/viant/bindly"
	"github.com/viant/datly/internal/selectorcompanion"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	"reflect"
)

// DescribeContracts describes an authoring-only source pair. It never builds an
// executable selector plan or changes the receiver's runtime selection.
func (c *Compiler) DescribeContracts(input, output *x.Type, types *typecatalog.Resolver) (*Result, error) {
	return c.describeContracts(input, output, types)
}
func (c *Compiler) describeContracts(input, output *x.Type, types *typecatalog.Resolver) (*Result, error) {
	if c == nil || types == nil || input == nil || output == nil {
		return nil, fmt.Errorf("source description requires canonical contracts")
	}
	selected, err := selectorcompanion.SelectContracts(c.input.Component, input, output, types.Descriptor)
	if err != nil {
		return nil, err
	}
	if err = sourceDescriptionShape(input, c.input.InputType, types, map[string]bool{}); err != nil {
		return nil, err
	}
	local := *c
	local.publicComponent = c.input.Component.Clone()
	if selected != nil {
		local.publicComponent.Parameters = nil
		for _, p := range c.input.Component.Parameters {
			if !selected.Contains(p) {
				local.publicComponent.Parameters = append(local.publicComponent.Parameters, p)
			}
		}
	}
	codecs, err := (ParamCodecCompiler{Component: local.publicComponent, InputType: c.input.InputType, LookupType: c.input.TypeLookup}).Describe()
	if err != nil {
		return nil, err
	}
	bindings, err := buildBindingSpecs(local.publicComponent, c.input.InputType, codecs, c.input.TypeLookup)
	if err != nil {
		return nil, err
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		return nil, err
	}
	plan, err := injector.CompilePlan(c.input.InputType, bindings...)
	if err != nil {
		return nil, fmt.Errorf("compile source business input plan: %w", err)
	}
	projection, err := local.compileProjection(plan)
	if err != nil {
		return nil, err
	}
	wireTypes, err := local.compileWireTypes(bindings)
	if err != nil {
		return nil, err
	}
	description, err := (routeContractCompiler{component: c.input.Component, inputType: c.input.InputType, bindings: bindings, injector: injector, canonicalPlan: plan, wireTypes: wireTypes}).compile(projection)
	if err != nil {
		return nil, err
	}
	return &Result{Bindings: bindings, Input: description}, nil
}

// Structural correspondence is local authoring evidence, never runtime identity.
func sourceDescriptionShape(descriptor *x.Type, actual reflect.Type, types *typecatalog.Resolver, seen map[string]bool) error {
	if descriptor == nil || actual == nil {
		return fmt.Errorf("source description shape missing")
	}
	if descriptor.Type != nil {
		if descriptor.Type != actual {
			return fmt.Errorf("source description linked shape mismatch: %s", descriptor.Key())
		}
		return nil
	}
	key := descriptor.Key() + "/" + actual.String()
	if seen[key] {
		return nil
	}
	seen[key] = true
	fields, err := xshape.New(descriptor, types.Descriptor).Fields()
	if err != nil {
		return err
	}
	if actual.Kind() != reflect.Struct || len(fields) != actual.NumField() {
		return fmt.Errorf("source description field coverage mismatch: %s", descriptor.Key())
	}
	for i, field := range fields {
		candidate := actual.Field(i)
		if field.Name != candidate.Name || field.Tag != candidate.Tag || field.Anonymous != candidate.Anonymous || field.Exported != (candidate.PkgPath == "") {
			return fmt.Errorf("source description field metadata mismatch: %s.%s", descriptor.Key(), field.Name)
		}
		expression, err := field.CanonicalType()
		if err != nil {
			return err
		}
		runtime := xshape.Runtime{Lookup: func(name string) (reflect.Type, error) {
			child, err := types.Descriptor(name)
			if err != nil {
				return nil, err
			}
			if child == nil {
				return nil, fmt.Errorf("source description unresolved type %s", name)
			}
			if child.Type != nil {
				return child.Type, nil
			}
			leaf := candidate.Type
			for leaf.Kind() == reflect.Pointer || leaf.Kind() == reflect.Slice || leaf.Kind() == reflect.Array {
				leaf = leaf.Elem()
			}
			if err := sourceDescriptionShape(child, leaf, types, seen); err != nil {
				return nil, err
			}
			return leaf, nil
		}}
		expected, err := runtime.Type(expression)
		if err != nil {
			return fmt.Errorf("source description field %s: %w", field.Name, err)
		}
		if expected != candidate.Type {
			return fmt.Errorf("source description field type mismatch: %s.%s", descriptor.Key(), field.Name)
		}
	}
	return nil
}
