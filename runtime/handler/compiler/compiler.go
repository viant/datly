package compiler

import (
	"fmt"
	"io/fs"
	"reflect"

	"github.com/viant/bindly"
	"github.com/viant/datly/internal/selectorcompanion"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	xcodec "github.com/viant/xdatly/codec"
)

type Input struct {
	Component    *spec.Component
	InputType    reflect.Type
	CodecFactory xcodec.Factory
	Resources    fs.FS
	TypeLookup   func(string) (reflect.Type, error)
}

type Result struct {
	SelectorPlan *selectorcompanion.BindingPlan
	Bindings     []bindly.BindingSpec
	Input        *registry.InputContract
}

// Compiler builds canonical binding metadata and route-effective input
// contracts from one resolved component contract.
type Compiler struct {
	selection       *selectorcompanion.Selection
	selected        bool
	selectionErr    error
	publicComponent *spec.Component
	input           Input
}

func New(input Input) *Compiler {
	return &Compiler{input: input}
}

// CompileBindings builds codec-aware binding metadata without requiring route
// exposure. SQL reader compilation consumes this phase directly.
func (c *Compiler) CompileBindings() ([]bindly.BindingSpec, error) {
	if c == nil {
		return nil, fmt.Errorf("handler input compiler is required")
	}
	input := c.input
	if err := c.selectCompanion(); err != nil {
		return nil, err
	}
	codecs, err := (ParamCodecCompiler{
		Component: input.Component, InputType: input.InputType, Factory: input.CodecFactory,
		Resources: input.Resources, LookupType: input.TypeLookup,
	}).Build()
	if err != nil {
		return nil, err
	}
	return BuildBindingSpecs(c.publicComponent, input.InputType, codecs)
}

func (c *Compiler) Compile() (*Result, error) { return c.compile(false) }

// Describe compiles binding/schema metadata without constructing codecs. It is
// an authoring surface: the returned contract must not be used for execution.
func (c *Compiler) Describe() (*Result, error) { return c.compile(true) }

func (c *Compiler) compile(describe bool) (*Result, error) {
	if c == nil {
		return nil, fmt.Errorf("handler input compiler is required")
	}
	if err := c.selectCompanion(); err != nil {
		return nil, err
	}
	var bindings []bindly.BindingSpec
	var err error
	if describe {
		var codecs map[string]ParamCodec
		codecs, err = (ParamCodecCompiler{Component: c.input.Component, InputType: c.input.InputType, LookupType: c.input.TypeLookup}).Describe()
		if err == nil {
			bindings, err = BuildBindingSpecs(c.publicComponent, c.input.InputType, codecs)
		}
	} else {
		bindings, err = c.CompileBindings()
	}
	if err != nil {
		return nil, err
	}
	result := &Result{Bindings: bindings}
	if c.input.InputType == nil {
		return result, nil
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		return nil, err
	}
	plan, err := injector.CompilePlan(c.input.InputType, bindings...)
	if err != nil {
		return nil, fmt.Errorf("compile canonical input binding plan: %w", err)
	}
	if c.selection != nil {
		companion := *c.input.Component
		companion.Parameters = c.selection.Parameters
		bindings, bindErr := BuildBindingSpecs(&companion, c.selection.Type, nil)
		if bindErr != nil {
			return nil, bindErr
		}
		companionPlan, bindErr := injector.CompilePlan(c.selection.Type, bindings...)
		if bindErr != nil {
			return nil, bindErr
		}
		result.SelectorPlan, bindErr = selectorcompanion.NewBindingPlan(c.input.Component, c.selection, c.input.InputType, plan, companionPlan, result.Bindings, bindings)
		if bindErr != nil {
			return nil, bindErr
		}
	}
	projection, err := c.compileProjection(plan)
	if err != nil {
		return nil, err
	}
	wireTypes, err := c.compileWireTypes(bindings)
	if err != nil {
		return nil, err
	}
	result.Input, err = (routeContractCompiler{
		component: c.input.Component, inputType: c.input.InputType, bindings: bindings,
		injector: injector, canonicalPlan: plan, wireTypes: wireTypes, selectors: result.SelectorPlan,
	}).compile(projection)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Compiler) compileProjection(plan *bindly.Plan) (*bindly.Projection, error) {
	fields, err := newContractFields(c.input.InputType)
	if err != nil {
		return nil, fmt.Errorf("compile input value projection: %w", err)
	}
	var params []*spec.Parameter
	if c.publicComponent != nil {
		params = c.publicComponent.Parameters
	}
	projected := make([]bindly.ProjectionField, 0, len(params))
	for _, param := range spec.EffectiveParameters(params) {
		if !isInputParam(param) {
			continue
		}
		field, ok, resolveErr := fields.resolve(param)
		if resolveErr != nil {
			return nil, fmt.Errorf("compile input value projection for %s: %w", param.Name, resolveErr)
		}
		if !ok {
			continue
		}
		projected = append(projected, bindly.ProjectionField{
			Path: field.field.Name, Names: tag.CanonicalBindingAliases(field.field, param),
		})
	}
	projection, err := plan.Projection(projected...)
	if err != nil {
		return nil, fmt.Errorf("compile input value projection: %w", err)
	}
	return projection, nil
}

func (c *Compiler) selectCompanion() error {
	if c.selected {
		return c.selectionErr
	}
	c.selected = true
	c.selection, c.selectionErr = selectorcompanion.Select(c.input.Component, c.input.InputType)
	if c.selectionErr != nil {
		return c.selectionErr
	}
	c.publicComponent = c.input.Component
	if c.selection != nil {
		component := *c.input.Component
		component.Parameters = nil
		for _, p := range c.input.Component.Parameters {
			if !c.selection.Contains(p) {
				component.Parameters = append(component.Parameters, p)
			}
		}
		c.publicComponent = &component
	}
	return nil
}
