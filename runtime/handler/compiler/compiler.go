package compiler

import (
	"fmt"
	"io/fs"
	"reflect"

	"github.com/viant/bindly"
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
	Bindings []bindly.BindingSpec
	Input    *registry.InputContract
}

// Compiler builds canonical binding metadata and route-effective input
// contracts from one resolved component contract.
type Compiler struct {
	input Input
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
	codecs, err := (ParamCodecCompiler{
		Component: input.Component, InputType: input.InputType, Factory: input.CodecFactory,
		Resources: input.Resources, LookupType: input.TypeLookup,
	}).Build()
	if err != nil {
		return nil, err
	}
	return BuildBindingSpecs(input.Component, input.InputType, codecs)
}

func (c *Compiler) Compile() (*Result, error) {
	if c == nil {
		return nil, fmt.Errorf("handler input compiler is required")
	}
	bindings, err := c.CompileBindings()
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
	projection, err := c.compileProjection(plan)
	if err != nil {
		return nil, err
	}
	result.Input, err = (routeContractCompiler{
		component: c.input.Component, inputType: c.input.InputType, bindings: bindings,
		injector: injector, canonicalPlan: plan,
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
	if c.input.Component != nil {
		params = c.input.Component.Parameters
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
			Path: field.field.Name, Names: tag.BindingAliases(field.field, param),
		})
	}
	projection, err := plan.Projection(projected...)
	if err != nil {
		return nil, fmt.Errorf("compile input value projection: %w", err)
	}
	return projection, nil
}
