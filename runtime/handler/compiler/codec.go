package compiler

import (
	"fmt"
	"io/fs"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	xcodec "github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
)

// ParamCodec keeps the provider-facing source contract beside the executable
// codec so plan compilation cannot discard either half of the transformation.
type ParamCodec struct {
	Instance   xcodec.Instance
	SourceType reflect.Type
}

// ParamCodecCompiler resolves codec source contracts before constructing their
// executable transformations.
type ParamCodecCompiler struct {
	Component  *spec.Component
	InputType  reflect.Type
	Factory    xcodec.Factory
	Resources  fs.FS
	LookupType func(string) (reflect.Type, error)
}

func (c ParamCodecCompiler) Build() (map[string]ParamCodec, error) {
	if c.Component == nil || c.InputType == nil || c.InputType.Kind() != reflect.Struct {
		return nil, nil
	}
	fields, err := newContractFields(c.InputType)
	if err != nil {
		return nil, err
	}
	if c.Factory == nil {
		for _, param := range spec.EffectiveParameters(c.Component.Parameters) {
			if param != nil && param.Codec != nil {
				return nil, fmt.Errorf("codec %q required by param %q but no codec factory provided", param.Codec.Body, param.Name)
			}
		}
		return nil, nil
	}
	var result map[string]ParamCodec
	for _, param := range spec.EffectiveParameters(c.Component.Parameters) {
		if param == nil || param.Codec == nil {
			continue
		}
		resolved, ok, err := fields.resolve(param)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("codec param field not found: %s", param.Name)
		}
		field := resolved.field
		sourceType, err := c.sourceType(fields, param, field.Type)
		if err != nil {
			return nil, err
		}
		instance, err := c.Factory.New(&xcodec.Config{
			Body:                 param.Codec.Body,
			SourceType:           sourceType,
			DestinationType:      field.Type,
			Args:                 append([]string(nil), param.Codec.Args...),
			OutputTypeExpression: param.Codec.OutputType,
		}, xcodec.WithResourceFS(c.Resources))
		if err != nil {
			return nil, fmt.Errorf("build codec for %s: %w", param.Name, err)
		}
		if result == nil {
			result = map[string]ParamCodec{}
		}
		result[field.Name] = ParamCodec{Instance: instance, SourceType: sourceType}
	}
	return result, nil
}

func (c ParamCodecCompiler) sourceType(fields *contractFields, param *spec.Parameter, destination reflect.Type) (reflect.Type, error) {
	if param == nil {
		return nil, fmt.Errorf("codec parameter is required")
	}
	if !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "param") {
		expression := strings.TrimSpace(param.TypeExpr)
		if expression == "" {
			return nil, fmt.Errorf("codec param %q source type is required", param.Name)
		}
		options := []xreflect.Option(nil)
		if c.LookupType != nil {
			options = append(options, xreflect.WithTypeLookup(func(name string, _ ...xreflect.Option) (reflect.Type, error) {
				return c.LookupType(name)
			}))
		}
		sourceType, err := xreflect.Parse(expression, options...)
		if err != nil {
			return nil, fmt.Errorf("codec param %q source type %q: %w", param.Name, expression, err)
		}
		if sourceType == nil {
			return nil, fmt.Errorf("codec param %q source type %q was not resolved", param.Name, expression)
		}
		return sourceType, nil
	}
	name := strings.TrimSpace(param.Source.Name)
	var source *spec.Parameter
	for _, candidate := range spec.EffectiveParameters(c.Component.Parameters) {
		if !isInputParam(candidate) || !strings.EqualFold(strings.TrimSpace(candidate.Name), name) {
			continue
		}
		if source != nil {
			return nil, fmt.Errorf("codec param %q source %q is ambiguous", param.Name, name)
		}
		source = candidate
	}
	if source == nil {
		return nil, fmt.Errorf("codec param %q source %q was not found", param.Name, name)
	}
	resolved, ok, err := fields.resolve(source)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("codec param %q source field %q was not found", param.Name, name)
	}
	return resolved.field.Type, nil
}
