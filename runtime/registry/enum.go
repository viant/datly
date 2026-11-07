package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/xform"
	"github.com/viant/tagly/choice"
)

// EnumBinding composes field-level enum wire conversion with native binding.
// Existing explicit codecs must declare participation rather than be bypassed.
func EnumBinding(binding bindly.BindingSpec, field reflect.StructField) (bindly.BindingSpec, error) {
	plan, err := choice.ParseEnum(field.Tag, field.Type)
	if err != nil {
		return binding, err
	}
	switch binding.Location.Kind {
	case "body", "query", "form", "path", "header", "cookie":
	default:
		return binding, nil
	}
	previousEnum, _ := binding.Transformer.(*enumInput)
	if aware, ok := binding.Transformer.(interface {
		WithEnum(*choice.Enum) (xform.Transformer, error)
	}); ok {
		binding.Transformer, err = aware.WithEnum(plan)
		if binding.Transformer == nil {
			binding.SourceType = nil
			if previousEnum != nil {
				binding.SourceType = previousEnum.originalSource
			}
		} else if selected, ok := binding.Transformer.(*enumInput); ok {
			binding.SourceType = selected.sourceType()
		}
		return binding, err
	}
	if plan == nil {
		return binding, nil
	}
	if binding.Transformer != nil {
		return binding, fmt.Errorf("enum field %s conflicts with its explicit codec", field.Name)
	}
	transform := &enumInput{plan: plan, target: field.Type, raw: binding.Location.Kind == "body", originalSource: binding.SourceType}
	binding.SourceType = transform.sourceType()
	binding.Transformer = transform
	return binding, nil
}

type enumInput struct {
	raw            bool
	originalSource reflect.Type
	plan           *choice.Enum
	target         reflect.Type
}

func (e *enumInput) WireSourceType() reflect.Type {
	if e.plan.Accept == choice.Scalar {
		return reflect.TypeFor[any]()
	}
	if e.plan.Accept == choice.Literal {
		return reflect.TypeFor[string]()
	}
	return reflect.TypeFor[int64]()
}
func (e *enumInput) WithEnum(plan *choice.Enum) (xform.Transformer, error) {
	if plan == nil {
		return nil, nil
	}
	copy := *e
	copy.plan = plan
	return &copy, nil
}
func (e *enumInput) Transform(_ context.Context, _ locator.Resolver, value any) (any, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	converted, err := e.plan.DecodeJSON(raw)
	if err != nil {
		return nil, err
	}
	target := reflect.New(e.target)
	if err = json.Unmarshal(converted, target.Interface()); err != nil {
		return nil, err
	}
	return target.Elem().Interface(), nil
}

func (e *enumInput) sourceType() reflect.Type {
	if e.raw {
		return reflect.TypeFor[json.RawMessage]()
	}
	if e.plan.Accept == choice.Literal || e.plan.Accept == choice.Scalar {
		return reflect.TypeFor[string]()
	}
	return reflect.TypeFor[json.Number]()
}
