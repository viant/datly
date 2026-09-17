package compiler

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/bindly/xform"
	"github.com/viant/datly/spec"
	xcodec "github.com/viant/xdatly/codec"
)

// BuildBindingSpecs compiles package tags and component parameter metadata
// into Bindly's single registration-time binding model. Component metadata
// overrides matching tag metadata; tags can complete an omitted source name
// only when both sources use the same kind.
func BuildBindingSpecs(component *spec.Component, inputType reflect.Type, codecs map[string]ParamCodec) ([]bindly.BindingSpec, error) {
	if inputType == nil || inputType.Kind() != reflect.Struct {
		return nil, nil
	}
	fields, err := newContractFields(inputType)
	if err != nil {
		return nil, err
	}
	fieldParams, err := fields.fieldParams(component)
	if err != nil {
		return nil, err
	}
	for _, param := range spec.EffectiveParameters(component.Parameters) {
		if param == nil || param.EmitOutput || !param.IsTransportInput() {
			continue
		}
		if _, ok, resolveErr := fields.resolve(param); resolveErr != nil {
			return nil, resolveErr
		} else if !ok {
			return nil, fmt.Errorf("binding param field not found: %s", param.Name)
		}
	}
	result := make([]bindly.BindingSpec, 0, inputType.NumField())
	for _, item := range fields.items {
		field, tagged, hasTag := item.field, item.binding, item.tagged
		param := fieldParams[field.Name]
		if param == nil {
			if hasTag {
				if err := applyTimeFormat(field, &tagged); err != nil {
					return nil, err
				}
				result = append(result, tagged)
			}
			continue
		}
		compiled, ok, err := bindingSpecFromParam(field, param, tagged, hasTag, codecs[field.Name])
		if err != nil {
			return nil, err
		}
		if ok {
			if err := applyTimeFormat(field, &compiled); err != nil {
				return nil, err
			}
			result = append(result, compiled)
		}
	}
	return result, nil
}

func bindingSpecFromParam(field reflect.StructField, param *spec.Parameter, tagged bindly.BindingSpec, hasTag bool, codec ParamCodec) (bindly.BindingSpec, bool, error) {
	kind := strings.ToLower(strings.TrimSpace(param.Source.Kind))
	var defaultValue any
	if param.Value != nil {
		defaultValue = *param.Value
	}
	if kind == "" {
		if !hasTag {
			return bindly.BindingSpec{}, false, nil
		}
		tagged.Name = param.Name
		tagged.Required = cloneBool(param.Required)
		tagged.Cacheable = cloneBool(param.Cacheable)
		tagged.MinAllowedRecords, tagged.MaxAllowedRecords, tagged.ExpectedReturned = param.MinAllowedRecords, param.MaxAllowedRecords, param.ExpectedReturned
		tagged.When = param.When
		tagged.Scope = param.Scope
		tagged.With = param.With
		tagged.URI = activationURI(param)
		tagged.ResourceRef = param.ResourceRef
		tagged.Async = param.Async
		tagged.DefaultValue = defaultValue
		tagged.DataType = param.TypeExpr
		tagged.ErrorCode = param.ErrorStatusCode
		tagged.ErrorMessage = param.ErrorMessage
		tagged.Cardinality = param.Cardinality
		tagged.Extension = param
		tagged.SourceType = codec.SourceType
		tagged.Transformer = newCodecTransformer(codec.Instance)
		return tagged, true, nil
	}
	name := strings.TrimSpace(param.Source.Name)
	if name == "" && hasTag && strings.EqualFold(tagged.Location.Kind, kind) {
		name = tagged.Location.In
	}
	if name == "" && sourceNameRequired(kind) {
		return bindly.BindingSpec{}, false, fmt.Errorf("missing source name for param %s", param.Name)
	}
	bindingName := param.Name
	if param.QuerySelector != nil {
		bindingName = field.Name
	}
	return bindly.BindingSpec{
		Path:              field.Name,
		SourceType:        codec.SourceType,
		Name:              bindingName,
		Location:          bindstate.Location{Kind: kind, In: name},
		Required:          cloneBool(param.Required),
		Cacheable:         cloneBool(param.Cacheable),
		MinAllowedRecords: param.MinAllowedRecords, MaxAllowedRecords: param.MaxAllowedRecords, ExpectedReturned: param.ExpectedReturned,
		When:         param.When,
		Scope:        param.Scope,
		With:         param.With,
		URI:          activationURI(param),
		ResourceRef:  param.ResourceRef,
		Async:        param.Async,
		DefaultValue: defaultValue,
		DataType:     param.TypeExpr,
		ErrorCode:    param.ErrorStatusCode,
		ErrorMessage: param.ErrorMessage,
		Cardinality:  param.Cardinality,
		Extension:    param,
		Transformer:  newCodecTransformer(codec.Instance),
	}, true, nil
}

func activationURI(param *spec.Parameter) string {
	if param == nil || param.Activation == nil {
		return ""
	}
	return strings.TrimSpace(param.Activation.URI)
}

func sourceNameRequired(kind string) bool {
	switch kind {
	case "query", "path", "header", "cookie", "form", "component":
		return true
	default:
		return false
	}
}

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

type codecTransformer struct {
	codec xcodec.Instance
}

func newCodecTransformer(codec xcodec.Instance) xform.Transformer {
	if codec == nil {
		return nil
	}
	return &codecTransformer{codec: codec}
}

func (t *codecTransformer) Transform(ctx context.Context, _ locator.Resolver, input any) (any, error) {
	// Bindly already converts to the declared codec source type. Do not collapse
	// collection inputs; scalar codecs receive their declared scalar type.
	return t.codec.Value(ctx, input)
}
