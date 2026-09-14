package tag

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

type BindingField struct {
	Field   reflect.StructField
	Binding bindly.BindingSpec
	Tagged  bool
}

// BindingIndex is the parsed Bindly field metadata for one input contract.
type BindingIndex struct {
	typeOf reflect.Type
	fields []BindingField
}

func NewBindingIndex(structType reflect.Type) (*BindingIndex, error) {
	for structType != nil && structType.Kind() == reflect.Pointer {
		structType = structType.Elem()
	}
	if structType == nil || structType.Kind() != reflect.Struct {
		return &BindingIndex{typeOf: structType}, nil
	}
	result := &BindingIndex{typeOf: structType, fields: make([]BindingField, 0, structType.NumField())}
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		if !field.IsExported() {
			continue
		}
		binding, tagged, err := bindly.BindingSpecFromField(field)
		if err != nil {
			return nil, fmt.Errorf("parse bind tag for %s: %w", field.Name, err)
		}
		result.fields = append(result.fields, BindingField{Field: field, Binding: binding, Tagged: tagged})
	}
	return result, nil
}

func (i *BindingIndex) Fields() []BindingField {
	if i == nil || len(i.fields) == 0 {
		return nil
	}
	return append([]BindingField(nil), i.fields...)
}

// Resolve matches a canonical parameter to a Go input field. Structural names
// take precedence for ordinary parameters. View selectors first match their
// explicit source because different views can share a logical property name.
// Bindly aliases and source locations handle remaining package field matches.
func (i *BindingIndex) Resolve(param *spec.Parameter) (reflect.StructField, bool, error) {
	if i == nil || param == nil || i.typeOf == nil || i.typeOf.Kind() != reflect.Struct {
		return reflect.StructField{}, false, nil
	}
	if param.QuerySelector != nil && hasBindingSource(param.Source) {
		matches := matchBindingFields(i.fields, func(field BindingField) bool {
			return field.Tagged && sameBindingSource(field.Binding, param.Source) && strings.TrimSpace(field.Binding.Location.In) == strings.TrimSpace(param.Source.Name)
		})
		if len(matches) > 1 {
			return reflect.StructField{}, false, fmt.Errorf("selector parameter %q matches multiple input fields", param.Name)
		}
		if len(matches) == 1 {
			return matches[0].Field, true, nil
		}
		// Unlocated physical fields can receive their source from DQL. A
		// located field from another view must never win via a logical alias.
		for _, field := range i.fields {
			if field.Field.Name == param.Name && (!field.Tagged || field.Binding.Location.Kind == "") {
				return field.Field, true, nil
			}
		}
		return reflect.StructField{}, false, fmt.Errorf("selector parameter %q has no input field for %s/%s", param.Name, param.Source.Kind, param.Source.Name)
	}
	if field, ok := typecatalog.FieldByName(i.typeOf, param.Name); ok && field.IsExported() {
		return field, true, nil
	}
	matches := matchBindingFields(i.fields, func(field BindingField) bool {
		return field.Tagged && strings.EqualFold(bindingFieldName(field), strings.TrimSpace(param.Name))
	})
	if len(matches) > 1 && hasBindingSource(param.Source) {
		matches = matchBindingFields(matches, func(field BindingField) bool {
			return sameBindingSource(field.Binding, param.Source)
		})
	}
	if len(matches) == 0 && hasBindingSource(param.Source) {
		matches = matchBindingFields(i.fields, func(field BindingField) bool {
			return field.Tagged && sameBindingSource(field.Binding, param.Source)
		})
	}
	if len(matches) == 0 {
		return reflect.StructField{}, false, nil
	}
	if len(matches) > 1 {
		return reflect.StructField{}, false, fmt.Errorf("parameter %q matches multiple input fields", param.Name)
	}
	return matches[0].Field, true, nil
}

func matchBindingFields(fields []BindingField, match func(BindingField) bool) []BindingField {
	result := make([]BindingField, 0, len(fields))
	for _, field := range fields {
		if match(field) {
			result = append(result, field)
		}
	}
	return result
}

func bindingFieldName(field BindingField) string {
	if name := strings.TrimSpace(field.Binding.Name); name != "" {
		return name
	}
	return field.Field.Name
}

func hasBindingSource(source spec.BindSource) bool {
	return strings.TrimSpace(source.Kind) != "" && strings.TrimSpace(source.Name) != ""
}

func sameBindingSource(binding bindly.BindingSpec, source spec.BindSource) bool {
	return strings.EqualFold(strings.TrimSpace(binding.Location.Kind), strings.TrimSpace(source.Kind)) &&
		strings.EqualFold(strings.TrimSpace(binding.Location.In), strings.TrimSpace(source.Name))
}
