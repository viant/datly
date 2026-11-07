package openapi

import (
	"encoding/json"
	"fmt"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/tagly/format"
	xshape "github.com/viant/x/shape"
	"reflect"
	"strings"
)

// A static named JSON body scope stages value resolution, not transport selection.
// Dynamic authority and other provider kinds retain their existing rejection.
func stagedNamedBody(field registry.InputField) bool {
	b := field.Binding()
	return b.Scope != "" && b.When == "" && b.With == "" && b.Location.Kind == "body" && b.Location.In != "" && field.SourceType() != nil && b.BodyNullPolicy == ""
}

func (b *bodyBuilder) composeStagedBody() (*openapi3.Schema, error) {
	fail := func() (*openapi3.Schema, error) {
		return nil, fmt.Errorf("whole-body and selected-body bindings overlap or lack static disjoint native object authority")
	}
	if b.wholeField == nil || b.media != "application/json" {
		return fail()
	}
	for _, staged := range b.stagedProperties {
		if !staged {
			return fail()
		}
	}
	f := *b.wholeField
	binding := f.Binding()
	t := f.SourceType()
	if t == nil || binding.Transformer != nil || f.WireType() != nil || f.WireSchema() != nil || len(f.WireSchemas()) != 0 || binding.Scope != "" || binding.When != "" || binding.With != "" {
		return fail()
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct || t.Implements(reflect.TypeFor[json.Unmarshaler]()) || reflect.PointerTo(t).Implements(reflect.TypeFor[json.Unmarshaler]()) {
		return fail()
	}
	// Use the same linked JSON field metadata as the ordinary object projection.
	fields, err := xshape.Linked(t).JSONFields()
	if err != nil {
		return nil, err
	}
	if f.Representation() != nil && f.Representation().Changed() {
		return fail()
	}
	var owned []string
	for _, field := range fields {
		owned = append(owned, field.Name)
		policy, err := format.Parse(field.Field.Tag)
		if err != nil {
			return nil, err
		}
		owned = append(owned, strings.Split(policy.InputNames, "|")...)
		owned = append(owned, strings.Split(policy.InputNamesExclude, "|")...)
	}
	for name := range b.properties {
		for _, other := range owned {
			if strings.EqualFold(name, other) {
				return fail()
			}
		}
	}
	original := b.whole
	nullable := false
	if len(original.AnyOf) == 2 && original.AnyOf[1] != nil && original.AnyOf[1].Nullable && len(original.AnyOf[1].Enum) == 1 && original.AnyOf[1].Enum[0] == nil {
		original = original.AnyOf[0]
		nullable = true
	}
	if original.Ref != "" {
		original = b.schemas[strings.TrimPrefix(original.Ref, "#/components/schemas/")]
	}
	if original == nil || original.Type != "object" || len(original.AllOf) != 0 || len(original.AnyOf) != 0 || len(original.OneOf) != 0 || original.Not != nil || original.AdditionalProperties != nil || original.AdditionalPropertiesAllowed != nil {
		return fail()
	}
	merged := *original
	merged.Properties = openapi3.Schemas{}
	merged.Required = append([]string(nil), original.Required...)
	for key, value := range original.Properties {
		merged.Properties[key] = value
	}
	for key, value := range b.properties {
		for previous := range merged.Properties {
			if strings.EqualFold(previous, key) {
				return fail()
			}
		}
		merged.Properties[key] = value
	}
	merged.Description = strings.TrimSpace(merged.Description + " Named body properties may be resolved in handler-selected execution scopes; suppliedness and conditional required validation remain runtime responsibilities.")
	if nullable {
		return &openapi3.Schema{AnyOf: openapi3.SchemaList{&merged, b.whole.AnyOf[1]}}, nil
	}
	return &merged, nil
}
