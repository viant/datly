package output

import (
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/francoispqt/gojay"
	xshape "github.com/viant/x/shape"
)

// ErrSchemaUnavailable means the encoder cannot be described by a static schema.
// Consumers may omit optional discovery metadata, but must not substitute Go names.
var ErrSchemaUnavailable = errors.New("output JSON schema unavailable")

// JSONSchema describes JSON output using the compiled presentation settings.
// Unlike Wire, this discovery path permits dynamic leaves (including Status.Error).
// Field naming is kept here, beside the encoder, rather than in MCP input binding.
func (p *Plan) JSONSchema() (map[string]any, error) {
	if p == nil || p.typeOf == nil {
		return nil, nil
	}
	if p.custom != nil || p.TransportReady() {
		return nil, ErrSchemaUnavailable
	}
	t := p.typeOf
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	b := jsonSchemaBuilder{plan: p, active: map[reflect.Type]string{}}
	return b.value(t, "#", "")
}

type jsonSchemaBuilder struct {
	plan   *Plan
	active map[reflect.Type]string
}

func (b *jsonSchemaBuilder) value(t reflect.Type, ref, path string) (map[string]any, error) {
	if t.Kind() == reflect.Pointer {
		item, err := b.value(t.Elem(), ref, path)
		return nullableJSONSchema(item), err
	}
	if t == reflect.TypeFor[time.Time]() {
		layout := b.plan.timeLayout
		if layout == "" {
			layout = time.RFC3339Nano
		}
		return timeJSONSchema(layout), nil
	}
	if hasJSONSchemaMarshaler(t) || (b.plan.transformedJSON() && hasNativeSchemaMarshaler(t)) || t.Kind() == reflect.Interface {
		return map[string]any{}, nil
	}
	if previous, ok := b.active[t]; ok {
		if len(b.plan.exclude) > 0 {
			return nil, fmt.Errorf("%w: recursive type %s with path exclusions", ErrSchemaUnavailable, t)
		}
		return map[string]any{"$ref": previous}, nil
	}
	b.active[t] = ref
	defer delete(b.active, t)
	result := map[string]any{}
	switch t.Kind() {
	case reflect.Struct:
		fields, err := b.plan.jsonSchemaFields(t, path, false, map[reflect.Type]bool{})
		if err != nil {
			return nil, err
		}
		properties := map[string]any{}
		var required []string
		for _, field := range fields {
			if _, exists := properties[field.name]; exists {
				return nil, fmt.Errorf("%w: JSON name collision %q on %s", ErrSchemaUnavailable, field.name, t)
			}
			item, err := b.value(field.field.Type, ref+"/properties/"+schemaPointerToken(field.name), field.path)
			if err != nil {
				return nil, err
			}
			if field.quoted && !hasJSONSchemaMarshaler(field.field.Type) {
				item = map[string]any{"type": "string"}
				if field.field.Type.Kind() == reflect.Pointer {
					item = nullableJSONSchema(item)
				}
			}
			base := field.field.Type
			for base.Kind() == reflect.Pointer {
				base = base.Elem()
			}
			if field.timeLayout != "" && base == reflect.TypeFor[time.Time]() {
				item = timeJSONSchema(field.timeLayout)
				if field.field.Type.Kind() == reflect.Pointer {
					item = nullableJSONSchema(item)
				}
			}
			if field.nullable {
				item = nullableJSONSchema(item)
			}
			if description := field.field.Tag.Get("description"); description != "" {
				item["description"] = description
			}
			properties[field.name] = item
			if field.required {
				required = append(required, field.name)
			}
		}
		result["type"], result["properties"] = "object", properties
		if len(required) > 0 {
			result["required"] = required
		}
	case reflect.Slice, reflect.Array:
		if !b.plan.transformedJSON() && t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 && !hasJSONSchemaMarshaler(t.Elem()) {
			return nullableJSONSchema(map[string]any{"type": "string", "format": "byte"}), nil
		}
		item, err := b.value(t.Elem(), ref+"/items", path)
		if err != nil {
			return nil, err
		}
		result["type"], result["items"] = "array", item
		if t.Kind() == reflect.Array {
			result["minItems"], result["maxItems"] = t.Len(), t.Len()
		} else {
			result = nullableJSONSchema(result)
		}
	case reflect.Map:
		item, err := b.value(t.Elem(), ref+"/additionalProperties", path)
		if err != nil {
			return nil, err
		}
		result["type"], result["additionalProperties"] = "object", item
		result = nullableJSONSchema(result)
	case reflect.String:
		result["type"] = "string"
	case reflect.Bool:
		result["type"] = "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		result["type"] = "integer"
	case reflect.Float32, reflect.Float64:
		result["type"] = "number"
	default:
		return nil, fmt.Errorf("%w: unsupported type %s", ErrSchemaUnavailable, t)
	}
	return result, nil
}

func hasJSONSchemaMarshaler(t reflect.Type) bool {
	for _, contract := range []reflect.Type{reflect.TypeFor[json.Marshaler](), reflect.TypeFor[encoding.TextMarshaler]()} {
		if t.Implements(contract) || reflect.PointerTo(t).Implements(contract) {
			return true
		}
	}
	return false
}

func hasNativeSchemaMarshaler(t reflect.Type) bool {
	for _, contract := range []reflect.Type{reflect.TypeFor[gojay.MarshalerJSONObject](), reflect.TypeFor[gojay.MarshalerJSONArray]()} {
		if t.Implements(contract) || reflect.PointerTo(t).Implements(contract) {
			return true
		}
	}
	return false
}

func nullableJSONSchema(source map[string]any) map[string]any {
	if source == nil || len(source) == 0 {
		return source
	}
	if kind, ok := source["type"].(string); ok {
		source["type"] = []string{kind, "null"}
	} else if source["$ref"] != nil {
		return map[string]any{"anyOf": []any{source, map[string]any{"type": "null"}}}
	}
	return source
}

func timeJSONSchema(layout string) map[string]any {
	result := map[string]any{"type": "string"}
	switch layout {
	case time.RFC3339, time.RFC3339Nano:
		result["format"] = "date-time"
	case "2006-01-02":
		result["format"] = "date"
	}
	return result
}

func schemaPointerToken(name string) string {
	return strings.ReplaceAll(strings.ReplaceAll(name, "~", "~0"), "/", "~1")
}

// Standard JSON and transformed JSON have different embedding and tag rules.
// Standard field dominance remains owned by x/shape, just as in Structology.
func (p *Plan) standardSchemaFields(t reflect.Type, path string) ([]jsonSchemaField, error) {
	fields, err := xshape.Linked(t).JSONFields()
	if err != nil {
		return nil, err
	}
	result := make([]jsonSchemaField, 0, len(fields))
	for _, field := range fields {
		result = append(result, jsonSchemaField{field: field.Field.StructField(), name: field.Name,
			path: schemaFieldPath(path, field.Field.Name), required: !field.MayOmit(), quoted: field.Quoted})
	}
	return result, nil
}
