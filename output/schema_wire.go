package output

import (
	"fmt"
	jsonmarshal "github.com/viant/structology/encoding/json/marshal"
	"reflect"
)

// nativeSchema lowers the compiled serializer's facts, including inline-map
// collision order. It does not infer naming or omission from Go tags.
func (p *Plan) nativeSchema(s *jsonmarshal.WireShape, ref, path string, active map[*jsonmarshal.WireShape]string) (map[string]any, error) {
	if previous, ok := active[s]; ok {
		return map[string]any{"$ref": previous}, nil
	}
	if s.Kind() == reflect.Pointer {
		value, err := p.nativeSchema(s.Element(), ref, path, active)
		return nullableJSONSchema(value), err
	}
	active[s] = ref
	defer delete(active, s)
	result := map[string]any{}
	switch s.Kind() {
	case reflect.Interface:
		return result, nil
	case reflect.Struct:
		properties := map[string]any{}
		required := []string{}
		for _, field := range s.Properties() {
			fieldPath := path
			source := s.Source()
			for source.Kind() == reflect.Pointer {
				source = source.Elem()
			}
			for _, index := range field.Field().Index {
				f := source.Field(index)
				fieldPath = schemaFieldPath(fieldPath, f.Name)
				source = f.Type
				for source.Kind() == reflect.Pointer {
					source = source.Elem()
				}
			}
			value, err := p.nativeSchema(field.Shape(), ref+"/properties/"+schemaPointerToken(field.Name()), fieldPath, active)
			if err != nil {
				return nil, err
			}
			if description := field.Field().Tag.Get("description"); description != "" {
				value["description"] = description
			}
			properties[field.Name()] = value
			if field.Required() && !p.selectableRowPath(path) {
				required = append(required, field.Name())
			}
		}
		result["type"], result["properties"] = "object", properties
		if len(required) > 0 {
			result["required"] = required
		}
		if s.Additional() != nil {
			value, err := p.nativeSchema(s.Additional(), ref+"/additionalProperties", path, active)
			if err != nil {
				return nil, err
			}
			result["additionalProperties"] = value
		}
	case reflect.Map:
		value, err := p.nativeSchema(s.Element(), ref+"/additionalProperties", path, active)
		if err != nil {
			return nil, err
		}
		result["type"], result["additionalProperties"] = "object", value
	case reflect.Slice, reflect.Array:
		value, err := p.nativeSchema(s.Element(), ref+"/items", path, active)
		if err != nil {
			return nil, err
		}
		result["type"], result["items"] = "array", value
		if s.Length() >= 0 {
			result["minItems"], result["maxItems"] = s.Length(), s.Length()
		}
	case reflect.String:
		result["type"] = "string"
	case reflect.Bool:
		result["type"] = "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		result["type"] = "integer"
	case reflect.Float32, reflect.Float64:
		result["type"] = "number"
	default:
		return nil, fmt.Errorf("%w: native wire kind %s", ErrSchemaUnavailable, s.Kind())
	}
	if s.Format() != "" {
		result["format"] = s.Format()
	}
	if s.Nullable() {
		result = nullableJSONSchema(result)
	}
	return result, nil
}
