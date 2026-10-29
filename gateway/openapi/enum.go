package openapi

import (
	"github.com/viant/datly/gateway/openapi/openapi3"
	"strings"
)

// enumSchema copies only the selected path so one route's domain cannot mutate
// a shared type schema used by another input or response.
func (b *schemaBuilder) enumSchema(schema *openapi3.Schema, path []string, values []any, depth int) (*openapi3.Schema, bool) {
	if schema == nil || depth > 64 {
		return schema, false
	}
	copy := *schema
	if len(path) == 0 {
		copy.Enum = append([]any(nil), values...)
		if copy.Nullable {
			hasNull := false
			for _, value := range copy.Enum {
				if value == nil {
					hasNull = true
					break
				}
			}
			if !hasNull {
				copy.Enum = append(copy.Enum, nil)
			}
		}
		return &copy, true
	}
	if schema.Ref != "" {
		name := strings.TrimPrefix(schema.Ref, "#/components/schemas/")
		if target := b.schemas[name]; target != nil {
			return b.enumSchema(target, path, values, depth+1)
		}
	}
	key := path[0]
	child, found := schema.Properties[key]
	if !found {
		for candidate, property := range schema.Properties {
			if !strings.EqualFold(candidate, path[0]) {
				continue
			}
			if found {
				return schema, false
			}
			key, child, found = candidate, property, true
		}
	}
	if found {
		updated, ok := b.enumSchema(child, path[1:], values, depth+1)
		if !ok {
			return schema, false
		}
		copy.Properties = openapi3.Schemas{}
		for k, v := range schema.Properties {
			copy.Properties[k] = v
		}
		copy.Properties[key] = updated
		return &copy, true
	}

	for i, child := range schema.AllOf {
		updated, ok := b.enumSchema(child, path, values, depth+1)
		if ok {
			copy.AllOf = append(openapi3.SchemaList(nil), schema.AllOf...)
			copy.AllOf[i] = updated
			return &copy, true
		}
	}
	return schema, false
}
