package tool

import (
	docs "github.com/viant/datly/documentation"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema/jsonschema"
	"reflect"
	"strings"
)

type schemaProjector struct {
	docs    *docs.Snapshot
	schemas map[string]*spec.WireSchema
}

func schemaForType(t reflect.Type, _ map[reflect.Type]bool) (map[string]interface{}, error) {
	return (&schemaProjector{}).argument(t, "", "", nil)
}
func (p *schemaProjector) argument(t reflect.Type, path, property string, schema *spec.WireSchema) (map[string]interface{}, error) {
	if schema != nil {
		return wireSchemaMap(schema), nil
	}
	result, err := (jsonschema.Reflector{Annotate: func(path string, field reflect.StructField) (string, any) {
		annotation := p.docs.StructField(path, field)
		var example any
		if annotation.Example != "" {
			example = annotation.Example
		}
		return annotation.Description, example
	}}).Compile(jsonschema.Request{Type: t, Path: path, Property: property})
	if err != nil {
		return nil, err
	}
	for schemaPath, schema := range p.schemas {
		applyWireSchema(result, schemaPath, schema)
	}
	return result, nil
}

func wireSchemaMap(schema *spec.WireSchema) map[string]interface{} {
	if schema == nil {
		return nil
	}
	result := map[string]interface{}{}
	if schema.Nullable {
		result["type"] = []string{schema.Type, "null"}
	} else {
		result["type"] = schema.Type
	}
	if schema.Format != "" {
		result["format"] = schema.Format
	}
	return result
}

func applyWireSchema(root map[string]interface{}, path string, schema *spec.WireSchema) {
	if root == nil || schema == nil || strings.TrimSpace(path) == "" {
		return
	}
	parts := strings.Split(path, ".")
	if len(parts) > 0 {
		// The root schema already represents the argument at parts[0].
		parts = parts[1:]
	}
	if len(parts) == 0 {
		for key := range root {
			delete(root, key)
		}
		for key, value := range wireSchemaMap(schema) {
			root[key] = value
		}
		return
	}
	applyWireSchemaParts(root, parts, schema)
}

func applyWireSchemaParts(node map[string]interface{}, parts []string, schema *spec.WireSchema) bool {
	if len(parts) == 0 {
		return false
	}
	node = schemaObjectNode(node)
	properties, _ := node["properties"].(map[string]interface{})
	if properties == nil {
		return false
	}
	propertyName := schemaPropertyName(properties, parts[0])
	child, _ := properties[propertyName].(map[string]interface{})
	if child == nil {
		return false
	}
	if len(parts) == 1 {
		mergeWireSchema(child, schema)
		return true
	}
	return applyWireSchemaParts(child, parts[1:], schema)
}

func mergeWireSchema(target map[string]interface{}, schema *spec.WireSchema) {
	for _, key := range []string{"$ref", "anyOf", "oneOf", "allOf", "items", "properties", "additionalProperties", "format", "type"} {
		delete(target, key)
	}
	for key, value := range wireSchemaMap(schema) {
		target[key] = value
	}
}

func schemaPropertyName(properties map[string]interface{}, name string) string {
	if _, ok := properties[name]; ok {
		return name
	}
	if name != "" {
		candidate := strings.ToLower(name[:1]) + name[1:]
		if _, ok := properties[candidate]; ok {
			return candidate
		}
	}
	return name
}

func schemaObjectNode(node map[string]interface{}) map[string]interface{} {
	if node == nil {
		return nil
	}
	if node["properties"] != nil {
		return node
	}
	if items, ok := node["items"].(map[string]interface{}); ok {
		return schemaObjectNode(items)
	}
	for _, key := range []string{"anyOf", "oneOf", "allOf"} {
		list, _ := node[key].([]interface{})
		for _, item := range list {
			child, _ := item.(map[string]interface{})
			if object := schemaObjectNode(child); object != nil && object["properties"] != nil {
				return object
			}
		}
	}
	return node
}
