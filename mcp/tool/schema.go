package tool

import (
	docs "github.com/viant/datly/documentation"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema/fieldname"
	"github.com/viant/mcp-protocol/schema/jsonschema"
	"reflect"
	"sort"
	"strings"
)

type schemaProjector struct {
	docs       *docs.Snapshot
	schemas    map[string]*spec.WireSchema
	wireType   reflect.Type
	caseFormat string
}

func schemaForType(t reflect.Type, _ map[reflect.Type]bool) (map[string]interface{}, error) {
	return (&schemaProjector{}).argument(t, "", "", nil)
}
func (p *schemaProjector) argument(t reflect.Type, path, property string, schema *spec.WireSchema) (map[string]interface{}, error) {
	if schema != nil {
		return wireSchemaMap(schema), nil
	}
	var authority map[reflect.Type]reflect.Type
	if p.wireType != nil {
		source := t
		for source.Kind() == reflect.Pointer {
			source = source.Elem()
		}
		authority = map[reflect.Type]reflect.Type{source: p.wireType}
	}
	visibility := memberNameVisibility(p.caseFormat)
	if p.wireType != nil {
		visibility.FallbackName = nil
	}
	result, err := (jsonschema.Reflector{FallbackName: visibility.FallbackName, WireTypes: authority, ExcludeInternal: visibility.ExcludeInternal, ExcludeField: visibility.ExcludeField, Annotate: func(path string, field reflect.StructField) (string, any) {
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
		applyWireSchema(result, canonicalSchemaPath(t, schemaPath, visibility), schema)
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
		if schema.Type == "" {
			return
		}
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
		if schema.Type != "" {
			mergeWireSchema(child, schema)
		}
		if schema.Required {
			addRequiredProperty(node, propertyName)
			if !schema.Nullable {
				removeSchemaNull(child)
			}
		}
		if schema.MinItems > 0 {
			child["minItems"] = schema.MinItems
		}
		return true
	}
	return applyWireSchemaParts(child, parts[1:], schema)
}

func addRequiredProperty(parent map[string]interface{}, name string) {
	set := map[string]bool{name: true}
	switch existing := parent["required"].(type) {
	case []string:
		for _, item := range existing {
			set[item] = true
		}
	case []interface{}:
		for _, item := range existing {
			if value, ok := item.(string); ok {
				set[value] = true
			}
		}
	}
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	sort.Strings(values)
	parent["required"] = values
}

func removeSchemaNull(node map[string]interface{}) {
	var kinds []string
	switch value := node["type"].(type) {
	case []string:
		kinds = value
	case []interface{}:
		for _, item := range value {
			if kind, ok := item.(string); ok {
				kinds = append(kinds, kind)
			}
		}
	}
	if len(kinds) > 0 {
		allowed := make([]string, 0, len(kinds))
		for _, kind := range kinds {
			if kind != "null" {
				allowed = append(allowed, kind)
			}
		}
		if len(allowed) == 1 {
			node["type"] = allowed[0]
		} else {
			node["type"] = allowed
		}
	}
	for _, key := range []string{"anyOf", "oneOf"} {
		alternatives, ok := node[key].([]interface{})
		if !ok {
			continue
		}
		allowed := make([]interface{}, 0, len(alternatives))
		for _, alternative := range alternatives {
			child, ok := alternative.(map[string]interface{})
			if ok && child["type"] == "null" {
				continue
			}
			allowed = append(allowed, alternative)
		}
		node[key] = allowed
	}
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

// One policy is shared by native schema projection and MCP JSON-key normalization.
func memberNameVisibility(formats ...string) fieldname.Visibility {
	result := fieldname.Visibility{ExcludeInternal: true, ExcludeField: excludedMCPMember}
	if len(formats) != 0 && strings.TrimSpace(formats[0]) != "" {
		result.FallbackName = func(field reflect.StructField, fallback string) string {
			return caseFormattedFallback(field, fallback, formats[0])
		}
	}
	return result
}

// Wire schema paths identify original members, independent of public spelling.
func canonicalSchemaPath(t reflect.Type, path string, visibility fieldname.Visibility) string {
	parts := strings.Split(path, ".")
	for i := 1; i < len(parts); i++ {
		for t != nil && (t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Array) {
			if fieldname.CustomJSON(t) {
				visibility.FallbackName = nil
			}
			t = t.Elem()
		}
		if t == nil || t.Kind() != reflect.Struct {
			break
		}
		if fieldname.CustomJSON(t) {
			visibility.FallbackName = nil
		}
		entries, err := fieldname.Fields(t, visibility)
		if err != nil {
			break
		}
		found := false
		for _, entry := range entries {
			if parts[i] == entry.Field.Name || parts[i] == entry.JSONName || parts[i] == entry.Name.Public {
				parts[i], t, found = entry.Name.Public, entry.Field.Type, true
				break
			}
		}
		if !found {
			break
		}
	}
	return strings.Join(parts, ".")
}
func excludedMCPMember(field reflect.StructField) bool {
	return strings.TrimSpace(field.Tag.Get("mcp")) == "-"
}
