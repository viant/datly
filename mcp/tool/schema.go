package tool

import (
	"fmt"
	docs "github.com/viant/datly/documentation"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema/fieldname"
	"github.com/viant/mcp-protocol/schema/jsonschema"
	"github.com/viant/tagly/choice"
	"github.com/viant/tagly/format"
	xshape "github.com/viant/x/shape"
	"reflect"
	"sort"
	"strings"
)

type schemaProjector struct {
	nativeBody     bool
	rootInputTag   reflect.StructTag
	field          *xshape.JSONField
	representation *xshape.Representation
	docs           *docs.Snapshot
	schemas        map[string]*spec.WireSchema
	wireType       reflect.Type
	caseFormat     string
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
	visibility := representationVisibility(p.representation, p.caseFormat)
	if p.wireType != nil {
		visibility.FallbackName = nil
	}
	jsonxAlternatives := map[string]*spec.WireSchema{}
	scalarAlternatives := map[string]map[string]interface{}{}
	runtimeSelectionPaths := map[string]bool{}
	var timePolicyErr error
	nativeAliases := p.nativeBody && p.wireType == nil
	if nativeAliases && p.rootInputTag != "" {
		policy, err := registry.JSONXInputSchema(reflect.StructField{Type: t, Tag: p.rootInputTag})
		if err != nil {
			return nil, err
		}
		if policy != nil {
			for _, branch := range policy.AnyOf {
				if branch.Type == "object" {
					jsonxAlternatives[path] = policy
				}
			}
		}
	}
	var requireField func(string, reflect.StructField) bool
	if nativeAliases {
		requiredPaths := map[string]bool{}
		for schemaPath, field := range p.schemas {
			if field != nil && field.Required {
				requiredPaths[canonicalSchemaPath(t, schemaPath, visibility)] = true
			}
		}
		requireField = func(path string, _ reflect.StructField) bool {
			return requiredPaths[canonicalSchemaPath(t, path, visibility)]
		}
	}
	argumentPath := path
	result, err := (jsonschema.Reflector{InputAliases: nativeAliases, RequireField: requireField, FallbackName: visibility.FallbackName, WireTypes: authority, ExcludeInternal: visibility.ExcludeInternal, ExcludeField: visibility.ExcludeField, Annotate: func(path string, field reflect.StructField) (string, any) {
		if collection, err := choice.ParseCollectionEnum(field.Tag, field.Type); err != nil {
			timePolicyErr = err
		} else if collection != nil && (p.field == nil || path != argumentPath) {
			timePolicyErr = fmt.Errorf("MCP collection enum %s requires a direct named or flattened body argument", path)
		}
		if nativeAliases {
			policy, err := format.Parse(field.Tag)
			if err != nil {
				timePolicyErr = err
			} else if policy.InputNamesPriority == "alias" || policy.InputGroup != "" || policy.InputUnless != "" {
				runtimeSelectionPaths[canonicalSchemaPath(t, path, visibility)] = true
			}
		}
		if nativeAliases && !runtimeSelectionPaths[canonicalSchemaPath(t, path, visibility)] {
			if policy, err := registry.TimeInputSchema(field); err != nil {
				timePolicyErr = err
			} else if policy != nil {
				scalarAlternatives[path] = wireSchemaMap(policy)
			}
			if enumeration, enumErr := choice.ParseEnum(field.Tag, field.Type); enumErr == nil && enumeration != nil {
				if domain := enumeration.InputDomain(); domain != nil {
					scalarAlternatives[path] = wireSchemaMap(registry.EnumInputSchema(enumeration, field.Type.Kind() == reflect.Pointer))
				}
			}
			if policy, err := registry.JSONXInputSchema(field); err != nil {
				timePolicyErr = err
			} else if policy != nil {
				jsonxAlternatives[path] = policy
			}
		}
		annotation := p.docs.StructField(path, field)
		var example any
		if annotation.Example != "" {
			example = annotation.Example
		}
		return annotation.Description, example
	}}).Compile(jsonschema.Request{Field: p.field, Type: t, Path: path, Property: property, Representation: p.representation})
	if err != nil {
		return nil, err
	}
	if timePolicyErr != nil {
		return nil, timePolicyErr
	}
	for fieldPath, alternatives := range scalarAlternatives {
		parts := strings.Split(canonicalSchemaPath(t, fieldPath, visibility), ".")
		_, child, _ := schemaMember(result, parts[1:])
		if child == nil {
			continue
		}
		for _, key := range []string{"type", "enum", "format", "anyOf", "oneOf", "allOf", "pattern", "minLength", "maxLength"} {
			delete(child, key)
		}
		for key, value := range alternatives {
			child[key] = value
		}
	}
	for schemaPath, schema := range p.schemas {
		canonicalPath := canonicalSchemaPath(t, schemaPath, visibility)
		if !runtimeSelectionPaths[canonicalPath] {
			applyWireSchema(result, canonicalPath, schema, nativeAliases)
		}
	}
	for fieldPath, policy := range jsonxAlternatives {
		parts := strings.Split(canonicalSchemaPath(t, fieldPath, visibility), ".")
		_, child, _ := schemaMember(result, parts[1:])
		if fieldPath == path {
			child = result
		}
		if child == nil {
			continue
		}
		items := child["items"]
		minItems := child["minItems"]
		alternatives := wireSchemaMap(policy)
		if len(policy.AnyOf) == 0 {
			ordinary := map[string]any{}
			for key, value := range child {
				ordinary[key] = value
			}
			alternatives = map[string]any{"anyOf": []any{ordinary, map[string]any{"type": "null"}}}
		}
		if branches, ok := alternatives["anyOf"].([]any); ok {
			for _, value := range branches {
				branch := value.(map[string]any)
				if branch["type"] == "object" {
					if record, ok := items.(map[string]any); ok {
						delete(branch, "type")
						for key, value := range record {
							branch[key] = value
						}
					}
				}
				if branch["type"] == "array" {
					if items != nil {
						branch["items"] = map[string]any{"anyOf": []any{items, map[string]any{"type": "null"}}}
					}
					if minItems != nil {
						branch["minItems"] = minItems
					}
				}
			}
		}
		for _, key := range []string{"type", "enum", "format", "anyOf", "oneOf", "allOf", "pattern", "minLength", "maxLength", "minimum", "maximum", "items", "minItems", "maxItems"} {
			delete(child, key)
		}
		for key, value := range alternatives {
			child[key] = value
		}
		for ownerPath, owner := range p.schemas {
			if owner != nil && owner.Required && !owner.Nullable && canonicalSchemaPath(t, ownerPath, visibility) == canonicalSchemaPath(t, fieldPath, visibility) {
				removeSchemaNull(child)
			}
		}
	}
	return result, nil
}

func wireSchemaMap(schema *spec.WireSchema) map[string]interface{} {
	if schema == nil {
		return nil
	}
	result := map[string]interface{}{}
	if len(schema.AnyOf) > 0 {
		alternatives := make([]any, 0, len(schema.AnyOf)+1)
		for _, child := range schema.AnyOf {
			alternatives = append(alternatives, wireSchemaMap(child))
		}
		if schema.Nullable {
			alternatives = append(alternatives, map[string]any{"type": "null"})
		}
		result["anyOf"] = alternatives
		return result
	}
	if schema.Minimum != nil {
		result["minimum"] = *schema.Minimum
	}
	if schema.Maximum != nil {
		result["maximum"] = *schema.Maximum
	}
	if schema.Pattern != "" {
		result["pattern"] = schema.Pattern
	}
	if schema.MinLength != nil {
		result["minLength"] = *schema.MinLength
	}
	if schema.MaxLength != nil {
		result["maxLength"] = *schema.MaxLength
	}
	if len(schema.Enum) > 0 {
		values := append([]any(nil), schema.Enum...)
		if schema.Nullable {
			values = append(values, nil)
		}
		result["enum"] = values
	}
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

func applyWireSchema(root map[string]interface{}, path string, schema *spec.WireSchema, requiredProjected ...bool) {
	if root == nil || schema == nil || strings.TrimSpace(path) == "" {
		return
	}
	parts := strings.Split(path, ".")
	if len(parts) > 0 {
		// The root schema already represents the argument at parts[0].
		parts = parts[1:]
	}
	if len(parts) == 0 {
		if schema.Type == "" && len(schema.AnyOf) == 0 {
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
	applyWireSchemaParts(root, parts, schema, requiredProjected...)
}

func applyWireSchemaParts(node map[string]interface{}, parts []string, schema *spec.WireSchema, requiredProjected ...bool) bool {
	node, child, propertyName := schemaMember(node, parts)
	if child == nil {
		return false
	}
	if schema.Type != "" || len(schema.AnyOf) > 0 {
		mergeWireSchema(child, schema)
	}
	if schema.Required {
		if len(requiredProjected) == 0 || !requiredProjected[0] {
			addRequiredProperty(node, propertyName)
		}
		if !schema.Nullable {
			removeSchemaNull(child)
		}
	}
	if schema.MinItems > 0 {
		child["minItems"] = schema.MinItems
	}
	return true
}

func schemaMember(node map[string]interface{}, parts []string) (map[string]interface{}, map[string]interface{}, string) {
	if len(parts) == 0 {
		return nil, nil, ""
	}
	node = schemaObjectNode(node)
	properties, _ := node["properties"].(map[string]interface{})
	if properties == nil {
		return nil, nil, ""
	}
	propertyName := schemaPropertyName(properties, parts[0])
	child, _ := properties[propertyName].(map[string]interface{})
	if child == nil {
		return nil, nil, ""
	}
	if len(parts) == 1 {
		return node, child, propertyName
	}
	return schemaMember(child, parts[1:])
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
	for _, key := range []string{"$ref", "anyOf", "oneOf", "allOf", "items", "properties", "additionalProperties", "format", "type", "enum", "pattern", "minLength", "maxLength"} {
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
	if values, ok := node["additionalProperties"].(map[string]interface{}); ok {
		return schemaObjectNode(values)
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
		for t != nil && (t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Array || t.Kind() == reflect.Map) {
			if fieldname.CustomJSON(t) {
				visibility.FallbackName = nil
			}
			if visibility.Representation != nil {
				visibility.Representation = visibility.Representation.Elem()
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
				visibility.Representation = entry.Representation
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

func representationVisibility(metadata *xshape.Representation, formats ...string) fieldname.Visibility {
	visibility := memberNameVisibility(formats...)
	visibility.Representation = metadata
	return visibility
}
