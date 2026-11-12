package gateway

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/repository"
	dpath "github.com/viant/datly/repository/path"
	dynamic "github.com/viant/datly/service/contract"
	"github.com/viant/datly/view/state"
	"github.com/viant/mcp-protocol/schema/fieldname"
	"github.com/viant/mcp-protocol/schema/jsonschema"
	"github.com/viant/tagly/choice"
	"github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
)

func mcpSchemaReflector(output bool) jsonschema.Reflector {
	return jsonschema.Reflector{ExcludeInternal: true, Output: output, Annotate: func(_ string, field reflect.StructField) (string, any) {
		raw := field.Tag.Get("example")
		if raw == "" {
			return field.Tag.Get("description"), nil
		}
		var value any = raw
		typ := field.Type
		for typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		if typ.Kind() != reflect.String && json.Valid([]byte(raw)) {
			decoder := json.NewDecoder(strings.NewReader(raw))
			decoder.UseNumber()
			_ = decoder.Decode(&value)
		}
		return field.Tag.Get("description"), value
	}}
}

func mcpParameterField(c *repository.Component, p *state.Parameter, input *shape.Representation, forceRequired bool) (reflect.StructField, *shape.Representation, bool, error) {
	canonical := canonicalMCPParameter(c, p)
	selectedParameter, _ := repository.EffectiveParameter(canonical, input)
	effectiveParameter := *p
	effectiveParameter.Required = selectedParameter.Required
	field := reflect.StructField{Name: strings.Title(p.Name), Type: mcpToolParameterType(p), Tag: buildMCPFieldTagWithRequired(&effectiveParameter, true, forceRequired)}
	var metadata *shape.Representation
	if input != nil && c.Input.Type.Schema != nil {
		typ := indirectType(c.Input.Type.Schema.Type())
		original, ok := typ.FieldByName(canonical.Name)
		selected, found := dynamic.LookupField(input, canonical.Name)
		if ok && found {
			var err error
			field.Tag, err = dynamic.ProjectTags(field.Tag, original.Tag, selected.Tag)
			if err != nil {
				return field, nil, false, err
			}
			if selected.Name != original.Name {
				field.Name = selected.Name
			}
			metadata, _ = input.Lookup(canonical.Name)
		}
	}
	if forceRequired {
		field.Tag = (&xcontract.Field{Name: field.Name, Tag: field.Tag}).Required().Tag
	}
	name, err := fieldname.Resolve(field, field.Name)
	hidden := name.Hidden || field.Tag.Get("internal") == "true" || strings.Split(field.Tag.Get("json"), ",")[0] == "-"
	return field, metadata, hidden, err
}

// Build object properties directly from linked parameter/body types. In
// particular, never synthesize an anonymous struct containing promoted methods.
func (r *Router) mcpInputSchema(c *repository.Component, path *dpath.Path, effective *repository.EffectiveContract) (map[string]any, error) {
	properties := map[string]any{}
	var required []string
	defs := map[string]any{}
	reflector := mcpSchemaReflector(false)
	seenQuery, seenPath := map[string]bool{}, map[string]bool{}
	add := func(name string, doc map[string]any, isRequired bool) {
		if _, exists := properties[name]; exists {
			return
		}
		properties[name] = doc
		if isRequired {
			required = append(required, name)
		}
	}
	for _, p := range r.collectToolParameters(c) {
		force := false
		switch p.In.Kind {
		case state.KindPath:
			if path != nil && strings.Contains(path.URI, "{"+p.In.Name+"}") {
				force = true
			}
			if path != nil && p.URI != "" {
				if !mcpParameterURIPathMatches(p.URI, path.URI) {
					continue
				}
				force = true
			}
			if seenPath[p.In.Name] {
				continue
			}
			seenPath[p.In.Name] = true
		case state.KindQuery, state.KindForm:
			if seenQuery[p.In.Name] {
				continue
			}
			seenQuery[p.In.Name] = true
		case state.KindRequestBody:
		default:
			continue
		}
		field, metadata, hidden, err := mcpParameterField(c, p, effective.Input, force)
		if err != nil {
			return nil, err
		}
		if hidden {
			continue
		}
		if isMCPAnonymousBody(p) && indirectType(p.Schema.Type()).Kind() == reflect.Struct {
			doc, err := reflector.Compile(jsonschema.Request{Type: p.Schema.Type(), Representation: metadata})
			if err != nil {
				return nil, err
			}
			added := map[string]bool{}
			if fields, ok := doc["properties"].(map[string]any); ok {
				for name, value := range fields {
					if _, exists := properties[name]; !exists {
						added[name] = true
					}
					add(name, value.(map[string]any), false)
				}
			}
			if names, ok := doc["required"].([]string); ok {
				for _, name := range names {
					if added[name] {
						required = append(required, name)
					}
				}
			}
			if definitions, ok := doc["$defs"].(map[string]any); ok {
				for name, value := range definitions {
					if prior, exists := defs[name]; exists && !reflect.DeepEqual(prior, value) {
						return nil, fmt.Errorf("conflicting schema definition %q", name)
					}
					defs[name] = value
				}
			}
			continue
		}
		jsonName := strings.Split(field.Tag.Get("json"), ",")[0]
		if jsonName == "" {
			jsonName = field.Name
		}
		name, err := fieldname.Resolve(field, jsonName)
		if err != nil {
			return nil, err
		}
		selected := shape.JSONField{Name: jsonName, Field: shape.Field{Name: field.Name, ReflectedType: field.Type, Tag: field.Tag, Exported: true}}
		doc, err := reflector.Compile(jsonschema.Request{Type: field.Type, Field: &selected, Representation: metadata, Property: name.Public})
		if err != nil {
			return nil, err
		}
		add(name.Public, doc, fieldname.Required(field))
	}
	if componentSupportsMCPRawBlob(c) {
		doc, err := reflector.Compile(jsonschema.Request{Type: reflect.TypeFor[mcpBlob](), Property: "Blob"})
		if err != nil {
			return nil, err
		}
		add("Blob", doc, false)
	}
	for _, p := range c.Input.Type.Parameters {
		if isMCPMultipartFileParameter(p) {
			doc, err := reflector.Compile(jsonschema.Request{Type: reflect.TypeFor[mcpMultipartPayload](), Property: "Payload"})
			if err != nil {
				return nil, err
			}
			add("Payload", doc, false)
			break
		}
	}
	result := map[string]any{"type": "object", "properties": properties}
	if len(required) > 0 {
		result["required"] = required
	}
	if len(defs) > 0 {
		result["$defs"] = defs
	}
	return result, nil
}

// Check the declared JSON enum form before URL encoding erases its JSON type.
// Keep the inbound value: the native HTTP binder owns storage conversion.
func validateMCPEnum(field reflect.StructField, value any) error {
	enumeration, err := choice.ParseEnum(field.Tag, field.Type)
	if err != nil || enumeration == nil {
		return err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = enumeration.DecodeJSON(raw)
	return err
}

func canonicalMCPParameter(c *repository.Component, public *state.Parameter) *state.Parameter {
	if public.In != nil {
		for _, candidate := range c.Input.Type.Parameters {
			if candidate.In != nil && candidate.In.Kind == public.In.Kind && candidate.In.Name == public.In.Name {
				return candidate
			}
		}
	}
	return public
}
