package openapi

import (
	"fmt"
	documentation "github.com/viant/datly/documentation"
	"reflect"
	"strings"

	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
	"golang.org/x/net/http/httpguts"
)

type inputBuilder struct {
	schemas  *schemaBuilder
	template *route.PathTemplate
	endpoint *spec.Route
}

func (b *inputBuilder) build(fields []registry.InputField, operation *openapi3.Operation) error {
	seen := map[string]bool{}
	paths := map[string]bool{}
	for _, name := range b.template.Parameters() {
		paths[name] = false
	}
	body := &bodyBuilder{}
	for _, field := range fields {
		binding := field.Binding()
		kind := binding.Location.Kind
		switch kind {
		case "path", "query", "header", "cookie", "body", "form":
		default:
			continue
		}
		if binding.When != "" || binding.Scope != "" || binding.With != "" {
			return fmt.Errorf("input %s has conditional binding without representable transport authority", field.Path())
		}
		name := binding.Location.In
		if name == "" && kind != "body" {
			name = binding.Name
		}
		if name == "" && kind != "body" {
			return fmt.Errorf("input %s has no transport name", field.Path())
		}
		if (kind == "header" || kind == "cookie") && !httpguts.ValidHeaderFieldName(name) {
			return fmt.Errorf("invalid HTTP %s name %q", kind, name)
		}
		key := kind + ":" + name
		if kind == "header" {
			key = strings.ToLower(key)
		}
		if seen[key] {
			return fmt.Errorf("duplicate transport input %s", key)
		}
		seen[key] = true
		required := kind == "path" || binding.Required != nil && *binding.Required
		sourceType := field.SourceType()
		// Required transport values cannot be null in the Bindly plan.
		if required || kind != "body" {
			sourceType = (xshape.Runtime{}).Indirect(sourceType)
		}
		originalDocs := b.schemas.docs
		if owned := field.Documentation(); owned != nil {
			b.schemas.docs = owned
		}
		b.schemas.path = field.Path()
		schema, err := b.schemas.input(sourceType, kind)
		b.schemas.path = ""
		if err != nil {
			return fmt.Errorf("input %s: %w", field.Path(), err)
		}
		if (required || kind != "body") && schema.Nullable {
			copy := *schema
			copy.Nullable = false
			schema = &copy
		}
		description := ""
		example := ""
		if param, ok := binding.Extension.(*spec.Parameter); ok && param != nil {
			description = param.Description
			example = param.Example
		}
		annotation := b.schemas.docs.Parameter(binding.Name, documentation.Annotation{Description: description, Example: example})
		description, example = annotation.Description, annotation.Example
		b.schemas.docs = originalDocs
		if schema.Ref != "" && (description != "" || example != "") {
			schema = &openapi3.Schema{AllOf: openapi3.SchemaList{schema}}
		}
		if description != "" {
			schema.Description = description
		}
		if example != "" {
			schema.Example = example
		}
		if kind == "body" || kind == "form" {
			if err := body.add(field, schema); err != nil {
				return err
			}
			continue
		}

		if kind == "path" {
			if _, ok := paths[name]; !ok {
				return fmt.Errorf("path input %q is absent from route", name)
			}
			paths[name] = true
			required = true
		}
		base := (xshape.Runtime{}).Indirect(field.SourceType())
		if base == nil {
			return fmt.Errorf("input %s has no source type", field.Path())
		}
		if kind == "header" && (strings.EqualFold(name, "Authorization") || strings.EqualFold(name, "Accept") || strings.EqualFold(name, "Content-Type")) {
			if strings.EqualFold(name, "Accept") {
				if param, ok := binding.Extension.(*spec.Parameter); ok && param != nil && param.FormatSelector {
					continue // response media entries describe this selector
				}
			}
			// OpenAPI ignores these header Parameter Objects. Require an enforced
			// API-key policy or compiled verifier evidence before emitting security.
			if strings.EqualFold(name, b.endpoint.APIKeyHeader) || strings.EqualFold(name, "Authorization") && field.VerifiesJWT() {
				continue
			}
			return fmt.Errorf("header %q cannot be represented as an OpenAPI parameter without runtime policy", name)
		}
		if kind == "header" && strings.EqualFold(name, b.endpoint.APIKeyHeader) {
			continue
		}
		parameter := &openapi3.Parameter{Name: name, In: kind, Required: required, Description: description, Schema: schema, Example: schema.Example}
		switch base.Kind() {
		case reflect.Struct, reflect.Map, reflect.Interface:
			if schema.Type == "string" {
				break
			}
			return fmt.Errorf("%s input %q requires unsupported object serialization", kind, name)
		case reflect.Slice, reflect.Array:
			if kind != "query" {
				return fmt.Errorf("%s array %q has no matching OpenAPI serialization", kind, name)
			}
			explode := true
			parameter.Style = "form"
			parameter.Explode = &explode
		}
		operation.Parameters = append(operation.Parameters, parameter)
	}
	for name, present := range paths {
		if !present {
			return fmt.Errorf("route placeholder %q has no effective input", name)
		}
	}
	var err error
	operation.RequestBody, err = body.build(strings.ToUpper(strings.TrimSpace(b.endpoint.Method)))
	return err
}
