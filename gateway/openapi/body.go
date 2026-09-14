package openapi

import (
	"fmt"

	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/runtime/registry"
)

// bodyBuilder assembles the single OpenAPI request body from effective bindings.
// Named body inputs are literal top-level JSON keys, as in Bindly's body source.
type bodyBuilder struct {
	properties openapi3.Schemas
	required   []string
	whole      *openapi3.Schema
	media      string
	count      int
	mandatory  bool
}

func (b *bodyBuilder) add(field registry.InputField, schema *openapi3.Schema) error {
	binding := field.Binding()
	media := "application/json"
	name := binding.Location.In
	if binding.Location.Kind == "form" {
		media = "application/x-www-form-urlencoded"
		if name == "" {
			name = binding.Name
		}
		if err := b.formSchema(schema); err != nil {
			return fmt.Errorf("form input %s: %w", name, err)
		}
	}
	if b.count > 0 && media != b.media {
		return fmt.Errorf("mixed body and form bindings are not representable")
	}
	b.count++
	b.media = media
	required := binding.Required != nil && *binding.Required
	b.mandatory = b.mandatory || required
	if name == "" {
		if b.whole != nil {
			return fmt.Errorf("multiple whole-body contracts")
		}
		b.whole = schema
		return nil
	}
	if b.properties == nil {
		b.properties = openapi3.Schemas{}
	}
	if _, exists := b.properties[name]; exists {
		return fmt.Errorf("duplicate body property %q", name)
	}
	b.properties[name] = schema
	if required {
		b.required = append(b.required, name)
	}
	return nil
}

func (b *bodyBuilder) build(method string) (*openapi3.RequestBody, error) {
	if b.count == 0 {
		return nil, nil
	}
	// OpenAPI 3.0 consumers can ignore bodies on methods without defined body
	// semantics. Fail rather than document a request clients cannot reproduce.
	switch method {
	case "POST", "PUT", "PATCH":
	default:
		return nil, fmt.Errorf("request body on %s is not representable by this OpenAPI 3.0 generator", method)
	}
	schema := &openapi3.Schema{Type: "object", Properties: b.properties, Required: b.required}
	if b.whole != nil {
		if len(b.properties) > 0 {
			return nil, fmt.Errorf("whole-body and selected-body bindings overlap")
		}
		schema = b.whole
	}
	return &openapi3.RequestBody{Required: b.mandatory, Content: openapi3.Content{b.media: {Schema: schema}}}, nil
}

// Form binding uses repeated text values, not JSON object expansion.
func (b *bodyBuilder) formSchema(schema *openapi3.Schema) error {
	switch schema.Type {
	case "string", "number", "integer", "boolean":
		return nil
	case "array":
		if schema.Items != nil {
			switch schema.Items.Type {
			case "string", "number", "integer", "boolean":
				copy := *schema.Items
				copy.Nullable = false
				schema.Items = &copy
				return nil
			}
		}
	}
	return fmt.Errorf("object serialization is not supported")
}
