package openapi

import (
	"crypto/sha256"
	"fmt"
	"reflect"

	"github.com/viant/datly/gateway/openapi/openapi3"
	xshape "github.com/viant/x/shape"
)

// object publishes a placeholder before projecting properties so recursive Go
// object graphs become ordinary component references. Native shape owns fields
// and canonical type expressions; this method owns only their OpenAPI encoding.
func (b *schemaBuilder) object(key schemaKey) (*openapi3.Schema, error) {
	t, input := key.typeOf, key.input
	if name := b.recursive[t]; name != "" {
		return &openapi3.Schema{Ref: "#/components/schemas/" + name}, nil
	}
	s := &openapi3.Schema{}
	expression, err := (xshape.Resolver{}).Expression(t)
	if err != nil {
		return nil, err
	}
	identity := t.PkgPath() + ":" + expression
	if previous := b.identities[identity]; previous != nil && previous != t {
		return nil, fmt.Errorf("schema identity collision for %s", t)
	}
	b.identities[identity] = t
	mode := "Output"
	if input {
		mode = "Input"
	}
	name := fmt.Sprintf("%s_%x", mode, sha256.Sum256([]byte(b.scope+":"+key.path+":"+t.PkgPath()+":"+expression)))
	if owner, ok := b.owners[name]; ok && owner != key {
		return nil, fmt.Errorf("schema identity collision for %s", t)
	}
	b.owners[name] = key
	b.names[key] = name
	b.recursive[t] = name
	defer delete(b.recursive, t)
	b.schemas[name] = s
	s.Type = "object"
	s.Properties = openapi3.Schemas{}
	fields, err := xshape.Linked(t).JSONFields()
	if err != nil {
		return nil, err
	}
	for _, projected := range fields {
		field := projected.Field
		if field.Tag.Get("setMarker") == "true" {
			if input {
				continue
			}
			return nil, fmt.Errorf("output presence field %s.%s must be hidden by its JSON contract", t, field.Name)
		}
		name := projected.Name
		parentPath := b.path
		if b.path != "" {
			b.path += "."
		}
		b.path += field.Name
		annotation := b.docs.StructField(b.path, field.StructField())
		property, err := b.schema(field.ReflectedType, input)
		b.path = parentPath
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", t, field.Name, err)
		}
		if projected.Quoted {
			property = &openapi3.Schema{Type: "string", Nullable: field.ReflectedType.Kind() == reflect.Pointer}
		}
		if annotation.Description != "" || annotation.Example != "" {
			if property.Ref != "" {
				property = &openapi3.Schema{AllOf: openapi3.SchemaList{property}}
			}
			property.Description = annotation.Description
			if annotation.Example != "" {
				property.Example = annotation.Example
			}
		}
		s.Properties[name] = property
		if !input && !projected.MayOmit() {
			s.Required = append(s.Required, name)
		}
	}
	return &openapi3.Schema{Ref: "#/components/schemas/" + name}, nil
}
