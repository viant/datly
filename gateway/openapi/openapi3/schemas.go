// Adapted from viant/datly gateway/router/openapi/openapi3 (Apache-2.0).
// See LICENSE and NOTICE. Mutable document-loading methods are intentionally omitted.
package openapi3

type (
	Schemas map[string]*Schema

	SchemaList []*Schema

	Schema struct {
		Ref   string     `json:"$ref,omitempty" yaml:"$ref,omitempty"`
		Type  string     `json:"type,omitempty" yaml:"type,omitempty"`
		AllOf SchemaList `json:"allOf,omitempty" yaml:"allOf,omitempty"`
		OneOf SchemaList `json:"oneOf,omitempty" yaml:"oneOf,omitempty"`
		AnyOf SchemaList `json:"anyOf,omitempty" yaml:"anyOf,omitempty"`

		Not   *Schema `json:"not,omitempty" yaml:"not,omitempty"`
		Items *Schema `json:"items,omitempty" yaml:"items,omitempty"`

		Properties Schemas `json:"properties,omitempty" yaml:"properties,omitempty"`

		// AdditionalPropertiesAllowed represents the boolean alternative of this OpenAPI keyword.
		AdditionalPropertiesAllowed *bool       `json:"-" yaml:"-"`
		AdditionalProperties        *Schema     `json:"additionalProperties,omitempty" yaml:"additionalProperties,omitempty"`
		Description                 string      `json:"description,omitempty" yaml:"description,omitempty"`
		Format                      string      `json:"format,omitempty" yaml:"format,omitempty"`
		Default                     interface{} `json:"default,omitempty" yaml:"default,omitempty"`

		// Properties
		Nullable      bool           `json:"nullable,omitempty" yaml:"nullable,omitempty"`
		Discriminator *Discriminator `json:"discriminator,omitempty" yaml:"discriminator,omitempty"`

		ReadOnly        bool                   `json:"readOnly,omitempty" yaml:"readOnly,omitempty"`
		WriteOnly       bool                   `json:"writeOnly,omitempty" yaml:"writeOnly,omitempty"`
		AllowEmptyValue bool                   `json:"allowEmptyValue,omitempty" yaml:"allowEmptyValue,omitempty"`
		XML             *XML                   `json:"xml,omitempty" yaml:"xml,omitempty"`
		ExternalDocs    *ExternalDocumentation `json:"externalDocs,omitempty" yaml:"externalDocs,omitempty"`
		Deprecated      bool                   `json:"deprecated,omitempty" yaml:"deprecated,omitempty"`
		Example         interface{}            `json:"example,omitempty" yaml:"example,omitempty"`

		//The following are JSON validation schema

		Title      string   `json:"title,omitempty" yaml:"title,omitempty"`
		MultipleOf *float64 `json:"multipleOf,omitempty" yaml:"multipleOf,omitempty"`

		Max          *float64 `json:"maximum,omitempty" yaml:"maximum,omitempty"`
		ExclusiveMax bool     `json:"exclusiveMaximum,omitempty" yaml:"exclusiveMaximum,omitempty"`
		Min          *float64 `json:"minimum,omitempty" yaml:"minimum,omitempty"`
		ExclusiveMin bool     `json:"exclusiveMinimum,omitempty" yaml:"exclusiveMinimum,omitempty"`

		MaxLength *uint64 `json:"maxLength,omitempty" yaml:"maxLength,omitempty"`
		MinLength uint64  `json:"minLength,omitempty" yaml:"minLength,omitempty"`
		Pattern   string  `json:"pattern,omitempty" yaml:"pattern,omitempty"`

		MaxItems    *uint64 `json:"maxItems,omitempty" yaml:"maxItems,omitempty"`
		MinItems    uint64  `json:"minItems,omitempty" yaml:"minItems,omitempty"`
		UniqueItems bool    `json:"uniqueItems,omitempty" yaml:"uniqueItems,omitempty"`

		MaxProps *uint64       `json:"maxProperties,omitempty" yaml:"maxProperties,omitempty"`
		MinProps uint64        `json:"minProperties,omitempty" yaml:"minProperties,omitempty"`
		Required []string      `json:"required,omitempty" yaml:"required,omitempty"`
		Enum     []interface{} `json:"enum,omitempty" yaml:"enum,omitempty"`
	}

	Discriminator struct {
		PropertyName string            `json:"propertyName" yaml:"propertyName"`
		Mapping      map[string]string `json:"mapping,omitempty" yaml:"mapping,omitempty"`
	}

	XML struct {
		Name      string `json:"name,omitempty"`
		Namespace string `json:"namespace,omitempty"`
		Prefix    string `json:"prefix,omitempty"`
		Attribute bool   `json:"attribute,omitempty"`
		Wrapped   bool   `json:"wrapped,omitempty"`
	}
)
