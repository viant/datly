package openapi3

import (
	"context"
	"encoding/json"
)

type (
	Schemas map[string]*Schema

	SchemaList []*Schema

	Schema struct {
		Extension `json:",omitempty" yaml:",inline"`

		Ref   string     `json:"$ref,omitempty" yaml:"$ref,omitempty"`
		Type  string     `json:"type,omitempty" yaml:"type,omitempty"`
		AllOf SchemaList `json:"allOf,omitempty" yaml:"allOf,omitempty"`
		OneOf SchemaList `json:"oneOf,omitempty" yaml:"oneOf,omitempty"`
		AnyOf SchemaList `json:"anyOf,omitempty" yaml:"anyOf,omitempty"`

		Not   *Schema `json:"not,omitempty" yaml:"not,omitempty"`
		Items *Schema `json:"items,omitempty" yaml:"items,omitempty"`

		Properties                  Schemas     `json:"properties,omitempty" yaml:"properties,omitempty"`
		AdditionalPropertiesAllowed *bool       `multijson:"additionalProperties,omitempty" json:"-" yaml:"-"` // In this order...
		AdditionalProperties        *Schema     `multijson:"additionalProperties,omitempty" json:"-" yaml:"-"` // ...for multijson
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
		Extension `json:",omitempty" yaml:",inline"`
		Name      string `json:"name,omitempty"`
		Namespace string `json:"namespace,omitempty"`
		Prefix    string `json:"prefix,omitempty"`
		Attribute bool   `json:"attribute,omitempty"`
		Wrapped   bool   `json:"wrapped,omitempty"`
	}
)

func (s *Schema) UnmarshalJSON(b []byte) error {
	type temp Schema
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*s = Schema(tmp)
	return s.Extension.UnmarshalJSON(b)
}

func (s *Schema) MarshalJSON() ([]byte, error) {
	type temp Schema
	tmp := temp(*s)
	tmp.Extension = nil
	data, err := json.Marshal(tmp)
	if err != nil {
		return nil, err
	}
	if len(s.Extension) == 0 {
		return data, nil
	}
	extData, err := json.Marshal(s.Extension)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return extData, nil
	}
	res := mergeJSON(data, extData)
	return res, nil
}

func (s *XML) UnmarshalJSON(b []byte) error {
	type temp XML
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*s = XML(tmp)
	return s.Extension.UnmarshalJSON(b)
}

func (s *XML) MarshalJSON() ([]byte, error) {
	type temp XML
	tmp := temp(*s)
	tmp.Extension = nil
	data, err := json.Marshal(tmp)
	if err != nil {
		return nil, err
	}
	if len(s.Extension) == 0 {
		return data, nil
	}
	extData, err := json.Marshal(s.Extension)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return extData, nil
	}
	res := mergeJSON(data, extData)
	return res, nil
}

func (s *XML) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp XML
	tmp := temp(*s)
	err := fn(&tmp)
	if err != nil {
		return err
	}
	ext := CustomExtension{}
	err = fn(&ext)
	if err != nil {
		return err
	}
	tmp.Extension = Extension(ext)
	*s = XML(tmp)
	return nil
}

func (s *Schema) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp Schema
	tmp := temp(*s)
	err := fn(&tmp)
	if err != nil {
		return err
	}
	ext := CustomExtension{}
	err = fn(&ext)
	if err != nil {
		return err
	}
	tmp.Extension = Extension(ext)
	*s = Schema(tmp)
	if tmp.Ref == "" {
		return nil
	}
	session := LookupSession(ctx)
	param, err := session.LookupSchema(session.Location, tmp.Ref)
	if err == nil {
		*s = *param
	}
	return err
}
