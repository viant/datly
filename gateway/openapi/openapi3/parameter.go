// Adapted from viant/datly gateway/router/openapi/openapi3 (Apache-2.0).
// See LICENSE and NOTICE. Mutable document-loading methods are intentionally omitted.
package openapi3

type (
	Parameters    []*Parameter
	ParametersMap map[string]*Parameter

	Parameter struct {
		Ref             string      `json:"$ref,omitempty" yaml:"$ref,omitempty"`
		Name            string      `json:"name,omitempty" yaml:"name,omitempty"`
		In              string      `json:"in,omitempty" yaml:"in,omitempty"`
		Description     string      `json:"description,omitempty" yaml:"description,omitempty"`
		Style           string      `json:"style,omitempty" yaml:"style,omitempty"`
		Explode         *bool       `json:"explode,omitempty" yaml:"explode,omitempty"`
		AllowEmptyValue bool        `json:"allowEmptyValue,omitempty" yaml:"allowEmptyValue,omitempty"`
		AllowReserved   bool        `json:"allowReserved,omitempty" yaml:"allowReserved,omitempty"`
		Deprecated      bool        `json:"deprecated,omitempty" yaml:"deprecated,omitempty"`
		Required        bool        `json:"required,omitempty" yaml:"required,omitempty"`
		Schema          *Schema     `json:"schema,omitempty" yaml:"schema,omitempty"`
		Example         interface{} `json:"example,omitempty" yaml:"example,omitempty"`
		Examples        Examples    `json:"examples,omitempty" yaml:"examples,omitempty"`
		Content         Content     `json:"content,omitempty" yaml:"content,omitempty"`
	}
)
