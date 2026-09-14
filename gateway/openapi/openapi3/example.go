// Adapted from viant/datly gateway/router/openapi/openapi3 (Apache-2.0).
// See LICENSE and NOTICE. Mutable document-loading methods are intentionally omitted.
package openapi3

type (
	Examples map[string]*Example

	Example struct {
		Ref           string      `json:"$ref,omitempty" yaml:"$ref,omitempty"`
		Summary       string      `json:"summary,omitempty" yaml:"summary,omitempty"`
		Description   string      `json:"description,omitempty" yaml:"description,omitempty"`
		Value         interface{} `json:"value,omitempty" yaml:"value,omitempty"`
		ExternalValue string      `json:"externalValue,omitempty" yaml:"externalValue,omitempty"`
	}
)
