// Adapted from viant/datly gateway/router/openapi/openapi3 (Apache-2.0).
// See LICENSE and NOTICE. Mutable document-loading methods are intentionally omitted.
package openapi3

type (
	RequestBodies map[string]*RequestBody

	// RequestBody is specified by OpenAPI/Swagger 3.0 standard.
	RequestBody struct {
		Ref         string  `json:"$ref,omitempty" yaml:"$ref,omitempty"`
		Description string  `json:"description,omitempty" yaml:"description,omitempty"`
		Required    bool    `json:"required,omitempty" yaml:"required,omitempty"`
		Content     Content `json:"content,omitempty" yaml:"content,omitempty"`
	}
)
