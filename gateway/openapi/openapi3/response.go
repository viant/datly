// Adapted from viant/datly gateway/router/openapi/openapi3 (Apache-2.0).
// See LICENSE and NOTICE. Mutable document-loading methods are intentionally omitted.
package openapi3

// Responses is specified by OpenAPI/Swagger 3.0 standard.
type (
	Responses map[string]*Response

	// Response is specified by OpenAPI/Swagger 3.0 standard.
	Response struct {
		Ref         string  `json:"$ref,omitempty" yaml:"$ref,omitempty"`
		Description *string `json:"description,omitempty" yaml:"description,omitempty"`
		Headers     Headers `json:"headers,omitempty" yaml:"headers,omitempty"`
		Content     Content `json:"content,omitempty" yaml:"content,omitempty"`
		Links       Links   `json:"links,omitempty" yaml:"links,omitempty"`
	}
)
