// Adapted from viant/datly gateway/router/openapi/openapi3 (Apache-2.0).
// See LICENSE and NOTICE. Mutable document-loading methods are intentionally omitted.
package openapi3

type (
	Tags []*Tag

	Tag struct {
		Name         string                 `json:"name,omitempty" yaml:"name,omitempty"`
		Description  string                 `json:"description,omitempty" yaml:"description,omitempty"`
		ExternalDocs *ExternalDocumentation `json:"externalDocs,omitempty" yaml:"externalDocs,omitempty"`
	}
)
