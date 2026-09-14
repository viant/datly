// Adapted from viant/datly gateway/router/openapi/openapi3 (Apache-2.0).
// See LICENSE and NOTICE. Mutable document-loading methods are intentionally omitted.
package openapi3

type ExternalDocumentation struct {
	Description string `json:"description,omitempty"`
	URL         string `json:"url,omitempty"`
}
