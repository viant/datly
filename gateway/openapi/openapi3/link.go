// Adapted from viant/datly gateway/router/openapi/openapi3 (Apache-2.0).
// See LICENSE and NOTICE. Mutable document-loading methods are intentionally omitted.
package openapi3

type (
	Links map[string]*Link
	Link  struct {
		Ref          string                 `json:"$ref,omitempty" yaml:"$ref,omitempty"`
		OperationID  string                 `json:"operationId,omitempty" yaml:"operationId,omitempty"`
		OperationRef string                 `json:"operationRef,omitempty" yaml:"operationRef,omitempty"`
		Description  string                 `json:"description,omitempty" yaml:"description,omitempty"`
		Parameters   map[string]interface{} `json:"parameters,omitempty" yaml:"parameters,omitempty"`
		Server       *Server                `json:"server,omitempty" yaml:"server,omitempty"`
		RequestBody  interface{}            `json:"requestBody,omitempty" yaml:"requestBody,omitempty"`
	}
)
