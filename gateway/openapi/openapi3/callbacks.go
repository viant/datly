// Adapted from viant/datly gateway/router/openapi/openapi3 (Apache-2.0).
// See LICENSE and NOTICE. Mutable document-loading methods are intentionally omitted.
package openapi3

type (
	Callbacks   map[string]*CallbackRef
	CallbackRef struct {
		Ref      string `json:"$ref,omitempty" yaml:"$ref,omitempty"`
		Callback `yaml:",inline"`
	}
	Callback map[string]*PathItem
)
