// Adapted from viant/datly gateway/router/openapi/openapi3 (Apache-2.0).
// See LICENSE and NOTICE. Mutable document-loading methods are intentionally omitted.
package openapi3

// Paths represents a path defined by OpenAPI/Swagger standard version 3.0.
type (
	Paths    map[string]*PathItem
	PathItem struct {
		Ref         string `json:"$ref,omitempty" yaml:"$ref,omitempty"`
		Summary     string `json:"summary,omitempty" yaml:"summary,omitempty"`
		Description string `json:"description,omitempty" yaml:"description,omitempty"`

		Delete     *Operation `json:"delete,omitempty" yaml:"delete,omitempty"`
		Get        *Operation `json:"get,omitempty" yaml:"get,omitempty"`
		Head       *Operation `json:"head,omitempty" yaml:"head,omitempty"`
		Options    *Operation `json:"options,omitempty" yaml:"options,omitempty"`
		Patch      *Operation `json:"patch,omitempty" yaml:"patch,omitempty"`
		Post       *Operation `json:"post,omitempty" yaml:"post,omitempty"`
		Put        *Operation `json:"put,omitempty" yaml:"put,omitempty"`
		Trace      *Operation `json:"trace,omitempty" yaml:"trace,omitempty"`
		Servers    Servers    `json:"servers,omitempty" yaml:"servers,omitempty"`
		Parameters Parameters `json:"parameters,omitempty" yaml:"parameters,omitempty"`
	}
)
