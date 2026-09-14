// Adapted from viant/datly gateway/router/openapi/openapi3 (Apache-2.0).
// See LICENSE and NOTICE. Mutable document-loading methods are intentionally omitted.
package openapi3

// Info represents document info
type (
	Info struct {
		Title          string   `json:"title" yaml:"title"` // Required
		Description    string   `json:"description,omitempty" yaml:"description,omitempty"`
		TermsOfService string   `json:"termsOfService,omitempty" yaml:"termsOfService,omitempty"`
		Contact        *Contact `json:"contact,omitempty" yaml:"contact,omitempty"`
		License        *License `json:"license,omitempty" yaml:"license,omitempty"`
		Version        string   `json:"version" yaml:"version"` // Required
	}
	Contact struct {
		Name  string `json:"name,omitempty" yaml:"name,omitempty"`
		URL   string `json:"url,omitempty" yaml:"url,omitempty"`
		Email string `json:"email,omitempty" yaml:"email,omitempty"`
	}
	License struct {
		Name string `json:"name" yaml:"name"` // Required
		URL  string `json:"url,omitempty" yaml:"url,omitempty"`
	}
)
