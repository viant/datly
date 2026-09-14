// Adapted from viant/datly gateway/router/openapi/openapi3 (Apache-2.0).
// See LICENSE and NOTICE. Mutable document-loading methods are intentionally omitted.
package openapi3

// Server  represents a server node defined is specified by OpenAPI/Swagger standard version 3.0.
type (
	Servers []Server
	Server  struct {
		URL         string                    `json:"url" yaml:"url"`
		Description string                    `json:"description,omitempty" yaml:"description,omitempty"`
		Variables   map[string]ServerVariable `json:"variables,omitempty" yaml:"variables,omitempty"`
	}
	//ServerVariable represents server variables defined by OpenAPI/Swagger standard version 3.0.
	ServerVariable struct {
		Enum        []string `json:"enum,omitempty" yaml:"enum,omitempty"`
		Default     string   `json:"default,omitempty" yaml:"default,omitempty"`
		Description string   `json:"description,omitempty" yaml:"description,omitempty"`
	}
)
