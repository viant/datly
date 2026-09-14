// Adapted from viant/datly gateway/router/openapi/openapi3 (Apache-2.0).
// See LICENSE and NOTICE. Mutable document-loading methods are intentionally omitted.
package openapi3

type (
	SecurityRequirements []SecurityRequirement
	SecurityRequirement  map[string][]string
	SecuritySchemes      map[string]*SecurityScheme

	SecurityScheme struct {
		Ref              string      `json:"$ref,omitempty" yaml:"$ref,omitempty"`
		Type             string      `json:"type,omitempty" yaml:"type,omitempty"`
		Description      string      `json:"description,omitempty" yaml:"description,omitempty"`
		Name             string      `json:"name,omitempty" yaml:"name,omitempty"`
		In               string      `json:"in,omitempty" yaml:"in,omitempty"`
		Scheme           string      `json:"scheme,omitempty" yaml:"scheme,omitempty"`
		BearerFormat     string      `json:"bearerFormat,omitempty" yaml:"bearerFormat,omitempty"`
		Flows            *OAuthFlows `json:"flows,omitempty" yaml:"flows,omitempty"`
		OpenIdConnectUrl string      `json:"openIdConnectUrl,omitempty" yaml:"openIdConnectUrl,omitempty"`
	}
)
