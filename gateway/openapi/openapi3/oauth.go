// Adapted from viant/datly gateway/router/openapi/openapi3 (Apache-2.0).
// See LICENSE and NOTICE. Mutable document-loading methods are intentionally omitted.
package openapi3

type (
	OAuthFlows struct {
		Implicit          *OAuthFlow `json:"implicit,omitempty" yaml:"implicit,omitempty"`
		Password          *OAuthFlow `json:"password,omitempty" yaml:"password,omitempty"`
		ClientCredentials *OAuthFlow `json:"clientCredentials,omitempty" yaml:"clientCredentials,omitempty"`
		AuthorizationCode *OAuthFlow `json:"authorizationCode,omitempty" yaml:"authorizationCode,omitempty"`
	}

	OAuthFlow struct {
		AuthorizationURL string            `json:"authorizationUrl,omitempty" yaml:"authorizationUrl,omitempty"`
		TokenURL         string            `json:"tokenUrl,omitempty" yaml:"tokenUrl,omitempty"`
		RefreshURL       string            `json:"refreshUrl,omitempty" yaml:"refreshUrl,omitempty"`
		Scopes           map[string]string `json:"scopes" yaml:"scopes"`
	}
)
