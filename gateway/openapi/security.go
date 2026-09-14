package openapi

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"golang.org/x/net/http/httpguts"
)

type securityBuilder struct{ schemes openapi3.SecuritySchemes }

func (b *securityBuilder) apply(endpoint *spec.Route, fields []registry.InputField, operation *openapi3.Operation) error {
	requirement := openapi3.SecurityRequirement{}
	header := endpoint.APIKeyHeader
	if header != "" {
		if !httpguts.ValidHeaderFieldName(header) {
			return fmt.Errorf("invalid API-key header %q", header)
		}
		if endpoint.APIKeyValue == "" {
			return fmt.Errorf("API-key policy accepting an absent header cannot be represented as required security")
		}
		name := fmt.Sprintf("APIKey_%x", sha256.Sum256([]byte(strings.ToLower(header))))
		if b.schemes[name] == nil {
			b.schemes[name] = &openapi3.SecurityScheme{Type: "apiKey", In: "header", Name: header}
		}
		requirement[name] = []string{}
	}
	bearer, required := false, false
	for _, field := range fields {
		binding := field.Binding()
		if binding.Location.Kind != "header" || !strings.EqualFold(binding.Location.In, "Authorization") || !field.VerifiesJWT() {
			continue
		}
		bearer = true
		required = required || binding.Required != nil && *binding.Required
	}
	if bearer && strings.EqualFold(header, "Authorization") {
		return fmt.Errorf("API-key and verified JWT policies share Authorization; their combined credential cannot be represented independently")
	}
	var security openapi3.SecurityRequirements
	if bearer {
		b.schemes["BearerJWT"] = &openapi3.SecurityScheme{Type: "http", Scheme: "bearer", BearerFormat: "JWT"}
		withBearer := openapi3.SecurityRequirement{"BearerJWT": []string{}}
		for name, scopes := range requirement {
			withBearer[name] = scopes
		}
		security = append(security, withBearer)
		if !required {
			security = append(security, requirement)
		}
	} else if len(requirement) > 0 {
		security = append(security, requirement)
	}
	if len(security) > 0 {
		operation.Security = &security
	}
	return nil
}
