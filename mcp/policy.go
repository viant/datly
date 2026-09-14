package mcp

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/viant/datly/mcp/resource"
	"github.com/viant/mcp-protocol/authorization"
	oauthmeta "github.com/viant/mcp-protocol/oauth2/meta"
)

func compileAuthorizationPolicy(source *authorization.Policy, catalog *Catalog) (*authorization.Policy, error) {
	policy := cloneAuthorizationPolicy(source)
	if policy == nil {
		return nil, nil
	}
	if policy.Global != nil && (len(policy.Tools) > 0 || len(policy.Resources) > 0) {
		return nil, fmt.Errorf("MCP global authorization cannot be combined with tool or resource rules")
	}
	if err := validateAuthorizationRule("global", policy.Global); err != nil {
		return nil, err
	}
	for _, name := range slices.Sorted(maps.Keys(policy.Tools)) {
		rule := policy.Tools[name]
		if strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("MCP authorization tool name is required")
		}
		if _, ok := catalog.Tool(name); !ok {
			return nil, fmt.Errorf("MCP authorization references unknown tool %q", name)
		}
		if err := validateAuthorizationRule("tool "+name, rule); err != nil {
			return nil, err
		}
	}
	return policy, validateResourceRules(policy.Resources, catalog.ResourceCatalog())
}

func validateResourceRules(rules map[string]*authorization.Authorization, catalog *resource.Catalog) error {
	if len(rules) == 0 {
		return nil
	}
	static := map[string]bool{}
	for _, resource := range catalog.Resources() {
		static[resource.Uri] = true
	}
	templates := map[string]bool{}
	for _, template := range catalog.Templates() {
		templates[template.UriTemplate] = true
	}
	for _, uri := range slices.Sorted(maps.Keys(rules)) {
		rule := rules[uri]
		if strings.TrimSpace(uri) == "" {
			return fmt.Errorf("MCP authorization resource URI is required")
		}
		if static[uri] {
			if err := validateAuthorizationRule("resource "+uri, rule); err != nil {
				return err
			}
			continue
		}
		if templates[uri] {
			return fmt.Errorf("MCP resource template %q requires global authorization", uri)
		}
		if _, err := catalog.Resolve(uri); err == nil {
			return fmt.Errorf("MCP concrete template resource %q requires global authorization", uri)
		}
		return fmt.Errorf("MCP authorization references unknown resource %q", uri)
	}
	return nil
}

func validateAuthorizationRule(name string, rule *authorization.Authorization) error {
	if rule == nil {
		if name == "global" {
			return nil
		}
		return fmt.Errorf("MCP authorization rule for %s is required", name)
	}
	if rule.ProtectedResourceMetadata == nil || strings.TrimSpace(rule.ProtectedResourceMetadata.Resource) == "" {
		return fmt.Errorf("MCP authorization rule for %s requires protected resource metadata", name)
	}
	return nil
}

func cloneAuthorizationPolicy(source *authorization.Policy) *authorization.Policy {
	if source == nil {
		return nil
	}
	return &authorization.Policy{
		Global:     cloneAuthorization(source.Global),
		ExcludeURI: source.ExcludeURI,
		Tools:      cloneAuthorizationMap(source.Tools),
		Resources:  cloneAuthorizationMap(source.Resources),
	}
}

func cloneAuthorizationMap(source map[string]*authorization.Authorization) map[string]*authorization.Authorization {
	if source == nil {
		return nil
	}
	result := make(map[string]*authorization.Authorization, len(source))
	for name, rule := range source {
		result[name] = cloneAuthorization(rule)
	}
	return result
}

func cloneAuthorization(source *authorization.Authorization) *authorization.Authorization {
	if source == nil {
		return nil
	}
	return &authorization.Authorization{
		RequiredScopes:            slices.Clone(source.RequiredScopes),
		UseIdToken:                source.UseIdToken,
		ProtectedResourceMetadata: cloneProtectedResourceMetadata(source.ProtectedResourceMetadata),
	}
}

func cloneProtectedResourceMetadata(source *oauthmeta.ProtectedResourceMetadata) *oauthmeta.ProtectedResourceMetadata {
	if source == nil {
		return nil
	}
	result := *source
	result.AuthorizationServers = slices.Clone(source.AuthorizationServers)
	result.ScopesSupported = slices.Clone(source.ScopesSupported)
	result.BearerMethodsSupported = slices.Clone(source.BearerMethodsSupported)
	result.ResourceSigningAlgValuesSupported = slices.Clone(source.ResourceSigningAlgValuesSupported)
	result.AuthorizationDetailsTypesSupported = slices.Clone(source.AuthorizationDetailsTypesSupported)
	result.DPOPSigningAlgValuesSupported = slices.Clone(source.DPOPSigningAlgValuesSupported)
	result.JSONWebKeySet = cloneJSONWebKeySet(source.JSONWebKeySet)
	result.Extra = cloneMetadataMap(source.Extra)
	return &result
}

func cloneJSONWebKeySet(source *oauthmeta.JSONWebKeySet) *oauthmeta.JSONWebKeySet {
	if source == nil {
		return nil
	}
	result := &oauthmeta.JSONWebKeySet{Keys: make([]oauthmeta.JSONWebKey, len(source.Keys))}
	for i, key := range source.Keys {
		result.Keys[i] = key
		result.Keys[i].KeyOps = slices.Clone(key.KeyOps)
		result.Keys[i].X5c = slices.Clone(key.X5c)
		result.Keys[i].Extra = cloneMetadataMap(key.Extra)
	}
	return result
}

// MCP extension fields are JSON values, so only JSON's mutable container
// shapes require recursive copies.
func cloneMetadataMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	result := make(map[string]any, len(source))
	for name, value := range source {
		result[name] = cloneMetadataValue(value)
	}
	return result
}

func cloneMetadataValue(source any) any {
	switch actual := source.(type) {
	case map[string]any:
		return cloneMetadataMap(actual)
	case []any:
		result := make([]any, len(actual))
		for i, value := range actual {
			result[i] = cloneMetadataValue(value)
		}
		return result
	case []string:
		return slices.Clone(actual)
	case []byte:
		return slices.Clone(actual)
	default:
		return actual
	}
}
