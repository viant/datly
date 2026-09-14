package mcp

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/authorization"
	oauthmeta "github.com/viant/mcp-protocol/oauth2/meta"
)

func TestNewValidatesAuthorizationAgainstCompiledCatalog(t *testing.T) {
	components := authorizationComponents(t)
	rule := authorizationRule()
	tests := []struct {
		name   string
		policy *authorization.Policy
		match  string
	}{
		{name: "global", policy: &authorization.Policy{Global: rule}},
		{name: "exact tool", policy: &authorization.Policy{Tools: map[string]*authorization.Authorization{"search.run": rule}}},
		{name: "exact static resource", policy: &authorization.Policy{Resources: map[string]*authorization.Authorization{"datly://localhost/status": rule}}},
		{name: "mixed global", policy: &authorization.Policy{Global: rule, Tools: map[string]*authorization.Authorization{"search.run": rule}}, match: "cannot be combined"},
		{name: "unknown tool", policy: &authorization.Policy{Tools: map[string]*authorization.Authorization{"missing": rule}}, match: "unknown tool"},
		{name: "nil tool rule", policy: &authorization.Policy{Tools: map[string]*authorization.Authorization{"search.run": nil}}, match: "rule for tool"},
		{name: "unknown resource", policy: &authorization.Policy{Resources: map[string]*authorization.Authorization{"datly://localhost/missing": rule}}, match: "unknown resource"},
		{name: "template", policy: &authorization.Policy{Resources: map[string]*authorization.Authorization{"datly://localhost/orders/{id}": rule}}, match: "requires global"},
		{name: "concrete template", policy: &authorization.Policy{Resources: map[string]*authorization.Authorization{"datly://localhost/orders/1": rule}}, match: "requires global"},
		{name: "missing metadata", policy: &authorization.Policy{Tools: map[string]*authorization.Authorization{"search.run": {}}}, match: "protected resource metadata"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, err := New(Config{Components: components, Invoker: &serviceInvoker{}, Authorization: test.policy})
			if test.match == "" {
				if err != nil || service == nil {
					t.Fatalf("service=%v error=%v", service, err)
				}
				return
			}
			if err == nil || service != nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("service=%v error=%v, want %q", service, err, test.match)
			}
		})
	}
}

func TestAuthorizationPolicyIsDetached(t *testing.T) {
	rule := authorizationRule()
	rule.ProtectedResourceMetadata.AuthorizationServers = []string{"https://auth.example.com"}
	rule.ProtectedResourceMetadata.Extra = map[string]any{
		"vendor": map[string]any{"modes": []any{"strict"}},
	}
	rule.ProtectedResourceMetadata.JSONWebKeySet = &oauthmeta.JSONWebKeySet{Keys: []oauthmeta.JSONWebKey{{
		Kty:    "RSA",
		Kid:    "key-1",
		KeyOps: []string{"verify"},
		X5c:    []string{"certificate"},
		Extra:  map[string]any{"tenant": map[string]any{"ids": []any{"one"}}},
	}}}
	source := &authorization.Policy{Tools: map[string]*authorization.Authorization{"search.run": rule}}
	service, err := New(Config{Components: authorizationComponents(t), Invoker: &serviceInvoker{}, Authorization: source})
	if err != nil {
		t.Fatal(err)
	}
	source.Tools["search.run"].RequiredScopes[0] = "changed"
	source.Tools["search.run"].ProtectedResourceMetadata.AuthorizationServers[0] = "changed"
	sourceVendor := source.Tools["search.run"].ProtectedResourceMetadata.Extra["vendor"].(map[string]any)
	sourceVendor["modes"].([]any)[0] = "changed"
	sourceKey := &source.Tools["search.run"].ProtectedResourceMetadata.JSONWebKeySet.Keys[0]
	sourceKey.KeyOps[0] = "changed"
	sourceKey.X5c[0] = "changed"
	sourceKey.Extra["tenant"].(map[string]any)["ids"].([]any)[0] = "changed"
	first := service.Authorization()
	first.Tools["search.run"].RequiredScopes[0] = "caller-change"
	firstMetadata := first.Tools["search.run"].ProtectedResourceMetadata
	firstMetadata.AuthorizationServers[0] = "caller-change"
	firstMetadata.Extra["vendor"].(map[string]any)["modes"].([]any)[0] = "caller-change"
	firstKey := &firstMetadata.JSONWebKeySet.Keys[0]
	firstKey.KeyOps[0] = "caller-change"
	firstKey.X5c[0] = "caller-change"
	firstKey.Extra["tenant"].(map[string]any)["ids"].([]any)[0] = "caller-change"
	second := service.Authorization()
	if !reflect.DeepEqual(second.Tools["search.run"].RequiredScopes, []string{"read"}) {
		t.Fatalf("detached scopes = %v", second.Tools["search.run"].RequiredScopes)
	}
	metadata := second.Tools["search.run"].ProtectedResourceMetadata
	if !reflect.DeepEqual(metadata.AuthorizationServers, []string{"https://auth.example.com"}) {
		t.Fatalf("detached authorization servers = %v", metadata.AuthorizationServers)
	}
	if got := metadata.Extra["vendor"].(map[string]any)["modes"].([]any)[0]; got != "strict" {
		t.Fatalf("detached metadata extension = %v", got)
	}
	key := metadata.JSONWebKeySet.Keys[0]
	if !reflect.DeepEqual(key.KeyOps, []string{"verify"}) || !reflect.DeepEqual(key.X5c, []string{"certificate"}) {
		t.Fatalf("detached JWK slices = key_ops:%v x5c:%v", key.KeyOps, key.X5c)
	}
	if got := key.Extra["tenant"].(map[string]any)["ids"].([]any)[0]; got != "one" {
		t.Fatalf("detached JWK extension = %v", got)
	}
}

func authorizationComponents(t *testing.T) []*registry.RegisteredComponent {
	t.Helper()
	tool := serviceComponent(t, "Search", &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "search.run"})
	static := buildServiceComponent(t, serviceComponentFixture{
		name: "Status", inputType: reflect.TypeOf(struct{}{}),
		route: &spec.Route{Method: "GET", Path: "/status", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureResource, Name: "status"}}},
	})
	template := serviceResourceComponent(t, "Order", &spec.MCPExposure{Kind: spec.MCPExposureResourceTemplate, Name: "orders"})
	return []*registry.RegisteredComponent{tool, static, template}
}

func authorizationRule() *authorization.Authorization {
	return &authorization.Authorization{
		RequiredScopes: []string{"read"},
		ProtectedResourceMetadata: &oauthmeta.ProtectedResourceMetadata{
			Resource: "https://api.example.com",
		},
	}
}
