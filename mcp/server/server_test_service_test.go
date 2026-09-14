package server

import (
	"context"
	"sync"

	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/authorization"
	oauthmeta "github.com/viant/mcp-protocol/oauth2/meta"
	"github.com/viant/mcp-protocol/schema"
	protocolserver "github.com/viant/mcp-protocol/server"
)

const testResourceURI = "datly://localhost/files/a%2Fb"

type transportTestService struct {
	registry *protocolserver.Registry
	policy   *authorization.Policy

	mu    sync.Mutex
	token string
}

func newTransportTestService(policy *authorization.Policy) *transportTestService {
	registry := protocolserver.NewRegistry()
	registry.RegisterTool(&protocolserver.ToolEntry{
		Metadata: schema.Tool{Name: "registered"},
		Handler: func(context.Context, *schema.CallToolRequest) (*schema.CallToolResult, *jsonrpc.Error) {
			return &schema.CallToolResult{}, nil
		},
	})
	registry.RegisterResource(schema.Resource{Name: "file", Uri: testResourceURI}, nil)
	return &transportTestService{registry: registry, policy: policy}
}

func (s *transportTestService) Registry() *protocolserver.Registry { return s.registry }

func (s *transportTestService) Authorization() *authorization.Policy { return s.policy }

func (s *transportTestService) ReadResource(ctx context.Context, request *schema.ReadResourceRequest) (*schema.ReadResourceResult, *jsonrpc.Error) {
	if token, ok := ctx.Value(authorization.TokenKey).(*authorization.Token); ok && token != nil {
		s.mu.Lock()
		s.token = token.Token
		s.mu.Unlock()
	}
	uri := ""
	if request != nil {
		uri = request.Params.Uri
	}
	return &schema.ReadResourceResult{Contents: []schema.ReadResourceResultContentsElem{{
		Uri: uri, MimeType: testStringPointer("text/plain"), Text: "ready",
	}}}, nil
}

func (s *transportTestService) observedToken() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.token
}

func testAuthorizationPolicy() *authorization.Policy {
	return &authorization.Policy{Resources: map[string]*authorization.Authorization{
		testResourceURI: {
			RequiredScopes:            []string{"read"},
			ProtectedResourceMetadata: &oauthmeta.ProtectedResourceMetadata{Resource: "https://api.example.com"},
		},
	}}
}

func testStringPointer(value string) *string { return &value }
