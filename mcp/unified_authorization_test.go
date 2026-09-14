package mcp

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/authorization"
)

type authorizationInput struct {
	Authorization string
}

type authorizationOutput struct {
	Authorization string `json:"authorization"`
}

func TestToolProtocolTokenReachesCanonicalAuthorizationBinding(t *testing.T) {
	required := true
	component := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/app", Name: "Authorization"}, Name: "Authorization",
		Routes: []*spec.Route{{Method: "POST", Path: "/authorization", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "authorization.run"}}}},
		Parameters: []*spec.Parameter{{
			Name: "Authorization", Source: spec.BindSource{Kind: "header", Name: "Authorization"}, Required: &required,
		}},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(authorizationInput{}), OutputType: reflect.TypeOf(authorizationOutput{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	registered := &registry.RegisteredComponent{
		Component: component, Input: artifact.Input, OutputType: reflect.TypeOf(authorizationOutput{}),
		Handler: customhandler.NewFunc[authorizationInput, authorizationOutput](func(_ context.Context, input *authorizationInput) (*authorizationOutput, error) {
			return &authorizationOutput{Authorization: input.Authorization}, nil
		}),
	}
	token := &authorization.Token{Token: "secret"}
	ctx := context.WithValue(context.Background(), authorization.TokenKey, token)
	result := executeRuntimeToolContext(t, ctx, registered, "authorization.run", nil)
	if testharness.StructuredObject(t, result.StructuredContent)["authorization"] != "Bearer secret" || token.Token != "secret" {
		t.Fatalf("result=%+v token=%+v", result.StructuredContent, token)
	}
}
