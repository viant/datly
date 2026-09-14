package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	druntime "github.com/viant/datly/runtime"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

func TestHandlerUsesAuthoredBindingErrorStatusAndMessage(t *testing.T) {
	type input struct {
		Authorization string
	}
	type output struct{}
	required := true
	component := &spec.Component{
		Key:    spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo", Name: "Secure"},
		Name:   "Secure",
		Routes: []*spec.Route{{Method: http.MethodGet, Path: "/secure"}},
		Parameters: []*spec.Parameter{{
			Name:            "Authorization",
			Source:          spec.BindSource{Kind: "header", Name: "Authorization"},
			Required:        &required,
			ErrorStatusCode: http.StatusUnauthorized,
			ErrorMessage:    "authorization failed: ${error}",
		}},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component:  component,
		InputType:  reflect.TypeOf(input{}),
		OutputType: reflect.TypeOf(output{}),
	})
	if err != nil {
		t.Fatalf("BuildArtifact() error = %v", err)
	}
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{{
		Component:  component,
		Input:      artifact.Input,
		OutputType: reflect.TypeOf(output{}),
		Handler: customhandler.NewFunc[input, output](func(context.Context, *input) (*output, error) {
			t.Fatal("handler must not execute after required binding fails")
			return &output{}, nil
		}),
	}})
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	response := httptest.NewRecorder()
	NewHandler(runtime, nil, "test").ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/secure", nil))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "authorization failed: missing required header value") {
		t.Fatalf("body = %s", response.Body.String())
	}
}

func TestHandlerAPIKeyLookupUsesSameExactRoutePrecedenceAsDispatch(t *testing.T) {
	template := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo", Name: "Template"},
		Routes: []*spec.Route{{
			Method: http.MethodGet, Path: "/items/{id}", APIKeyHeader: "X-API-Key", APIKeyValue: "secret",
		}},
	}
	exact := &spec.Component{
		Key:    spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo", Name: "Exact"},
		Routes: []*spec.Route{{Method: http.MethodGet, Path: "/items/me"}},
	}
	templateArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: template, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(struct{}{})})
	if err != nil {
		t.Fatal(err)
	}
	exactArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: exact, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(struct{}{})})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{
		{Component: templateArtifact.Component, Input: templateArtifact.Input, OutputType: reflect.TypeOf(struct{}{})},
		{Component: exactArtifact.Component, Input: exactArtifact.Input, OutputType: reflect.TypeOf(struct{}{})},
	})
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	handler := NewHandler(runtime, nil, "test")
	if !handler.canHandle(httptest.NewRequest(http.MethodGet, "/items/me", nil)) {
		t.Fatal("exact route without API key must not inherit template authorization")
	}
	if handler.canHandle(httptest.NewRequest(http.MethodGet, "/items/42", nil)) {
		t.Fatal("template route must enforce its API key")
	}
}
