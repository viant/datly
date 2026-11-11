package openapi_test

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/gateway/openapi"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"reflect"
	"strings"
	"testing"
)

func TestTypedSuccessStaticHeadersDocumentation(t *testing.T) {
	type input struct{}
	type result struct {
		URL string `json:"url"`
	}
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/header", Name: "Report"}, Routes: []*spec.Route{{Method: "GET", Path: "/report"}, {Method: "HEAD", Path: "/report"}}, Settings: &spec.Settings{Output: &spec.OutputSettings{Headers: map[string]string{"Cache-Control": "no-store"}}}}
	fn := custom.NewFunc[input, result](func(context.Context, *input) (*result, error) { return &result{URL: "url"}, nil })
	a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[result]()})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := a.Registration(registry.RegisteredComponent{Handler: fn})
	if err != nil {
		t.Fatal(err)
	}
	doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
	if err != nil {
		t.Fatal(err)
	}
	for _, response := range []string{"GET", "HEAD"} {
		op := doc.Paths["/report"].Get
		if response == "HEAD" {
			op = doc.Paths["/report"].Head
		}
		success := op.Responses["200"]
		if success.Headers["Cache-Control"] == nil || success.Headers["Cache-Control"].Schema.Enum[0] != "no-store" {
			t.Fatal("missing success header")
		}
		if response == "HEAD" && len(success.Content) != 0 {
			t.Fatal("HEAD body documented")
		}
	}
	schema := documentAssertion{t, doc}.deref(doc.Paths["/report"].Get.Responses["200"].Content["application/json"].Schema)
	if len(schema.Properties) != 1 || schema.Properties["url"] == nil {
		t.Fatal("header leaked into body schema")
	}
	// Direct/MCP consumers retain the same typed encoder and never see headers.
	encoded, err := entry.Output.Encode(context.Background(), "json", &result{URL: "url"})
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]string
	if err = json.Unmarshal(encoded.Data, &body); err != nil || len(body) != 1 || strings.Contains(string(encoded.Data), "Cache-Control") {
		t.Fatalf("%s %v", encoded.Data, err)
	}
}
