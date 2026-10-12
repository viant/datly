package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

func TestMethodMismatchPolicy(t *testing.T) {
	for _, policy := range []string{"", "method-not-allowed", "not-found"} {
		t.Run(policy, func(t *testing.T) {
			executions, authorized := 0, 0
			var entries []*registry.RegisteredComponent
			for i, path := range []string{"/records", "/records/{id}", "/internal"} {
				c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example/methods", Name: path}, Routes: []*spec.Route{{Method: "GET", Path: path, Internal: i == 2}}}
				artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[configInput](), OutputType: reflect.TypeFor[configOutput]()})
				if err != nil {
					t.Fatal(err)
				}
				entries = append(entries, &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[configOutput](), Handler: custom.NewFunc[configInput, configOutput](func(context.Context, *configInput) (*configOutput, error) { executions++; return &configOutput{}, nil })})
			}
			rt, err := druntime.NewRuntime(entries)
			if err != nil {
				t.Fatal(err)
			}
			h, err := (Config{MethodMismatchPolicy: policy, Authorize: func(context.Context, *http.Request, exec.ComponentTarget) error { authorized++; return nil }}).NewHandler(rt, nil, "test")
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{"/records", "/records/1", "/missing", "/internal"} {
				w := httptest.NewRecorder()
				h.ServeHTTP(w, httptest.NewRequest("POST", path, nil))
				want := 405
				if policy == "not-found" || path == "/missing" || path == "/internal" {
					want = 404
				}
				if w.Code != want {
					t.Fatalf("%s status%d wanted%d", path, w.Code, want)
				}
				if want == 405 && w.Header().Get("Allow") != "GET" {
					t.Fatal("missing Allow")
				}
				if want == 404 && w.Header().Get("Allow") != "" {
					t.Fatal("404 leaked Allow")
				}
				if executions != 0 || authorized != 0 {
					t.Fatal("method rejection invoked component or authorization")
				}
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", "/records", nil))
			if w.Code != 200 || executions != 1 || authorized != 1 {
				t.Fatal("valid method changed", w.Code)
			}
		})
	}
	rt := routingConsumerRuntime(t)
	if _, err := (Config{MethodMismatchPolicy: "invalid"}).NewHandler(rt, nil, "test"); err == nil {
		t.Fatal("invalid policy accepted")
	}
}
