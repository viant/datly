package runtime

import (
	"context"
	"net/http"
	"net/url"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

func TestNestedComponentQueryPolicyPresence(t *testing.T) {
	type childInput struct {
		Name string `parameter:"Name,kind=query,in=name,value=fallback"`
	}
	type result struct{ Name string }
	type parentInput struct {
		Child *result `parameter:"Child,kind=component,in=GET:/query-child,required"`
	}
	ignore, preserve := true, false
	for _, tt := range []struct {
		name     string
		policy   *bool
		expected string
	}{
		{"inherit", nil, "fallback"}, {"explicit preserve", &preserve, ""}, {"explicit ignore", &ignore, "fallback"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			parentSpec := componentSpec("QueryParent", "GET", "/query-parent", nil)
			parentSpec.Settings = &spec.Settings{IgnoreEmptyQueryParameters: &ignore}
			childSpec := componentSpec("QueryChild", "GET", "/query-child", nil)
			childSpec.Settings = &spec.Settings{IgnoreEmptyQueryParameters: tt.policy}
			parent := componentArtifact(t, parentSpec, reflect.TypeOf(parentInput{}), reflect.TypeOf(result{}))
			child := componentArtifact(t, childSpec, reflect.TypeOf(childInput{}), reflect.TypeOf(result{}))
			runtime, err := NewRuntime([]*registry.RegisteredComponent{
				{Component: parent.Component, Input: parent.Input, OutputType: reflect.TypeOf(result{}), Handler: customhandler.NewFunc[parentInput, result](func(_ context.Context, input *parentInput) (*result, error) { return input.Child, nil })},
				{Component: child.Component, Input: child.Input, OutputType: reflect.TypeOf(result{}), Handler: customhandler.NewFunc[childInput, result](func(_ context.Context, input *childInput) (*result, error) { return &result{Name: input.Name}, nil })},
			})
			if err != nil {
				t.Fatal(err)
			}
			actual, err := executeTestRoute(t, runtime, context.Background(), testharness.NewRequest(http.MethodGet, "/query-parent").WithQuery(url.Values{"name": {""}}))
			if err != nil {
				t.Fatal(err)
			}
			if actual.(*result).Name != tt.expected {
				t.Fatalf("name=%q want=%q", actual.(*result).Name, tt.expected)
			}
		})
	}
}
