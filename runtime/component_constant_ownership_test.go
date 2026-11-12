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

// Component dependencies retain their own authored constants. A parent constant
// is not a declarative input argument to the child, even with the same name.
func TestComponentDependencyKeepsConstantsOwnedByChild(t *testing.T) {
	type childInput struct{ Policy string }
	type childOutput struct{ Policy string }
	type parentInput struct {
		Policy string
		Child  *childOutput
	}
	childValue := "child-policy"
	child := componentSpec("PolicyChild", http.MethodGet, "/policy-child", []*spec.Parameter{{Name: "Policy", Source: spec.BindSource{Kind: "const", Name: "Policy"}, TypeExpr: "string", Value: &childValue}})
	childArtifact := componentArtifact(t, child, reflect.TypeFor[childInput](), reflect.TypeFor[childOutput]())
	registrations := []*registry.RegisteredComponent{{Component: childArtifact.Component, Input: childArtifact.Input, OutputType: reflect.TypeFor[childOutput](), Handler: customhandler.NewFunc[childInput, childOutput](func(_ context.Context, input *childInput) (*childOutput, error) {
		return &childOutput{Policy: input.Policy}, nil
	})}}
	for _, name := range []string{"first", "second"} {
		value := name + "-policy"
		parent := componentSpec(name, http.MethodGet, "/"+name, []*spec.Parameter{
			{Name: "Policy", Source: spec.BindSource{Kind: "const", Name: "Policy"}, TypeExpr: "string", Value: &value},
			{Name: "Child", Source: spec.BindSource{Kind: "component", Name: "GET:/policy-child"}, TypeExpr: "*childOutput"},
		})
		artifact := componentArtifact(t, parent, reflect.TypeFor[parentInput](), reflect.TypeFor[childOutput]())
		registrations = append(registrations, &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[childOutput](), Handler: customhandler.NewFunc[parentInput, childOutput](func(_ context.Context, input *parentInput) (*childOutput, error) { return input.Child, nil })})
	}
	service, err := NewRuntime(registrations)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Shutdown(context.Background()) })
	for _, path := range []string{"/first", "/second", "/first"} {
		actual, err := executeTestRoute(t, service, context.Background(), testharness.NewRequest(http.MethodGet, path).WithQuery(url.Values{"Policy": {"untrusted-policy"}}))
		if err != nil {
			t.Fatal(err)
		}
		if got := actual.(*childOutput).Policy; got != childValue {
			t.Fatalf("child constant changed through caller: %q", got)
		}
	}
}
